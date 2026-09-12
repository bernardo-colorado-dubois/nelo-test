import os

from pyspark.sql import functions as F

from src.schemas import CATEGORY_FIELD_CANDIDATES, EVENT_FIELDS, ITEM_FIELDS
from src.spark_io import count_new_keys, upsert_by_key, write_single_csv_atomic

# item_params.value trae 4 variantes (string/int/float/double); solo una viene poblada.
VALUE_VARIANT_COLUMNS = ["string_value", "int_value", "float_value", "double_value"]


def load_raw_table(spark, table_path):
  return spark.read.parquet(table_path)


def explode_items(raw_df):
  """Una fila por item de body.items; eventos sin items quedan como una fila con item nulo."""
  event_cols = [F.col(f"body.{field}").alias(field) for field in EVENT_FIELDS]
  base = raw_df.select("message_id", "received_at", *event_cols, F.col("body.items").alias("items"))

  exploded = base.select(
    "message_id", "received_at", *EVENT_FIELDS,
    F.explode_outer("items").alias("item"),
  )

  item_cols = [F.col(f"item.{field}").alias(field) for field in ITEM_FIELDS]
  return exploded.select(
    "message_id", "received_at", *EVENT_FIELDS, *item_cols,
    F.col("item.item_params").alias("item_params"),
  )


def with_row_id(df):
  """id determinístico = hash(message_id + item_id); permite reprocesar sin duplicar filas."""
  key_expr = F.concat_ws("::", F.col("message_id"), F.coalesce(F.col("item_id"), F.lit("__no_item__")))
  return df.withColumn("id", F.sha2(key_expr, 256))


def coalesce_param_value():
  variants = [F.col(f"param.value.{column}").cast("string") for column in VALUE_VARIANT_COLUMNS]
  return F.coalesce(*variants)


def pivot_item_params(exploded_df):
  """Convierte item_params [{key, value}, ...] en columnas {key: valor}, una por fila de id."""
  params = (
    exploded_df
    .select("id", F.explode("item_params").alias("param"))
    .select("id", F.col("param.key").alias("key"), coalesce_param_value().alias("value"))
    .filter(F.col("key").isNotNull())
  )
  return params.groupBy("id").pivot("key").agg(F.first("value"))


def build_flat_dataframe(raw_df):
  """Explota body.items -> una fila por item, con item_params pivoteado a columnas."""
  exploded = with_row_id(explode_items(raw_df))
  base_flat = exploded.drop("item_params")
  pivoted = pivot_item_params(exploded)
  return base_flat.join(pivoted, on="id", how="left")


def pick_category_field(df):
  for field in CATEGORY_FIELD_CANDIDATES:
    if field in df.columns and df.filter(F.col(field).isNotNull()).limit(1).count() > 0:
      return field
  return None


def print_quick_analysis(df):
  print("\n--- Conteo por event_name ---")
  df.groupBy("event_name").count().orderBy(F.desc("count")).show(n=100, truncate=False)

  category_field = pick_category_field(df)
  if category_field:
    print(f"\n--- Conteo por categoría ({category_field}) ---")
    df.groupBy(category_field).count().orderBy(F.desc("count")).show(n=100, truncate=False)
  else:
    print("\n--- Conteo por categoría ---")
    print("No hay ningún campo de categoría con datos.")


def load_existing_csv(spark, csv_path):
  if os.path.exists(csv_path):
    return spark.read.option("header", True).csv(csv_path)
  return None


def upsert_flat_csv(spark, raw_table_path, output_csv):
  raw_df = load_raw_table(spark, raw_table_path)
  new_flat_df = build_flat_dataframe(raw_df)

  existing_df = load_existing_csv(spark, output_csv)
  added = count_new_keys(existing_df, new_flat_df, "id")

  merged_df = upsert_by_key(existing_df, new_flat_df, key_col="id", order_col="received_at")
  write_single_csv_atomic(merged_df, output_csv)

  return merged_df, added
