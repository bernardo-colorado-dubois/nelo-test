import os
import shutil
import sys

from pyspark.sql import Window
from pyspark.sql import functions as F

from src.schemas import CATEGORY_FIELD_CANDIDATES, EVENT_FIELDS, ITEM_FIELDS
from src.spark_session import get_spark

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_RAW_TABLE_PATH = os.path.join(BASE_DIR, "data", "raw_messages")
DEFAULT_OUTPUT_CSV = os.path.join(BASE_DIR, "output", "items_flat.csv")

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


def count_new_ids(existing_df, new_df):
  if existing_df is None:
    return new_df.select("id").distinct().count()
  return new_df.join(existing_df, "id", "left_anti").select("id").distinct().count()


def upsert_by_id(existing_df, new_df):
  """existing_df + new_df quedándose con la fila más reciente (received_at) por id."""
  combined = new_df if existing_df is None else existing_df.unionByName(new_df, allowMissingColumns=True)

  window = Window.partitionBy("id").orderBy(F.col("received_at").desc())
  return (
    combined
    .withColumn("_rn", F.row_number().over(window))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
  )


def write_csv_atomic(df, target_path):
  """Escribe df como un único archivo CSV; Spark solo sabe escribir directorios de partes."""
  tmp_dir = f"{target_path}.tmp_dir"
  if os.path.exists(tmp_dir):
    shutil.rmtree(tmp_dir)

  df.coalesce(1).write.mode("overwrite").option("header", True).csv(tmp_dir)

  part_file = next(name for name in os.listdir(tmp_dir) if name.startswith("part-") and name.endswith(".csv"))
  os.makedirs(os.path.dirname(target_path) or ".", exist_ok=True)
  if os.path.exists(target_path):
    os.remove(target_path)
  shutil.move(os.path.join(tmp_dir, part_file), target_path)
  shutil.rmtree(tmp_dir)


def run(raw_table_path=DEFAULT_RAW_TABLE_PATH, output_csv=DEFAULT_OUTPUT_CSV):
  spark = get_spark("transform-flatten-upsert")

  raw_df = load_raw_table(spark, raw_table_path)
  new_flat_df = build_flat_dataframe(raw_df)

  existing_df = load_existing_csv(spark, output_csv)
  added = count_new_ids(existing_df, new_flat_df)

  merged_df = upsert_by_id(existing_df, new_flat_df)
  write_csv_atomic(merged_df, output_csv)

  total = merged_df.count()
  print(f"Filas nuevas: {added} | Filas totales en {output_csv}: {total}")
  print(f"Columnas ({len(merged_df.columns)}): {merged_df.columns}")
  print(f"Guardado en: {output_csv}")

  print_quick_analysis(merged_df)
  return added, total


def main():
  raw_table_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_RAW_TABLE_PATH
  output_csv = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_OUTPUT_CSV
  run(raw_table_path=raw_table_path, output_csv=output_csv)


if __name__ == "__main__":
  main()
