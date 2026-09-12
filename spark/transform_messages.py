import os
import shutil
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from src.schemas import CATEGORY_FIELD_CANDIDATES, EVENT_FIELDS, ITEM_FIELDS

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_RAW_TABLE_PATH = os.path.join(PROJECT_ROOT, "data", "raw_messages")
DEFAULT_FLAT_TABLE_PATH = os.path.join(PROJECT_ROOT, "data", "items_flat")

if __name__ == "__main__":
  raw_table_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_RAW_TABLE_PATH
  flat_table_path = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_FLAT_TABLE_PATH

  # 0. Spark local[*] fuera de Docker (make pipeline); dentro del stack,
  #    docker-compose.yaml fija SPARK_MASTER_URL al cluster real.
  master_url = os.environ.get("SPARK_MASTER_URL", "local[*]")
  spark_builder = (
    SparkSession.builder
    .appName("transform-flatten-upsert")
    .master(master_url)
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.ui.showConsoleProgress", "false")
    # Algorithm v2 + umask 000: sin esto, escribir la tabla parquet sobre
    # un volumen bind-mounted de Docker falla con "Failed to rename ..."
    # (ver CLAUDE.md, "Decisiones no obvias" del stack).
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    .config("spark.hadoop.fs.permissions.umask-mode", "000")
  )
  if master_url == "local[*]":
    # Driver y "executor" son el mismo proceso en local[*]; fijar 127.0.0.1
    # evita que Spark intente resolver el hostname real de la máquina.
    spark_builder = spark_builder.config("spark.driver.host", "127.0.0.1")
  spark = spark_builder.getOrCreate()

  # 1. leemos la tabla cruda y sacamos, de cada mensaje, una fila por item de su lista de items.
  #    Un evento sin items (no es de e-commerce) queda igual como una fila, con el item en null.
  raw_df = spark.read.parquet(raw_table_path)
  event_cols = [F.col(f"body.{field}").alias(field) for field in EVENT_FIELDS]
  base = raw_df.select("message_id", "received_at", *event_cols, F.col("body.items").alias("items"))
  exploded_base = base.select(
    "message_id", "received_at", *EVENT_FIELDS,
    F.explode_outer("items").alias("item"),
  )
  item_cols = [F.col(f"item.{field}").alias(field) for field in ITEM_FIELDS]
  exploded = exploded_base.select(
    "message_id", "received_at", *EVENT_FIELDS, *item_cols,
    F.col("item.item_params").alias("item_params"),
  )

  # 2. a cada fila le asignamos un id fijo (hash de message_id + item_id), para poder
  #    reprocesar el mismo mensaje muchas veces sin que se dupliquen filas en la tabla final.
  key_expr = F.concat_ws("::", F.col("message_id"), F.coalesce(F.col("item_id"), F.lit("__no_item__")))
  exploded = exploded.withColumn("id", F.sha2(key_expr, 256))

  # 3. item_params llega como una lista de {key, value}; la convertimos en columnas sueltas,
  #    una por cada key distinta que aparezca en los datos (ej. "totalPrice", "discounts", ...).
  # item_params.value trae 4 variantes (string/int/float/double); solo una viene poblada por fila.
  value_variants = [
    F.col(f"param.value.{column}").cast("string")
    for column in ["string_value", "int_value", "float_value", "double_value"]
  ]
  params = (
    exploded
    .select("id", F.explode("item_params").alias("param"))
    .select("id", F.col("param.key").alias("key"), F.coalesce(*value_variants).alias("value"))
    .filter(F.col("key").isNotNull())
  )
  pivoted = params.groupBy("id").pivot("key").agg(F.first("value"))

  # 4. juntamos la fila base (sin la lista item_params, ya no hace falta) con esas columnas nuevas.
  new_flat_df = exploded.drop("item_params").join(pivoted, on="id", how="left")

  # 5. tabla existente (si la hay) + cuántos id son realmente nuevos
  existing_df = spark.read.parquet(flat_table_path) if os.path.exists(flat_table_path) else None
  if existing_df is None:
    added = new_flat_df.select("id").distinct().count()
    combined = new_flat_df
  else:
    added = new_flat_df.join(existing_df, "id", "left_anti").select("id").distinct().count()
    combined = existing_df.unionByName(new_flat_df, allowMissingColumns=True)

  # 6. upsert: por id, se queda con la fila más reciente (received_at desc). Cacheamos y
  #    materializamos antes de escribir: merged_df todavía depende de leer flat_table_path
  #    (vía existing_df) y el paso 7 borra y reemplaza esa carpeta — sin el cache, cualquier
  #    acción posterior (el count, el análisis) reevaluaría ese plan perezoso contra archivos
  #    que el propio rename ya hizo desaparecer (FileNotFoundException).
  window = Window.partitionBy("id").orderBy(F.col("received_at").desc())
  merged_df = (
    combined
    .withColumn("_rn", F.row_number().over(window))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
  ).cache()
  total = merged_df.count()

  # 7. escritura atómica: a un directorio temporal y recién al final se reemplaza la tabla
  tmp_path = f"{flat_table_path}.tmp"
  if os.path.exists(tmp_path):
    shutil.rmtree(tmp_path)
  merged_df.write.mode("overwrite").parquet(tmp_path)
  if os.path.exists(flat_table_path):
    shutil.rmtree(flat_table_path)
  os.rename(tmp_path, flat_table_path)

  print(f"Filas nuevas: {added} | Filas totales en {flat_table_path}: {total}")
  print(f"Columnas ({len(merged_df.columns)}): {merged_df.columns}")

  # 8. un vistazo rápido a los datos: cuántos eventos hay de cada tipo, y de cada categoría.
  print("\n--- Conteo por event_name ---")
  merged_df.groupBy("event_name").count().orderBy(F.desc("count")).show(n=100, truncate=False)

  category_field = next(
    (field for field in CATEGORY_FIELD_CANDIDATES
     if field in merged_df.columns and merged_df.filter(F.col(field).isNotNull()).limit(1).count() > 0),
    None,
  )
  if category_field:
    print(f"\n--- Conteo por categoría ({category_field}) ---")
    merged_df.groupBy(category_field).count().orderBy(F.desc("count")).show(n=100, truncate=False)
  else:
    print("\n--- Conteo por categoría ---")
    print("No hay ningún campo de categoría con datos.")
