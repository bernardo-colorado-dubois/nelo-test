import os
import shutil
import sys

from pyspark.sql import SparkSession

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_FLAT_TABLE_PATH = os.path.join(PROJECT_ROOT, "data", "items_flat")
DEFAULT_OUTPUT_CSV = os.path.join(PROJECT_ROOT, "output", "items_flat.csv")


if __name__ == "__main__":
  flat_table_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_FLAT_TABLE_PATH
  output_csv = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_OUTPUT_CSV

  # 0. Spark local[*] fuera de Docker (make pipeline); dentro del stack,
  #    docker-compose.yaml fija SPARK_MASTER_URL al cluster real.
  master_url = os.environ.get("SPARK_MASTER_URL", "local[*]")
  spark_builder = (
    SparkSession.builder
    .appName("export-flat-csv")
    .master(master_url)
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.ui.showConsoleProgress", "false")
    # Algorithm v2 + umask 000: sin esto, escribir el CSV final sobre un
    # volumen bind-mounted de Docker falla con "Failed to rename ..."
    # (ver CLAUDE.md, "Decisiones no obvias" del stack).
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    .config("spark.hadoop.fs.permissions.umask-mode", "000")
  )
  if master_url == "local[*]":
    # Driver y "executor" son el mismo proceso en local[*]; fijar 127.0.0.1
    # evita que Spark intente resolver el hostname real de la máquina.
    spark_builder = spark_builder.config("spark.driver.host", "127.0.0.1")
  spark = spark_builder.getOrCreate()

  # 1. leemos la tabla parquet ya upserteada por transform_messages.py: una fila
  #    por id, sin duplicados. Este script no hace merge de nada, solo vuelca
  #    el estado actual completo a CSV.
  flat_df = spark.read.parquet(flat_table_path)

  # 2. lo volcamos como un único archivo CSV. Spark solo sabe escribir directorios
  #    con varias partes, así que escribimos a una carpeta temporal y nos quedamos
  #    con la única parte generada, moviéndola al nombre de archivo final.
  tmp_dir = f"{output_csv}.tmp_dir"
  if os.path.exists(tmp_dir):
    shutil.rmtree(tmp_dir)
  flat_df.coalesce(1).write.mode("overwrite").option("header", True).csv(tmp_dir)
  part_file = next(name for name in os.listdir(tmp_dir) if name.startswith("part-") and name.endswith(".csv"))
  os.makedirs(os.path.dirname(output_csv) or ".", exist_ok=True)
  if os.path.exists(output_csv):
    os.remove(output_csv)
  shutil.move(os.path.join(tmp_dir, part_file), output_csv)
  shutil.rmtree(tmp_dir)

  total = flat_df.count()
  print(f"Filas exportadas: {total} | Guardado en: {output_csv}")
