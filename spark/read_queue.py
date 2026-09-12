import json
import os
import shutil
import sys
from datetime import datetime, timezone

import boto3
from dotenv import load_dotenv
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from src.pseudo_json import expand_nested_fields
from src.schemas import RECORD_SCHEMA

load_dotenv()

QUEUE_URL = os.environ["SQS_QUEUE_URL"]
REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

MAX_MESSAGES_PER_POLL = 10
WAIT_TIME_SECONDS = 20

# Script de solo lectura: nunca llama a delete_message.
# VisibilityTimeout=0 para no ocultar mensajes a otros consumidores.
VISIBILITY_TIMEOUT = 0

NESTED_FIELDS = ["items"]

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_TABLE_PATH = os.path.join(PROJECT_ROOT, "data", "raw_messages")


def run(table_path=DEFAULT_TABLE_PATH, loop=False):
  sqs = boto3.client("sqs", region_name=REGION)

  # 0. Spark local[*] fuera de Docker (make pipeline); dentro del stack,
  #    docker-compose.yaml fija SPARK_MASTER_URL al cluster real.
  master_url = os.environ.get("SPARK_MASTER_URL", "local[*]")
  spark_builder = (
    SparkSession.builder
    .appName("read-queue-upsert")
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

  if loop:
    print(f"Leyendo continuamente de {QUEUE_URL} (solo lectura, sin borrado)...")
    print(f"Upsert de mensajes nuevos en tabla parquet: {table_path}")

  while True:
    # 1. poll a SQS
    response = sqs.receive_message(
      QueueUrl=QUEUE_URL,
      MaxNumberOfMessages=MAX_MESSAGES_PER_POLL,
      WaitTimeSeconds=WAIT_TIME_SECONDS,
      VisibilityTimeout=VISIBILITY_TIMEOUT,
      MessageAttributeNames=["All"],
      AttributeNames=["All"],
    )

    records = []
    for message in response.get("Messages", []):
      try:
        body = json.loads(message["Body"])
        body = expand_nested_fields(body, NESTED_FIELDS)
      except json.JSONDecodeError:
        body = {}

      record = {
        "message_id": message["MessageId"],
        "received_at": datetime.now(timezone.utc).isoformat(),
        "body": body,
      }
      print(json.dumps(record, ensure_ascii=False, indent=2))
      print("-" * 80)
      records.append(record)

    added = 0
    if records:
      # 2. records crudos -> DataFrame con schema explícito (evita NullType en columnas siempre-null)
      json_lines = [json.dumps(record, ensure_ascii=False) for record in records]
      new_df = spark.read.schema(RECORD_SCHEMA).json(spark.sparkContext.parallelize(json_lines))

      # 3. tabla existente (si la hay) + cuántos message_id son realmente nuevos
      existing_df = spark.read.parquet(table_path) if os.path.exists(table_path) else None
      if existing_df is None:
        added = new_df.select("message_id").distinct().count()
        combined = new_df
      else:
        added = new_df.join(existing_df, "message_id", "left_anti").select("message_id").distinct().count()
        combined = existing_df.unionByName(new_df, allowMissingColumns=True)

      # 4. upsert: por message_id, se queda con la lectura más reciente (received_at desc)
      window = Window.partitionBy("message_id").orderBy(F.col("received_at").desc())
      merged_df = (
        combined
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
      )

      # 5. escritura atómica: a un directorio temporal y recién al final se reemplaza la tabla
      tmp_path = f"{table_path}.tmp"
      if os.path.exists(tmp_path):
        shutil.rmtree(tmp_path)
      merged_df.write.mode("overwrite").parquet(tmp_path)
      if os.path.exists(table_path):
        shutil.rmtree(table_path)
      os.rename(tmp_path, table_path)

    print(f"{len(records)} mensaje(s) leído(s), {added} nuevo(s) upserted en {table_path}.")

    if not loop:
      return len(records), added


def main():
  loop = "--loop" in sys.argv
  table_path = DEFAULT_TABLE_PATH
  if "--table-path" in sys.argv:
    table_path = sys.argv[sys.argv.index("--table-path") + 1]
  run(table_path=table_path, loop=loop)


if __name__ == "__main__":
  main()
