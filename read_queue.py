import json
import os
import shutil
import sys
from datetime import datetime, timezone

import boto3
from dotenv import load_dotenv
from pyspark.sql import Window
from pyspark.sql import functions as F

from src.pseudo_json import expand_nested_fields
from src.schemas import RECORD_SCHEMA
from src.spark_session import get_spark

load_dotenv()

QUEUE_URL = os.environ["SQS_QUEUE_URL"]
REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

MAX_MESSAGES_PER_POLL = 10
WAIT_TIME_SECONDS = 20

# Script de solo lectura: nunca llama a delete_message.
# VisibilityTimeout=0 para no ocultar mensajes a otros consumidores.
VISIBILITY_TIMEOUT = 0

NESTED_FIELDS = ["items"]

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_TABLE_PATH = os.path.join(BASE_DIR, "data", "raw_messages")


def get_sqs_client():
  return boto3.client("sqs", region_name=REGION)


def fetch_messages(sqs):
  """Hace un poll a SQS y arma los records (dict) listos para convertir a DataFrame."""
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

  return records


def build_new_dataframe(spark, records):
  """DataFrame de Spark de los mensajes recién leídos, con el schema explícito de RECORD_SCHEMA."""
  json_lines = [json.dumps(record, ensure_ascii=False) for record in records]
  return spark.read.schema(RECORD_SCHEMA).json(spark.sparkContext.parallelize(json_lines))


def load_existing_table(spark, table_path):
  if os.path.exists(table_path):
    return spark.read.parquet(table_path)
  return None


def count_new_message_ids(existing_df, new_df):
  if existing_df is None:
    return new_df.select("message_id").distinct().count()
  return new_df.join(existing_df, "message_id", "left_anti").select("message_id").distinct().count()


def upsert_by_message_id(existing_df, new_df):
  """existing_df + new_df quedándose con la lectura más reciente (received_at) por message_id."""
  combined = new_df if existing_df is None else existing_df.unionByName(new_df, allowMissingColumns=True)

  window = Window.partitionBy("message_id").orderBy(F.col("received_at").desc())
  return (
    combined
    .withColumn("_rn", F.row_number().over(window))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
  )


def write_table_atomic(df, table_path):
  """Escribe la tabla a un directorio temporal y recién al final reemplaza table_path."""
  tmp_path = f"{table_path}.tmp"
  if os.path.exists(tmp_path):
    shutil.rmtree(tmp_path)

  df.write.mode("overwrite").parquet(tmp_path)

  if os.path.exists(table_path):
    shutil.rmtree(table_path)
  os.rename(tmp_path, table_path)


def upsert_messages(spark, table_path, records):
  if not records:
    return 0

  new_df = build_new_dataframe(spark, records)
  existing_df = load_existing_table(spark, table_path)

  added = count_new_message_ids(existing_df, new_df)
  merged_df = upsert_by_message_id(existing_df, new_df)
  write_table_atomic(merged_df, table_path)

  return added


def run(table_path=DEFAULT_TABLE_PATH, loop=False):
  sqs = get_sqs_client()
  spark = get_spark("read-queue-upsert")

  def process_batch():
    records = fetch_messages(sqs)
    added = upsert_messages(spark, table_path, records)
    return len(records), added

  if loop:
    print(f"Leyendo continuamente de {QUEUE_URL} (solo lectura, sin borrado)...")
    print(f"Upsert de mensajes nuevos en tabla parquet: {table_path}")
    while True:
      count, added = process_batch()
      print(f"{count} mensaje(s) leído(s), {added} nuevo(s) upserted en {table_path}.")
  else:
    count, added = process_batch()
    print(f"{count} mensaje(s) leído(s), {added} nuevo(s) upserted en {table_path}.")
    return count, added


def main():
  loop = "--loop" in sys.argv
  table_path = DEFAULT_TABLE_PATH
  if "--table-path" in sys.argv:
    table_path = sys.argv[sys.argv.index("--table-path") + 1]
  run(table_path=table_path, loop=loop)


if __name__ == "__main__":
  main()
