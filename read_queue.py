import os
import sys

from dotenv import load_dotenv

from src.queue_reader import fetch_messages, get_sqs_client, upsert_messages
from src.spark_session import get_spark

load_dotenv()

QUEUE_URL = os.environ["SQS_QUEUE_URL"]
REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_TABLE_PATH = os.path.join(BASE_DIR, "data", "raw_messages")


def run(table_path=DEFAULT_TABLE_PATH, loop=False):
  sqs = get_sqs_client(REGION)
  spark = get_spark("read-queue-upsert")

  def process_batch():
    records = fetch_messages(sqs, QUEUE_URL)
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
