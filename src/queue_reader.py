import json
import os
from datetime import datetime, timezone

import boto3

from src.pseudo_json import expand_nested_fields
from src.schemas import RECORD_SCHEMA
from src.spark_io import count_new_keys, upsert_by_key, write_parquet_atomic

MAX_MESSAGES_PER_POLL = 10
WAIT_TIME_SECONDS = 20

# Script de solo lectura: nunca llama a delete_message.
# VisibilityTimeout=0 para no ocultar mensajes a otros consumidores.
VISIBILITY_TIMEOUT = 0

NESTED_FIELDS = ["items"]


def get_sqs_client(region):
  return boto3.client("sqs", region_name=region)


def fetch_messages(sqs, queue_url, verbose=True):
  """Hace un poll a SQS y arma los records (dict) listos para convertir a DataFrame."""
  response = sqs.receive_message(
    QueueUrl=queue_url,
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

    if verbose:
      print(json.dumps(record, ensure_ascii=False, indent=2))
      print("-" * 80)
    records.append(record)

  return records


def records_to_dataframe(spark, records):
  json_lines = [json.dumps(record, ensure_ascii=False) for record in records]
  return spark.read.schema(RECORD_SCHEMA).json(spark.sparkContext.parallelize(json_lines))


def load_existing_table(spark, table_path):
  if os.path.exists(table_path):
    return spark.read.parquet(table_path)
  return None


def upsert_messages(spark, table_path, records):
  """Hace upsert de los records nuevos contra la tabla parquet, por message_id."""
  if not records:
    return 0

  new_df = records_to_dataframe(spark, records)
  existing_df = load_existing_table(spark, table_path)

  added = count_new_keys(existing_df, new_df, "message_id")
  merged_df = upsert_by_key(existing_df, new_df, key_col="message_id", order_col="received_at")
  write_parquet_atomic(merged_df, table_path)

  return added
