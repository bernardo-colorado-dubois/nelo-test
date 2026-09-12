"""
DAG del ETL de nelo-test: envía read_queue.py, transform_messages.py y
export_csv.py al cluster de Spark standalone (spark-master:7077) vía
SparkSubmitOperator, uno detrás del otro.

Los tres scripts corren tal cual viven en la raíz del repo (montados de
solo lectura en /opt/spark-apps/, ver docker-compose.yaml) — nada de
código específico de Airflow adentro de ellos. Las rutas de datos que
usan dentro del cluster (/opt/spark-data, /opt/spark-output) son las
mismas carpetas data/ y output/ del repo, montadas también ahí.
"""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

RAW_TABLE_PATH = "/opt/spark-data/raw_messages"
FLAT_TABLE_PATH = "/opt/spark-data/items_flat"
OUTPUT_CSV_PATH = "/opt/spark-output/items_flat.csv"

# Parámetros operativos del poll a SQS (read_queue.py); el DAG es quien
# decide estos valores, no el script — ver VISIBILITY_TIMEOUT en
# read_queue.py para el único que sigue fijo a propósito.
MAX_MESSAGES_PER_POLL = "10"
WAIT_TIME_SECONDS = "20"

dag = DAG(
  dag_id="etl_pipeline",
  schedule=None,
  start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
  catchup=False,
  tags=["spark", "etl", "sqs"],
)

read_queue = SparkSubmitOperator(
  task_id="read_queue",
  application="/opt/spark-apps/read_queue.py",
  conn_id="spark_default",
  application_args=[
    "--table-path", RAW_TABLE_PATH,
    "--max-messages-per-poll", MAX_MESSAGES_PER_POLL,
    "--wait-time-seconds", WAIT_TIME_SECONDS,
  ],
  verbose=True,
  dag=dag,
)

transform_messages = SparkSubmitOperator(
  task_id="transform_messages",
  application="/opt/spark-apps/transform_messages.py",
  conn_id="spark_default",
  application_args=[RAW_TABLE_PATH, FLAT_TABLE_PATH],
  verbose=True,
  dag=dag,
)

export_csv = SparkSubmitOperator(
  task_id="export_csv",
  application="/opt/spark-apps/export_csv.py",
  conn_id="spark_default",
  application_args=[FLAT_TABLE_PATH, OUTPUT_CSV_PATH],
  verbose=True,
  dag=dag,
)

read_queue >> transform_messages >> export_csv
