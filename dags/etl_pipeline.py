"""
DAG del ETL de nelo-test: envía read_queue.py, transform_messages.py y
export_csv.py al cluster de Spark standalone (spark-master:7077) vía
SparkSubmitOperator, uno detrás del otro, y por último sube el CSV
resultante a Google Drive con un PythonOperator (upload_to_drive, en este
mismo directorio — no es un job de Spark, ver ese archivo).

Los tres scripts de Spark corren tal cual viven en la raíz del repo (montados
de solo lectura en /opt/spark-apps/, ver docker-compose.yaml) — nada de
código específico de Airflow adentro de ellos. Las rutas de datos que
usan dentro del cluster (/opt/spark-data, /opt/spark-output) son las
mismas carpetas data/ y output/ del repo, montadas también ahí.
"""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.python import PythonOperator

from upload_to_drive import upload_to_drive

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

# No es un job de Spark (sin SparkSubmitOperator): corre como driver de
# Airflow en airflow-worker-1, mismo contenedor que ya tiene el CSV montado
# en /opt/spark-output y las credenciales de Drive (ver docker-compose.yaml).
upload_drive = PythonOperator(
  task_id="upload_to_drive",
  python_callable=upload_to_drive,
  op_kwargs={"csv_path": OUTPUT_CSV_PATH, "drive_filename": "nelo_dashboard"},
  dag=dag,
)

read_queue >> transform_messages >> export_csv >> upload_drive
