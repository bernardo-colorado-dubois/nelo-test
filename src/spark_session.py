import os

from pyspark.sql import SparkSession


def get_spark(app_name):
  # Fuera del stack de Docker no está seteada -> local[*] (make pipeline).
  # Dentro del stack, docker-compose.yaml la fija a spark://spark-master:7077.
  master_url = os.environ.get("SPARK_MASTER_URL", "local[*]")

  builder = (
    SparkSession.builder
    .appName(app_name)
    .master(master_url)
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.ui.showConsoleProgress", "false")
    # Algorithm v2 commitea cada archivo directo a destino en vez de renombrar
    # todo el directorio de staging al final; v1 (default) falla con
    # "Failed to rename ..." al escribir sobre volúmenes bind-mounted de
    # Docker (visto corriendo read_queue.py dentro del stack, no en local).
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    # Hadoop aplica su propio umask (default 022) via setPermission() al
    # escribir a file://, sin importar el umask del proceso/contenedor.
    # Sin esto, lo escrito en /opt/spark-data queda con permisos que un
    # contenedor con otro UID no puede sobreescribir/borrar después.
    .config("spark.hadoop.fs.permissions.umask-mode", "000")
  )

  if master_url == "local[*]":
    # Driver y "executor" son el mismo proceso en local[*]; fijar 127.0.0.1
    # evita que Spark intente resolver el hostname real de la máquina.
    # Contra el cluster real (Docker), Spark debe auto-detectar el hostname
    # del contenedor para que spark-worker pueda devolverle la conexión.
    builder = builder.config("spark.driver.host", "127.0.0.1")

  return builder.getOrCreate()
