from pyspark.sql import SparkSession


def get_spark(app_name):
  return (
    SparkSession.builder
    .appName(app_name)
    .master("local[*]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.ui.showConsoleProgress", "false")
    .config("spark.driver.host", "127.0.0.1")
    .getOrCreate()
  )
