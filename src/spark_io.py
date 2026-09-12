import os
import shutil

from pyspark.sql import Window
from pyspark.sql import functions as F


def upsert_by_key(existing_df, new_df, key_col, order_col):
  """Combina existing_df + new_df quedándose con la fila más reciente por key_col."""
  combined = new_df if existing_df is None else existing_df.unionByName(new_df, allowMissingColumns=True)

  window = Window.partitionBy(key_col).orderBy(F.col(order_col).desc())
  return (
    combined
    .withColumn("_rn", F.row_number().over(window))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
  )


def count_new_keys(existing_df, new_df, key_col):
  """Cuenta cuántas claves de new_df no existían todavía en existing_df."""
  if existing_df is None:
    return new_df.select(key_col).distinct().count()
  return new_df.join(existing_df, key_col, "left_anti").select(key_col).distinct().count()


def write_parquet_atomic(df, target_path):
  """Escribe df como parquet en target_path sin dejar la tabla a medio escribir."""
  tmp_path = f"{target_path}.tmp"
  if os.path.exists(tmp_path):
    shutil.rmtree(tmp_path)

  df.write.mode("overwrite").parquet(tmp_path)

  if os.path.exists(target_path):
    shutil.rmtree(target_path)
  os.rename(tmp_path, target_path)


def write_single_csv_atomic(df, target_path):
  """Escribe df como un único archivo CSV en target_path (Spark solo sabe escribir directorios)."""
  tmp_dir = f"{target_path}.tmp_dir"
  if os.path.exists(tmp_dir):
    shutil.rmtree(tmp_dir)

  df.coalesce(1).write.mode("overwrite").option("header", True).csv(tmp_dir)

  part_file = next(name for name in os.listdir(tmp_dir) if name.startswith("part-") and name.endswith(".csv"))
  os.makedirs(os.path.dirname(target_path) or ".", exist_ok=True)
  if os.path.exists(target_path):
    os.remove(target_path)
  shutil.move(os.path.join(tmp_dir, part_file), target_path)
  shutil.rmtree(tmp_dir)
