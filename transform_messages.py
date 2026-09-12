import os
import sys

from src.flatten import print_quick_analysis, upsert_flat_csv
from src.spark_session import get_spark

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_RAW_TABLE_PATH = os.path.join(BASE_DIR, "data", "raw_messages")
DEFAULT_OUTPUT_CSV = os.path.join(BASE_DIR, "output", "items_flat.csv")


def run(raw_table_path=DEFAULT_RAW_TABLE_PATH, output_csv=DEFAULT_OUTPUT_CSV):
  spark = get_spark("transform-flatten-upsert")

  merged_df, added = upsert_flat_csv(spark, raw_table_path, output_csv)
  total = merged_df.count()

  print(f"Filas nuevas: {added} | Filas totales en {output_csv}: {total}")
  print(f"Columnas ({len(merged_df.columns)}): {merged_df.columns}")
  print(f"Guardado en: {output_csv}")

  print_quick_analysis(merged_df)
  return added, total


def main():
  raw_table_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_RAW_TABLE_PATH
  output_csv = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_OUTPUT_CSV
  run(raw_table_path=raw_table_path, output_csv=output_csv)


if __name__ == "__main__":
  main()
