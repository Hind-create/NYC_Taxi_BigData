import sys
from pyspark.sql import SparkSession

DEFAULT_INPUT = "hdfs://namenode:9000/taxi/raw/"

def main():
    input_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_INPUT

    spark = (
        SparkSession.builder
        .appName("NYC-Taxi-Batch-Ingestion")
        .config("spark.hadoop.dfs.replication", "2")
        .getOrCreate()
    )

    try:
        print(f"Lecture du dataset Parquet depuis : {input_path}")
        df = spark.read.parquet(input_path)

        print("\n===== SCHEMA =====")
        df.printSchema()

        print("\n===== NOMBRE DE LIGNES =====")
        print(df.count())

        print("\n===== APERCU DES 10 PREMIERES LIGNES =====")
        df.show(10, truncate=False)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
