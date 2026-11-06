from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, LongType, DoubleType, StringType
)

KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "taxi-trips"
OUTPUT_PATH = "hdfs://namenode:9000/taxi/stream/raw"
CHECKPOINT_PATH = "hdfs://namenode:9000/taxi/stream/checkpoint"

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("source_row_number", LongType(), True),
    StructField("VendorID", LongType(), True),
    StructField("tpep_pickup_datetime", StringType(), True),
    StructField("tpep_dropoff_datetime", StringType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("PULocationID", LongType(), True),
    StructField("DOLocationID", LongType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
])


def main():
    spark = (
        SparkSession.builder
        .appName("NYC-Taxi-Kafka-Structured-Streaming")
        .config("spark.hadoop.dfs.replication", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "5000")
        .load()
    )

    parsed = (
        kafka_stream
        .select(from_json(col("value").cast("string"), EVENT_SCHEMA).alias("event"))
        .select("event.*")
        .filter(col("event_id").isNotNull())
        .filter(col("event_timestamp").isNotNull())
        .withColumn("event_timestamp", to_timestamp(col("event_timestamp")))
        .withColumn(
            "tpep_pickup_datetime",
            to_timestamp(col("tpep_pickup_datetime"))
        )
        .withColumn(
            "tpep_dropoff_datetime",
            to_timestamp(col("tpep_dropoff_datetime"))
        )
    )

    query = (
        parsed.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="10 seconds")
        .start()
    )

    print(f"Streaming Kafka {KAFKA_BOOTSTRAP}/{KAFKA_TOPIC}")
    print(f"Sortie Parquet HDFS : {OUTPUT_PATH}")
    print(f"Checkpoint HDFS : {CHECKPOINT_PATH}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
