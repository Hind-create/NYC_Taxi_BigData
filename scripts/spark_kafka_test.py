from pyspark.sql import SparkSession

KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "taxi-trips"


def main():
    spark = (
        SparkSession.builder
        .appName("NYC-Taxi-Spark-Kafka-Connectivity-Test")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        print(f"Lecture batch Kafka : {KAFKA_BOOTSTRAP}/{KAFKA_TOPIC}")

        messages = (
            spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )

        count = messages.count()
        print(f"Nombre de messages Kafka lus par Spark : {count}")

        messages.selectExpr(
            "CAST(key AS STRING) AS message_key",
            "CAST(value AS STRING) AS message_value",
            "timestamp AS kafka_timestamp",
            "partition",
            "offset",
        ).show(10, truncate=False)

        if count == 0:
            print(
                "Aucun message disponible. Lance le Producer puis relance ce test."
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
