
import pandas as pd
import json
from kafka import KafkaProducer

# 1. Configuration
DATA_PATH = "data/yellow_tripdata_2025-01.parquet"
KAFKA_SERVER = "localhost:9094"
TOPIC = "taxi-trips"
LIMIT = 1000

# 2. Colonnes à utiliser
columns = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
    "total_amount"
]

# 3. Lecture des données
print("Lecture du dataset...")
df = pd.read_parquet(DATA_PATH, columns=columns)
df = df.head(LIMIT)

# 4. Connexion à Kafka
print("Connexion à Kafka...")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

# 5. Publication des événements
sent = 0

try:
    for _, row in df.iterrows():
        event = {}

        for col in columns:
            value = row[col]

            if pd.isna(value):
                value = None
            elif isinstance(value, pd.Timestamp):
                value = value.isoformat()
            elif hasattr(value, "item"):
                value = value.item()

            event[col] = value

        producer.send(TOPIC, value=event)
        sent += 1

    producer.flush()

    print(f"Publication terminee : {sent} evenements envoyes dans Kafka.")

finally:
    producer.close()
