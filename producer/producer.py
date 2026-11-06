import argparse
import json
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer

DEFAULT_DATA_PATH = "data/yellow_tripdata_2025-01.parquet"
DEFAULT_BOOTSTRAP_SERVER = "localhost:9094"
DEFAULT_TOPIC = "taxi-trips"

SOURCE_COLUMNS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
    "total_amount",
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Publie les courses NYC Yellow Taxi dans Kafka."
    )
    parser.add_argument("--data", default=DEFAULT_DATA_PATH, help="Fichier Parquet source.")
    parser.add_argument("--bootstrap-server", default=DEFAULT_BOOTSTRAP_SERVER)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Nombre maximum d'événements (défaut : 1000). Ignoré avec --all.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Rejoue toutes les lignes valides du fichier Parquet.",
    )
    parser.add_argument(
        "--delay-seconds",
        type=float,
        default=0.0,
        help="Pause entre deux messages (défaut : 0 seconde).",
    )
    return parser.parse_args()


def json_value(value):
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, float) and pd.isna(value):
        return None
    return value


def main():
    args = parse_args()

    if args.limit < 1 and not args.all:
        raise ValueError("--limit doit être supérieur ou égal à 1.")
    if args.delay_seconds < 0:
        raise ValueError("--delay-seconds ne peut pas être négatif.")

    data_path = Path(args.data)
    if not data_path.is_file():
        raise FileNotFoundError(
            f"Dataset introuvable : {data_path}. "
            "Exécute d'abord python scripts/download_dataset.py."
        )

    print(f"Lecture du dataset : {data_path}")
    df = pd.read_parquet(data_path, columns=SOURCE_COLUMNS)
    # Conserver le numéro de ligne du fichier avant le filtrage des dates invalides.
    df["source_row_number"] = df.index + 1
    df["tpep_pickup_datetime"] = pd.to_datetime(
        df["tpep_pickup_datetime"], errors="coerce"
    )
    df = df.dropna(subset=["tpep_pickup_datetime"])
    df = df.sort_values("tpep_pickup_datetime", kind="mergesort")

    if not args.all:
        df = df.head(args.limit)

    print(f"Événements à publier : {len(df)}")
    print(f"Kafka : {args.bootstrap_server}, topic : {args.topic}")
    print(f"Rejeu complet : {'oui' if args.all else 'non'}")
    print(f"Délai entre messages : {args.delay_seconds} seconde(s)")

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_server,
        value_serializer=lambda event: json.dumps(
            event, ensure_ascii=False, allow_nan=False
        ).encode("utf-8"),
        acks="all",
        retries=5,
    )

    sent = 0
    try:
        for row in df.itertuples(index=False):
            source_row_number = int(row.source_row_number)
            pickup = row.tpep_pickup_datetime
            event = {
                "event_id": f"yellow-2025-01-{source_row_number:09d}",
                "event_timestamp": pickup.isoformat(),
            }
            for column in SOURCE_COLUMNS:
                event[column] = json_value(getattr(row, column))

            producer.send(
                args.topic,
                key=event["event_id"].encode("utf-8"),
                value=event,
            )
            sent += 1

            if args.delay_seconds:
                time.sleep(args.delay_seconds)

        producer.flush()
        print(f"Publication terminée : {sent} événements envoyés.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
