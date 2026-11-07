# Couche d'ingestion

## Rôle
Le producer Python lit le fichier Parquet NYC Yellow Taxi et publie chaque course
comme un événement JSON dans le topic Kafka `taxi-trips`.

## Événement publié
Chaque message contient :
- `event_id` : identifiant unique (ex. `yellow-2025-01-000000001`), aussi utilisé comme clé Kafka
- `event_timestamp` : date de prise en charge (`tpep_pickup_datetime`), au format ISO 8601
- `VendorID`, `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `passenger_count`,
  `trip_distance`, `PULocationID`, `DOLocationID`, `fare_amount`, `total_amount`

Les événements sont triés par date de prise en charge avant l'envoi.

## Lancer le producer
```
python scripts/download_dataset.py
python producer/producer.py --limit 1000
```

Options :
- `--limit N` : nombre d'événements (défaut : 1000)
- `--all` : rejoue toutes les lignes du fichier
- `--delay-seconds S` : pause entre deux messages, pour simuler un flux
- `--bootstrap-server` : défaut `localhost:9094` (listener externe de Kafka)
- `--topic` : défaut `taxi-trips`

## Vérifier la publication
```
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic taxi-trips --from-beginning --max-messages 5

docker exec -it kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server kafka:9092 --topic taxi-trips
```
Le total des offsets doit correspondre au nombre d'événements envoyés.

## Remarque pour la partie ML
`tpep_dropoff_datetime`, `fare_amount` et `total_amount` ne sont pas connus au moment
de la prise en charge : ils ne doivent pas servir de variables explicatives si la
prédiction se fait au départ de la course.
