# Architecture du projet NYC Taxi Big Data

## 1. Vue générale

Le projet comprend deux voies d'ingestion distinctes : une ingestion batch des fichiers Parquet et une ingestion streaming des événements Kafka.

## 2. Voie batch : Parquet vers HDFS puis Spark

1. Le fichier `yellow_tripdata_2025-01.parquet` est téléchargé depuis NYC TLC.
2. Le fichier est transféré vers le NameNode et chargé dans HDFS sous `/taxi/raw/`.
3. Le job `scripts/batch_ingest.py` est soumis au cluster via `spark-submit --master spark://spark-master:7077`.
4. Spark lit le Parquet depuis HDFS, affiche le schéma, le nombre de lignes et un aperçu.

## 3. Voie streaming : Producer vers Kafka, Spark Structured Streaming et HDFS

1. `producer/producer.py` lit le Parquet sur le poste hôte.
2. Le Producer publie des événements JSON dans le topic Kafka `taxi-trips`.
3. Kafka est accessible depuis les conteneurs à `kafka:9092` et depuis Windows à `localhost:9094`.
4. `scripts/stream_ingest.py` consomme le topic avec Spark Structured Streaming.
5. Les événements sont écrits au format Parquet dans `hdfs://namenode:9000/taxi/stream/raw`.
6. Le checkpoint de reprise est conservé dans `hdfs://namenode:9000/taxi/stream/checkpoint`.

## 4. Services et noms réels des conteneurs

| Service | Rôle | Conteneur |
|---|---|---|
| namenode | Métadonnées HDFS | `hdfs-namenode` |
| datanode1 | Blocs HDFS | `hdfs-datanode1` |
| datanode2 | Répliques HDFS | `hdfs-datanode2` |
| spark-master | Coordination Spark et soumission des jobs | `spark-master` |
| spark-worker1 | Exécution Spark | `spark-worker1` |
| spark-worker2 | Exécution Spark | `spark-worker2` |
| kafka | Broker Kafka KRaft | `kafka` |
| kafka-init | Création automatique du topic | `kafka-init` |

## 5. Stockage et réplication

Le facteur de réplication HDFS est configuré à 2. Les DataNodes utilisent deux volumes Docker nommés distincts : `datanode1_data` et `datanode2_data`.

## 6. Statut

Les fichiers de configuration et les jobs fournis ici constituent la version à déployer et à tester. Ne considérer les voies batch et streaming comme validées qu'après les tests et captures décrits dans le README.

## 7. Éléments prévus pour les semaines suivantes

PostgreSQL est envisagé pour les semaines 3 et 4 afin de stocker les résultats destinés à la restitution. Il n'est pas inclus dans le Compose du Livrable 1. Le développement et l'évaluation du modèle de régression constituent également une étape ultérieure, après validation du cas métier.


## 8. Contrat d'un événement

Chaque message du topic `taxi-trips` représente une course source. Le Producer ajoute :
- `event_id` : identifiant technique stable basé sur le numéro de ligne d'origine ;
- `event_timestamp` : heure de prise en charge (`tpep_pickup_datetime`) au format ISO 8601 ;
- `source_row_number` : numéro de ligne dans le fichier source.

Les champs métier envoyés sont `VendorID`, `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `passenger_count`, `trip_distance`, `PULocationID`, `DOLocationID`, `fare_amount` et `total_amount`. Les valeurs absentes sont `null`. Les événements sont triés chronologiquement par heure de prise en charge.

## 9. Reproductibilité du dataset et tests de connectivité

Le fichier Parquet de janvier 2025 peut être téléchargé avec `scripts/download_dataset.py` depuis l'URL TLC :
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet

Le job `scripts/spark_kafka_test.py`, lancé par `spark-submit` sur `spark://spark-master:7077`, vérifie que le cluster Spark peut lire les messages du topic Kafka. Ce test est distinct du pipeline Structured Streaming qui écrit les événements en Parquet dans HDFS.
