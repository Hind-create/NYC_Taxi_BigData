# NYC Taxi – Big Data Pipeline

**Dernière mise à jour : 06 novembre 2026**

## 1. Présentation

Projet Big Data basé sur les données officielles NYC TLC Yellow Taxi Trip Records. Le système met en place une infrastructure distribuée Docker avec Kafka, Spark et Hadoop HDFS, ainsi que deux voies d'ingestion : batch et streaming.

## 2. Cas métier et tâche ML

**Cas métier :** estimer le prix d'une course de taxi à New York au moment de la commande.

**Tâche ML unique :** régression supervisée.

- **Cible :** `fare_amount`.
- **Unité :** dollar américain (USD).
- **Variables disponibles au moment de la prédiction :** `PULocationID`, `DOLocationID`, heure de commande, jour de la semaine et `passenger_count`.
- **Variables exclues pour éviter la fuite de données :** durée, vitesse, heure d'arrivée (`tpep_dropoff_datetime`), pourboire (`tip_amount`) et montant total (`total_amount`).

La préparation ML prévue comprend un découpage temporel train/test (entraînement sur la partie chronologiquement antérieure et test sur la partie ultérieure), une baseline fondée sur la médiane du `fare_amount` du jeu d'entraînement, puis la comparaison de trois techniques : régression linéaire, Random Forest Regressor et Gradient Boosting Regressor. Une seule technique sera ensuite réglée par recherche d'hyperparamètres. Les métriques d'évaluation seront MAE et RMSE.

**Validation du cas métier par l'enseignante : en attente.** Le statut ne sera changé qu'après confirmation explicite de l'enseignante.

## 3. Source des données

Source officielle TLC : https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Lien direct exact du fichier de janvier 2025 :
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet

Le fichier n'est pas versionné dans GitHub (taille importante). Depuis la racine du projet, le téléchargement reproductible se fait avec :

```bash
python scripts/download_dataset.py
```

Le fichier est enregistré dans `data/yellow_tripdata_2025-01.parquet`. Le script ne le retélécharge pas s'il existe déjà.

## 4. Versions utilisées

| Élément | Version / image |
|---|---|
| Apache Spark | 3.5.3, construit depuis l'archive officielle dans `config/spark/Dockerfile` |
| Python dans l'image Spark | 3.11 (image de base `python:3.11-slim-bookworm`) |
| Hadoop HDFS | Image BDE `2.0.0-hadoop3.2.1-java8` (Hadoop 3.2.1) |
| Apache Kafka | `apache/kafka:3.9.1` |
| Docker Engine | [À COMPLÉTER : résultat de `docker version`] |
| Docker Compose | [À COMPLÉTER : résultat de `docker compose version`] |
| Python hôte (Producer) | [À COMPLÉTER : résultat de `python --version`] |

Le client Hadoop embarqué dans la distribution Spark et le serveur HDFS sont des composants distincts ; la compatibilité doit être confirmée par les tests d'accès HDFS.

## 5. Architecture et services

| Service Compose | Rôle | Nom réel du conteneur |
|---|---|---|
| `namenode` | Métadonnées HDFS | `hdfs-namenode` |
| `datanode1` | Stockage HDFS | `hdfs-datanode1` |
| `datanode2` | Stockage HDFS | `hdfs-datanode2` |
| `spark-master` | Coordination Spark | `spark-master` |
| `spark-worker1` | Exécution Spark | `spark-worker1` |
| `spark-worker2` | Exécution Spark | `spark-worker2` |
| `kafka` | Broker Kafka KRaft | `kafka` |
| `kafka-init` | Création automatique de `taxi-trips` | `kafka-init` |

- Spark : 1 Master et 2 Workers.
- HDFS : 1 NameNode et 2 DataNodes.
- Kafka : 1 broker, topic `taxi-trips`, 3 partitions.
- Réplication HDFS : facteur 2.
- Persistance : volumes distincts `datanode1_data` et `datanode2_data`.

### Interfaces web

- Spark Master : http://localhost:8080
- Spark Worker 1 : http://localhost:8081
- Spark Worker 2 : http://localhost:8082
- HDFS NameNode : http://localhost:9870

## 6. Flux d'ingestion

### 6.1 Batch : Parquet → HDFS → Spark

Le fichier Parquet est chargé dans `/taxi/raw/` sur HDFS. Le job `scripts/batch_ingest.py`, soumis au cluster Spark, lit les données, affiche le schéma, compte les lignes et présente un aperçu.

### 6.2 Streaming : Producer → Kafka → Spark Structured Streaming → HDFS

Le Producer Python publie des événements JSON dans `taxi-trips`. Spark Structured Streaming lit Kafka via `kafka:9092`, puis écrit des fichiers Parquet dans `/taxi/stream/raw/`. Le checkpoint est stocké dans `/taxi/stream/checkpoint/`.

Le Producer Windows utilise `localhost:9094`. Les applications dans le réseau Docker utilisent `kafka:9092`.

## 7. Format et sémantique des événements Kafka

Un événement représente **une course Yellow Taxi** du fichier source. Il est sérialisé en JSON UTF-8 et publié dans le topic `taxi-trips`.

- `event_id` : identifiant technique déterministe construit à partir du numéro de ligne d'origine dans le fichier (`yellow-2025-01-000000001`, etc.). Il permet de distinguer les événements du fichier, y compris lorsque plusieurs courses ont des valeurs métier identiques.
- `event_timestamp` : horodatage de l'événement, correspondant à `tpep_pickup_datetime`, au format ISO 8601.
- `source_row_number` : numéro de ligne d'origine (à partir de 1) dans le fichier Parquet.
- `VendorID` : identifiant du fournisseur de technologie.
- `tpep_pickup_datetime` et `tpep_dropoff_datetime` : dates/heures de prise en charge et de dépose.
- `passenger_count` : nombre de passagers déclaré.
- `trip_distance` : distance de la course.
- `PULocationID` et `DOLocationID` : identifiants des zones de prise en charge et de dépose.
- `fare_amount` : montant de la course (USD).
- `total_amount` : montant total facturé (USD).

Les valeurs absentes sont représentées par `null`. Les dates sont sérialisées en ISO 8601. Les événements sont triés par heure de prise en charge avant publication. Les colonnes de trajet conservées dans le flux brut servent à l'ingestion et à l'analyse ; elles ne signifient pas qu'elles seront toutes utilisées comme variables du futur modèle. En particulier, les variables exclues pour la tâche de prédiction restent exclues de l'entraînement : `total_amount` est conservé dans le flux brut uniquement pour l'analyse, jamais comme variable explicative.

## 8. Structure du projet

```text
NYC_Taxi_BigData/
├── .env.example
├── .gitignore
├── README.md
├── config/
│   ├── docker-compose.yml
│   └── spark/
│       └── Dockerfile
├── data/
│   └── .gitkeep
├── producer/
│   └── producer.py
├── scripts/
│   ├── batch_ingest.py
│   ├── stream_ingest.py
│   ├── spark_kafka_test.py
│   ├── download_dataset.py
│   └── hdfs_test.txt
├── docs/
│   ├── architecture.png
│   ├── architecture.md
│   └── captures/
└── requirements.txt
```

## 9. Installation

Depuis la racine du dépôt :

```bash
# Windows (invite de commandes)
copy .env.example .env

# Linux / macOS
cp .env.example .env
```

Télécharger le dataset avec `python scripts/download_dataset.py`.

Vérifier la configuration :

```bash
docker compose --env-file .env -f config/docker-compose.yml config
```

Construire l'image Spark avec Python et démarrer les services :

```bash
docker compose --env-file .env -f config/docker-compose.yml up -d --build
```

Vérifier les conteneurs :

```bash
docker compose --env-file .env -f config/docker-compose.yml ps
```

Installer les dépendances du Producer (Windows, Linux ou macOS) :

```bash
python -m pip install -r requirements.txt
```

## 10. Chargement batch du Parquet dans HDFS

Créer le répertoire HDFS :

```bash
docker exec hdfs-namenode hdfs dfs -mkdir -p /taxi/raw
```

Copier le fichier depuis Windows dans le conteneur NameNode :

```bash
docker cp data/yellow_tripdata_2025-01.parquet hdfs-namenode:/tmp/yellow_tripdata_2025-01.parquet
```

Charger le fichier dans HDFS :

```bash
docker exec hdfs-namenode hdfs dfs -put -f /tmp/yellow_tripdata_2025-01.parquet /taxi/raw/
```

Vérifier :

```bash
docker exec hdfs-namenode hdfs dfs -ls /taxi/raw
```

Soumettre le job batch au cluster Spark :

```bash
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.hadoop.dfs.replication=2 --conf spark.driver.host=spark-master --conf spark.driver.bindAddress=0.0.0.0 /opt/app/scripts/batch_ingest.py
```

## 11. Ingestion streaming Kafka → Spark → HDFS

Le topic `taxi-trips` à 3 partitions est créé automatiquement par `kafka-init`.

Dans un premier terminal, démarrer Spark Structured Streaming :

```bash
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 --conf spark.jars.ivy=/tmp/.ivy --conf spark.hadoop.dfs.replication=2 --conf spark.driver.host=spark-master --conf spark.driver.bindAddress=0.0.0.0 --conf spark.executor.cores=1 --conf spark.cores.max=2 /opt/app/scripts/stream_ingest.py
```

Laisser ce terminal actif. Dans un deuxième terminal Windows, lancer le Producer :

```bash
python producer/producer.py
```

Par défaut, le Producer publie 1 000 courses triées par `tpep_pickup_datetime`. Il ajoute `event_id`, `event_timestamp` et `source_row_number`. Les valeurs manquantes sont envoyées en JSON `null` et les dates sont sérialisées au format ISO 8601.

Paramètres disponibles :

```bash
# 1 000 événements (comportement par défaut)
python producer/producer.py

# Publier 5 000 événements
python producer/producer.py --limit 5000

# Rejouer toutes les lignes valides du fichier, dans l'ordre chronologique
python producer/producer.py --all

# Envoyer 100 événements avec 0,2 seconde entre chaque message
python producer/producer.py --limit 100 --delay-seconds 0.2

# Utiliser un autre fichier source
python producer/producer.py --data data/yellow_tripdata_2025-01.parquet
```

`--all` ignore `--limit`. Le délai est exprimé en secondes. Un délai nul publie les messages aussi vite que possible.

## 12. Commandes de vérification

### Services Docker

```bash
docker compose --env-file .env -f config/docker-compose.yml ps -a
```

### Topic Kafka

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic taxi-trips
```

### Test de communication Spark ↔ Kafka sur le cluster

Après avoir publié des messages avec le Producer, exécuter ce job depuis `spark-master`. Il utilise le cluster Spark (`spark://spark-master:7077`) et lit les messages du topic depuis Kafka :

```bash
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 --conf spark.jars.ivy=/tmp/.ivy /opt/app/scripts/spark_kafka_test.py
```

Le résultat doit afficher le nombre de messages lus et un aperçu des valeurs JSON. Ce test prouve la connectivité Spark-Kafka ; il ne remplace pas le test Structured Streaming vers HDFS.

### Consommation Kafka avec le client console

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic taxi-trips --from-beginning
```

### Fichiers HDFS

```bash
docker exec hdfs-namenode hdfs dfs -ls -R /taxi/raw
docker exec hdfs-namenode hdfs dfs -ls -R /taxi/stream/raw
docker exec hdfs-namenode hdfs dfs -ls -R /taxi/stream/checkpoint
```

### Réplication et santé HDFS

```bash
docker exec hdfs-namenode hdfs fsck /taxi/raw -files -blocks -locations
docker exec hdfs-namenode hdfs fsck /taxi/stream/raw -files -blocks -locations
```

Vérifier dans les sorties que les fichiers ont `replication=2`, `Live_repl=2`, `Status: HEALTHY`, et qu'il n'y a pas de blocs sous-répliqués. La sortie dépend des données effectivement écrites et de l'état du cluster.

### Application Spark

Ouvrir http://localhost:8080 pendant l'exécution du job streaming. Vérifier que l'application est active et que les deux Workers sont enregistrés. Vérifier également les interfaces `http://localhost:8081` et `http://localhost:8082`.

## 13. Vérifications réalisées / à réaliser

Les cases ci-dessous sont volontairement laissées non cochées pour la nouvelle configuration : elles doivent être cochées après exécution effective sur la machine du groupe.

- [ ] `docker compose config` sans erreur.
- [ ] Les 8 conteneurs/services attendus apparaissent et sont dans l'état attendu.
- [ ] Spark Master et les deux Workers sont visibles.
- [ ] HDFS NameNode et les deux DataNodes sont visibles.
- [ ] Topic `taxi-trips` créé automatiquement avec 3 partitions.
- [ ] Producer → Kafka → Consumer validé.
- [ ] Test de lecture Kafka avec `spark-submit` sur le cluster Spark validé.
- [ ] Ingestion batch Parquet vers HDFS validée par `hdfs dfs -ls` et `hdfs fsck`.
- [ ] Ingestion Kafka → Spark Structured Streaming → HDFS validée.
- [ ] Checkpoint streaming visible dans HDFS.
- [ ] Réplication HDFS égale à 2 et état `HEALTHY` pour `/taxi/raw`.
- [ ] Réplication HDFS égale à 2 et état `HEALTHY` pour `/taxi/stream/raw`.
- [ ] Application streaming active sur Spark avec les deux Workers enregistrés.
- [ ] Captures ajoutées dans `docs/captures/`.

## 14. Captures à déposer

Ajouter les captures réelles suivantes dans `docs/captures/` :

1. `01_docker_compose_ps.png` — état des services Docker.
2. `02_spark_workers_application.png` — deux Workers et application streaming active.
3. `03_spark_kafka_test.png` — résultat du job Spark qui lit Kafka sur le cluster.
4. `04_hdfs_datanodes.png` — deux DataNodes dans NameNode UI.
5. `05_hdfs_fsck.png` — réplication 2 et état HEALTHY.
6. `06_kafka_consumer.png` — événements reçus.
7. `07_hdfs_taxi_raw.png` — fichiers batch.
8. `08_hdfs_taxi_stream_raw.png` — fichiers Parquet streaming.

Ne pas créer de fausses captures : elles doivent provenir de l'exécution réelle.

## 15. Suite du projet (hors Livrable 1)

Pour les semaines 3 et 4, un service PostgreSQL pourra être ajouté pour la base de restitution et les résultats analytiques. Il n'est volontairement pas présent dans le `docker-compose.yml` actuel. La partie Machine Learning sera développée après validation du cas métier.

## 16. État d'avancement

- [ ] Validation du cas métier par l'enseignante.
- [ ] Infrastructure Docker validée (toutes les vérifications de la section « Vérifications réalisées »).
- [ ] Ingestion batch validée.
- [ ] Ingestion streaming et checkpoint HDFS validés.
- [ ] Captures finales ajoutées.
- [ ] Contributions Git des quatre membres effectuées.
- [ ] Invitation du professeur au dépôt GitHub envoyée si un accès privé est demandé.

## 17. Membres du groupe

- EL BAZ HIND
- JDANI FATIMA ZAHRA
- QASSABA OUMAYMA
- BOUHAMIDI MOSTAFA



## 18. Dépôt GitHub

https://github.com/Hind-create/NYC_Taxi_BigData
