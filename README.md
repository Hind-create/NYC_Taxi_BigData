# NYC Taxi – Big Data Pipeline

## 1. Présentation du projet

Ce projet consiste à concevoir un pipeline Big Data de bout en bout basé sur les données NYC TLC Yellow Taxi Trip Records.

L'objectif est de mettre en place une infrastructure distribuée permettant l'ingestion, le stockage et le traitement de données de trajets de taxis.

## 2. Objectif du Livrable 1

Le premier livrable consiste à déployer une infrastructure distribuée avec Docker Compose et à vérifier la publication d'événements dans Kafka.

L'infrastructure comprend :

- Apache Spark : 1 Master et 2 Workers.
- Hadoop HDFS : 1 NameNode et 2 DataNodes.
- Apache Kafka : 1 Broker et le topic `taxi-trips`.
- Python : un Producer pour publier les événements issus du dataset NYC Taxi.

## 3. Architecture du projet

![Architecture globale du projet](docs/architecture.png)

Le schéma présente l'infrastructure globale et distingue les composants opérationnels du premier livrable des traitements prévus dans les étapes suivantes.

## 4. Technologies utilisées

- Docker et Docker Compose
- Apache Spark
- Hadoop HDFS
- Apache Kafka
- Python
- Pandas
- PyArrow
- kafka-python
- Git et GitHub

## 5. Dataset

Source officielle : [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

Fichier utilisé pour les tests :

`yellow_tripdata_2025-01.parquet`

Le dataset n'est pas inclus dans ce dépôt en raison de sa taille. Il doit être téléchargé séparément et placé dans le dossier `data/`.

## 6. Structure du projet

```text
NYC_Taxi_BigData/
├── config/
│   └── docker-compose.yml
├── data/
│   └── .gitkeep
├── producer/
│   └── producer.py
├── scripts/
│   └── hdfs_test.txt
├── docs/
│   ├── architecture.png
│   ├── architecture.md
│   ├── captures/
│   └── documentation/
├── requirements.txt
├── .gitignore
└── README.md
```

## 7. Installation et exécution

### Prérequis

- Docker Desktop
- Docker Compose
- Python
- Git

### Démarrer l'infrastructure

Depuis le répertoire du projet :

```bash
docker compose -f config/docker-compose.yml up -d
```

Vérifier les conteneurs :

```bash
docker compose -f config/docker-compose.yml ps
```

### Installer les dépendances Python

```bash
python -m pip install -r requirements.txt
```

### Lancer le Producer

Placer le fichier Parquet dans `data/`, puis exécuter :

```bash
python producer/producer.py
```

Le Producer publie les événements dans le topic Kafka `taxi-trips`.

### Consulter les événements Kafka

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic taxi-trips --from-beginning
```

## 8. Vérifications réalisées

- Démarrage des services Docker.
- Vérification du Spark Master et des deux Workers.
- Vérification du NameNode et des deux DataNodes.
- Création du topic Kafka `taxi-trips`.
- Publication de 1 000 événements avec le Producer Python.
- Réception des événements JSON avec le Consumer Kafka.
- Vérification de la réplication HDFS à 2 avec `hdfs fsck`.
- Vérification de l'état du système de fichiers : `HEALTHY`.

Les captures de ces vérifications sont disponibles dans `docs/captures/`.

## 9. État d'avancement

Le Livrable 1 couvre le déploiement de l'infrastructure distribuée et le test de l'ingestion des événements dans Kafka.

L'intégration de Spark Structured Streaming, l'archivage automatique dans HDFS et les traitements analytiques seront réalisés dans les étapes suivantes.

## 10. Membres du groupe

EL BAZ HIND,JDANI FATIMA ZAHRA,QASSABA OUMAYMA,BOUHAMIDI MOSTAFA