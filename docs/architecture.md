# Architecture du projet NYC Taxi Big Data

![Architecture globale du projet](architecture.png)

# Architecture du projet NYC Taxi Big Data

## 1. Architecture générale

Le projet repose sur une architecture distribuée conteneurisée avec Docker Compose.

```mermaid
flowchart TB
    A["Dataset NYC TLC<br/>Yellow Taxi - Parquet"]
    B["Python Producer"]
    C["Kafka Broker"]
    D["Topic taxi-trips<br/>3 partitions"]
    E["Kafka Consumer"]

    A --> B
    B --> C
    C --> D
    D --> E

    subgraph INFRA["Infrastructure distribuée Docker"]
        subgraph KAFKA["Apache Kafka"]
            C
            D
        end

        subgraph SPARK["Apache Spark"]
            SM["Spark Master"]
            SW1["Spark Worker 1"]
            SW2["Spark Worker 2"]
            SM --> SW1
            SM --> SW2
        end

        subgraph HDFS["Hadoop HDFS"]
            NN["NameNode"]
            DN1["DataNode 1"]
            DN2["DataNode 2"]
            NN --> DN1
            NN --> DN2
        end
    end

    D -. "Intégration Streaming prévue" .-> SM
    SM -. "Archivage HDFS prévu" .-> NN
```

## 2. Composants

- **NYC TLC Dataset** : données des trajets Yellow Taxi au format Parquet.
- **Python Producer** : lit les données et les transforme en événements JSON.
- **Kafka Broker** : reçoit les événements.
- **Topic taxi-trips** : topic Kafka organisé en trois partitions.
- **Kafka Consumer** : permet de vérifier la réception des événements.
- **Spark Master** : coordonne l'exécution distribuée.
- **Spark Workers** : exécutent les tâches de calcul.
- **HDFS NameNode** : gère les métadonnées du système de fichiers distribué.
- **HDFS DataNodes** : stockent les blocs de données avec un facteur de réplication de 2.

## 3. État du premier livrable

Les composants Docker, le Producer Python et le test Kafka sont opérationnels.

L'intégration de Spark Structured Streaming avec Kafka et l'archivage automatique dans HDFS seront réalisés dans les étapes suivantes du projet.
