# Image Spark locale avec Python 3.11 et Spark 3.5.3.
# Le cluster Spark et PySpark utilisent exactement la même distribution.
FROM python:3.11-slim-bookworm

ARG SPARK_VERSION=3.5.3
ARG SPARK_HADOOP_PROFILE=3

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       openjdk-17-jre-headless \
       ca-certificates \
       curl \
       procps \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fL \
      "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${SPARK_HADOOP_PROFILE}.tgz" \
      -o /tmp/spark.tgz \
    && tar -xzf /tmp/spark.tgz -C /opt \
    && mv "/opt/spark-${SPARK_VERSION}-bin-hadoop${SPARK_HADOOP_PROFILE}" "${SPARK_HOME}" \
    && rm /tmp/spark.tgz

WORKDIR /opt/app
