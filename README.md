# Streaming_Dashboard_pipeline

Just a project to learn about Kafka and how realtime dashboard works.

## 📌 Project Overview
This project demonstrates a simple real-time data pipeline using **Airflow**,**Docker**,**MySQL**, **Debezium**, **Kafka**, **Spark Streaming**, **Cassandra**, and **Grafana**.

```mermaid
flowchart TD
    A[Airflow DAG] -->|Call API every 30s (for 10 min)| B[MySQL]
    B -->|CDC Debezium| C[Kafka]
    C --> D[Spark Streaming Consumer]
    D --> E[Cassandra]
    E --> F[Grafana Dashboard]
