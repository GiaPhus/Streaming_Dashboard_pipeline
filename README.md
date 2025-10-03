# Streaming_Dashboard_pipeline

Just a project to learn about Kafka and how realtime dashboard works.

## 📌 Project Overview
This project demonstrates a simple real-time data pipeline using **Airflow**,**Docker**,**MySQL**, **Debezium**, **Kafka**, **Spark Streaming**, **Cassandra**, and **Grafana**.

```mermaid
flowchart TD
    A[Airflow DAG (Docker)] -->|Call API every 30s (for 10 min)| B[MySQL (Docker)]
    B -->|CDC Debezium (Docker)| C[Kafka (Docker)]
    C --> D[Spark Streaming Consumer (Docker)]
    D --> E[Cassandra (Docker)]
    E --> F[Grafana Dashboard (Docker)]
