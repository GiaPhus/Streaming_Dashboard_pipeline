# Streaming_Dashboard_pipeline

Just a project to learn about Kafka and how realtime dashboard works.

## Data Source

This project uses the [Random User API](https://randomuser.me/api/) as a mock data source.  
The API generates random user profile data (name, gender, email, location, etc.), which is collected by Airflow and inserted into MySQL before being streamed through the pipeline.

## 📌 Project Overview
This project demonstrates a simple real-time data pipeline using **Airflow**,**Docker**,**MySQL**, **Debezium**, **Kafka**, **Spark Streaming**, **Cassandra**, and **Grafana**.

```mermaid
flowchart TD
    A[Airflow DAG] -->|Call API every 30s for 10 min| B[MySQL]
    B -->|CDC Debezium| C[Kafka]
    C --> D[Spark Streaming Consumer]
    D --> E[Cassandra]
    E --> F[Grafana Dashboard]
