# event-to-lakehouse

**Event-to-Lakehouse** is a streaming data platform that transforms event data from Kafka into analytics-ready Iceberg tables using Spark and MinIO.

It provides an end-to-end pipeline for:
- ingesting events from multiple source systems,
- landing raw data in a bronze layer,
- automatically inferring schemas,
- normalizing heterogeneous sources into canonical silver models,
- and managing Iceberg table creation and evolution.

The platform is designed around **data contracts**, **schema governance**, and **human-in-the-loop normalization**, enabling users to auto-map disparate event schemas into a unified analytical model while retaining raw payloads for audit and evolution.

---

## Key Concepts

- **Streaming-first**  
  Kafka and Kafka Connect for reliable event ingestion

- **Lakehouse-native**  
  Iceberg tables on MinIO (S3-compatible object storage)

- **Medallion architecture**  
  Bronze → Silver (→ Gold) data layers

- **Schema discovery & mapping**  
  Automated schema inference with user-approved normalization

- **Governed evolution**  
  Safe, intentional schema changes using Iceberg’s evolution model

---

## Typical Flow

1. Events are produced to Kafka topics  
2. Kafka Connect lands raw data into the bronze layer  
3. Spark jobs infer source schemas and profile data  
4. Auto-mapping suggests a canonical (silver) schema  
5. Approved mappings normalize data into Iceberg tables  
6. Tables are immediately queryable by Spark, Trino, or BI tools  

---

## Tech Stack

- Apache Kafka (KRaft mode)
- Kafka Connect
- Apache Spark
- Apache Iceberg (REST + Hadoop catalog)
- MinIO (S3-compatible object storage)
- Docker Compose for local orchestration

---

## Purpose

This repository serves as both a **proof of concept** and a **foundation for a production-grade streaming lakehouse platform**, demonstrating how to combine streaming ingestion, schema governance, and lakehouse storage into a single cohesive system.
