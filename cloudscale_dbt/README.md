# CloudScale — Real-Time E-Commerce Data Engineering Platform

![Python](https://img.shields.io/badge/Python-3.12-blue)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-4.3.1-black)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-4.2.0-orange)
![dbt](https://img.shields.io/badge/dbt-1.12.3-orange)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-3.0.6-blue)
![Power BI](https://img.shields.io/badge/Power%20BI-Analytics-yellow)
![Docker](https://img.shields.io/badge/Docker-Containerized-blue)
![GitHub](https://img.shields.io/badge/GitHub-Version%20Control-black)

---

## Overview

**CloudScale** is an end-to-end real-time e-commerce data engineering and analytics platform built to demonstrate a modern production-style data pipeline.

The project processes e-commerce order data through multiple engineering layers including:

- Real-time event ingestion
- Relational data storage
- Data quality validation
- Distributed data processing
- Columnar data storage
- Analytics engineering
- Incremental processing
- Slowly Changing Dimensions
- Data testing
- Workflow orchestration
- Dimensional modeling
- Business intelligence
- Interactive analytics

The complete pipeline connects:

```text
E-Commerce Dataset
        ↓
Apache Kafka
        ↓
PostgreSQL
        ↓
Data Quality
        ↓
Apache PySpark
        ↓
Parquet
        ↓
dbt
        ↓
Silver / Gold Analytics
        ↓
Apache Airflow
        ↓
Power BI
