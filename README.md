# CloudScale — E-Commerce Data Engineering Platform

An end-to-end E-Commerce Data Engineering platform for data ingestion, processing, analytics, orchestration, business intelligence, and infrastructure monitoring.

---

## Project Architecture

```text
                         E-COMMERCE DATA PLATFORM

┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCE                              │
│                                                                 │
│                 Brazilian Olist Dataset                        │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                     DATA INGESTION                              │
│                                                                 │
│                     Apache Kafka                                │
│                  Producer → orders topic                        │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                        STORAGE                                  │
│                                                                 │
│                      PostgreSQL                                │
│                    cloudscale database                          │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                     DATA QUALITY                                │
│                                                                 │
│             Validation → Cleaning → Rejected Records            │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                     DATA PROCESSING                             │
│                                                                 │
│                 Apache Spark / PySpark                          │
│                         ↓                                       │
│                       Parquet                                   │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                  ANALYTICS ENGINEERING                          │
│                                                                 │
│                           dbt                                   │
│              ┌──────────────┼──────────────┐                    │
│              ▼              ▼              ▼                    │
│           Staging         Gold        Incremental              │
│                                             │                   │
│                                             ▼                   │
│                                         SCD Type 2              │
│                                                                 │
│                        dbt Tests                                │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                      ORCHESTRATION                              │
│                                                                 │
│                     Apache Airflow                              │
│                                                                 │
│       Data Quality → Spark → dbt Run → dbt Test                 │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                   BUSINESS INTELLIGENCE                         │
│                                                                 │
│                      Microsoft Power BI                         │
│                                                                 │
│             Star Schema → DAX → KPIs → Dashboards               │
└─────────────────────────────────────────────────────────────────┘


                    INFRASTRUCTURE MONITORING

              Docker Containers / Services
                         │
                         ▼
                     cAdvisor
                         │
                         ▼
                    Prometheus
                         │
                         ▼
                     Grafana
```

---

# Data Flow

```text
E-Commerce Dataset
        │
        ▼
   Apache Kafka
        │
        ▼
    PostgreSQL
        │
        ▼
  Data Quality
        │
        ▼
    PySpark
        │
        ▼
     Parquet
        │
        ▼
       dbt
        │
   ┌────┴────┐
   ▼         ▼
 Silver     Gold
   │         │
   └────┬────┘
        │
        ▼
   dbt Testing
        │
        ▼
    Airflow
        │
        ▼
    Power BI
```

---

# Technology Stack

## Data Engineering

- Python 3.12
- SQL
- Apache Kafka 4.3.1
- PostgreSQL 16
- Apache Spark 4.2.0
- PySpark
- Apache Parquet

## Analytics Engineering

- dbt Core 1.12.3
- dbt PostgreSQL 1.11.0
- dbt Staging Models
- dbt Gold Models
- Incremental Models
- dbt Snapshots
- SCD Type 2
- dbt Tests

## Orchestration

- Apache Airflow 3.0.6
- LocalExecutor
- DAGs
- BashOperator
- EmptyOperator

## Business Intelligence

- Microsoft Power BI
- Star Schema
- Dimensional Modeling
- DAX
- Time Intelligence
- KPI Analysis
- Drill-through
- Interactive Slicers

## Infrastructure & DevOps

- Docker
- Docker Compose
- Docker Networking
- cAdvisor
- Prometheus
- Grafana
- Docker Networking
- Git
- GitHub
- uv


---

# Installation & Dependencies

## Prerequisites

Install the following:

- Python 3.12
- Docker Desktop
- Git
- uv
- Power BI Desktop

> Java is not required on the host machine because Spark runs inside Docker.

---

## Clone Repository

```bash
git clone https://github.com/vishalmuralikumar/CloudScale-Data-Engineering.git

cd CloudScale-Data-Engineering
```

---

## Python Environment

Create and synchronize the project environment using `uv`:

```bash
uv sync
```

---

## Main Python Dependencies

```text
pyspark==4.2.0
dbt-core==1.12.3
dbt-postgres==1.11.0
kafka-python
python-dotenv
```

---

## Docker Services

The project uses Docker Compose for infrastructure.

```bash
docker network create cloudscale-network
```

Start the core services:

```bash
docker compose up -d
```

Services:

```text
Kafka
Kafka UI
PostgreSQL
Spark
```

Start Airflow:

```bash
cd airflow

docker compose up -d --build
```

---

# Service URLs

| Service | URL |
|---|---|
| Kafka UI | http://localhost:8080 |
| dbt Docs | http://localhost:8081 |
| Airflow | http://localhost:8082 |
| PostgreSQL | localhost:5433 |

---

# Project Components

```text
CloudScale
│
├── Streaming
│   └── Apache Kafka
│
├── Storage
│   └── PostgreSQL
│
├── Data Quality
│   └── Python
│
├── Processing
│   └── Apache PySpark
│
├── Data Storage
│   └── Parquet
│
├── Analytics Engineering
│   └── dbt
│       ├── Staging
│       ├── Silver
│       ├── Gold
│       ├── Incremental
│       └── SCD Type 2
│
├── Orchestration
│   └── Apache Airflow
│
├── Visualization
│   └── Power BI
│
├── Monitoring
│   ├── Prometheus
│   ├── Grafana
│   └── cAdvisor
│
└── Infrastructure
    ├── Docker
    └── Docker Compose
```

---

# End-to-End Architecture

```text
                       CLOUDSCALE DATA PLATFORM

 ┌────────────────────────────────────────────────────────────┐
 │                     DATA INGESTION                          │
 │                                                            │
 │              E-Commerce Dataset                            │
 │                       │                                    │
 │                       ▼                                    │
 │                 Apache Kafka                              │
 │                       │                                    │
 └───────────────────────┼────────────────────────────────────┘
                         │
                         ▼
 ┌────────────────────────────────────────────────────────────┐
 │                       STORAGE                              │
 │                                                            │
 │                   PostgreSQL                               │
 │                                                            │
 └───────────────────────┬────────────────────────────────────┘
                         │
                         ▼
 ┌────────────────────────────────────────────────────────────┐
 │                   DATA QUALITY                             │
 │                                                            │
 │        Validation → Cleaning → Rejected Records           │
 │                                                            │
 └───────────────────────┬────────────────────────────────────┘
                         │
                         ▼
 ┌────────────────────────────────────────────────────────────┐
 │                  DATA PROCESSING                            │
 │                                                            │
 │                    PySpark                                 │
 │                       │                                    │
 │                       ▼                                    │
 │                    Parquet                                 │
 │                                                            │
 └───────────────────────┬────────────────────────────────────┘
                         │
                         ▼
 ┌────────────────────────────────────────────────────────────┐
 │                ANALYTICS ENGINEERING                       │
 │                                                            │
 │                        dbt                                 │
 │                         │                                  │
 │          ┌──────────────┼──────────────┐                   │
 │          ▼              ▼              ▼                   │
 │      Staging          Gold        Incremental              │
 │                                         │                  │
 │                                         ▼                  │
 │                                    SCD Type 2              │
 │                                                            │
 │                         │                                  │
 │                         ▼                                  │
 │                    dbt Tests                               │
 │                                                            │
 └───────────────────────┬────────────────────────────────────┘
                         │
                         ▼
 ┌────────────────────────────────────────────────────────────┐
 │                    ORCHESTRATION                           │
 │                                                            │
 │                  Apache Airflow                            │
 │                                                            │
 │       Data Quality → Spark → dbt Run → dbt Test            │
 │                                                            │
 └───────────────────────┬────────────────────────────────────┘
                         │
                         ▼
 ┌────────────────────────────────────────────────────────────┐
 │                   BUSINESS INTELLIGENCE                     │
 │                                                            │
 │                     Power BI                               │
 │                                                            │
 │       Star Schema → DAX → KPIs → Dashboards                │
 │                                                            │
 └────────────────────────────────────────────────────────────┘


                 INFRASTRUCTURE MONITORING

 ┌────────────────────────────────────────────────────────────┐
 │                    MONITORING                              │
 │                                                            │
 │                  Docker Containers                         │
 │                         │                                  │
 │                         ▼                                  │
 │                      cAdvisor                              │
 │                         │                                  │
 │                         ▼                                  │
 │                    Prometheus                              │
 │                         │                                  │
 │                         ▼                                  │
 │                      Grafana                               │
 │                                                            │
 │          CPU → Memory → Network → Container Health         │
 │                                                            │
 └────────────────────────────────────────────────────────────┘
```

---

# Author

## Vishal Muralikumar

**Data Engineering | Analytics | Business Intelligence**

GitHub:

https://github.com/vishalmuralikumar/CloudScale-Data-Engineering

---

# License

This project is licensed under the Apache License 2.0.
