# ShopFlow Data Warehouse & ETL Platform

An end-to-end e-commerce data engineering platform for ingesting,
transforming, validating, orchestrating, and analyzing transactional data.

## Project Overview

ShopFlow simulates a real-world e-commerce data platform where
transactional data is extracted from an operational PostgreSQL database,
processed through an ETL pipeline, and loaded into a dimensional data
warehouse for analytics.

The platform is designed around:

- Batch data ingestion
- Data validation and quality checks
- ETL/ELT processing
- Dimensional data modeling
- Incremental data loading
- Historical data tracking
- Workflow orchestration
- SQL analytics
- Business intelligence dashboards

## Target Architecture

PostgreSQL / CSV / JSON
        ↓
Raw Data Layer
        ↓
Data Validation
        ↓
PySpark ETL
        ↓
BigQuery
        ↓
dbt Transformations
        ↓
Dimensional Data Warehouse
        ↓
SQL Analytics
        ↓
Looker Studio

## Technologies

- Python
- SQL
- PySpark
- PostgreSQL
- Google BigQuery
- Google Cloud Storage
- Apache Airflow
- dbt
- Docker
- GitHub
- Looker Studio

## Current Implementation Status

The local pipeline baseline is implemented using PostgreSQL, Python, PySpark,
and dbt. Airflow orchestration and the cloud-based components shown in the
target architecture are planned next.
