# World Bank GDP Data Pipeline

An end-to-end data engineering pipeline that ingests global GDP data from the World Bank public API, processes it with Apache Spark, stores it in a PostgreSQL data warehouse, and delivers insights through a Power BI dashboard.

---

## Overview

This pipeline automates the daily collection and transformation of GDP data for all countries from 1960 to the present using the World Bank indicator `NY.GDP.MKTP.CD` (GDP at market prices, current USD). Raw data lands in Amazon S3, is cleaned and validated by PySpark, loaded into a PostgreSQL star schema, and visualized in Power BI — all orchestrated by Apache Airflow running on Docker.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Apache Airflow (Daily DAG)                        │
│                                                                             │
│   ┌─────────────┐    ┌─────────────┐    ┌──────────────┐    ┌───────────┐  │
│   │ extract_data│───▶│ upload_to_s3│───▶│spark_preproc │───▶│cleanup_   │  │
│   │             │    │             │    │              │    │logs       │  │
│   └─────────────┘    └─────────────┘    └──────────────┘    └───────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
        │                    │                    │
        ▼                    ▼                    ▼
 ┌─────────────┐    ┌─────────────────┐   ┌──────────────────────────────────┐
 │ World Bank  │    │   Amazon S3      │   │         PostgreSQL               │
 │   REST API  │    │                 │   │  ┌──────────┐  ┌──────────────┐  │
 │             │    │ raw/world_bank/ │   │  │dim_country│  │  dim_time    │  │
 │ NY.GDP.MKTP │    │  year=YYYY/     │   │  └──────────┘  └──────────────┘  │
 │    .CD      │    │  month=MM/      │   │       │              │            │
 └─────────────┘    │  day=DD/        │   │       └──────┬───────┘            │
                    │                 │   │         ┌────▼────┐               │
                    │ processed/      │   │         │fact_gdp │               │
                    │ analytics/      │   │         └─────────┘               │
                    └─────────────────┘   └──────────────────────────────────┘
                                                         │
                                                         ▼
                                                  ┌─────────────┐
                                                  │  Power BI   │
                                                  │  Dashboard  │
                                                  └─────────────┘
```

---

## Features

- **Automated Ingestion** — paginated extraction from the World Bank REST API with exponential backoff retry (max 3 attempts) and resilient handling of transient errors
- **Data Lake on S3** — raw JSON stored with Hive-style date partitioning (`year=YYYY/month=MM/day=DD`) and AES-256 server-side encryption
- **PySpark ETL** — distributed cleaning, type casting, and analytics aggregation (average GDP per country across all years)
- **Data Quality Validation** — five automated checks with CRITICAL / WARNING severity levels run on every pipeline execution
- **Star Schema Warehouse** — idempotent SQL pipeline populates dimension and fact tables with `ON CONFLICT` upsert semantics
- **Daily Orchestration** — Airflow DAG with retry logic, execution timeouts, and structured failure callbacks
- **Power BI Dashboard** — connected directly to PostgreSQL, visualizing GDP trends, country rankings, and time-series analysis
- **Containerized** — full local environment via Docker Compose (Airflow + PostgreSQL + Redis)
- **Test Coverage** — 40+ pytest test cases covering extraction, transformation, S3 upload, and data quality checks

---

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.10.1 (CeleryExecutor) |
| Processing | Apache Spark 3.5.0 (PySpark) |
| Data Lake | Amazon S3 |
| Data Warehouse | PostgreSQL 13 |
| Visualization | Microsoft Power BI |
| Containerization | Docker + Docker Compose |
| Language | Python 3.x |
| Testing | pytest 9.0.3 |
| Task Queue | Redis 7.2 |

---

## Pipeline Stages

### 1. Extract (`scripts/extract.py`)

Fetches all records for indicator `NY.GDP.MKTP.CD` from the World Bank API with full pagination support (1,000 records per page). Implements exponential backoff for 5xx and connection errors. Raw records are saved locally to `logs/raw_YYYY-MM-DD.json`.

### 2. Upload (`scripts/s3_upload.py`)

Uploads the raw JSON file to S3 with Hive-style date partitioning:

```
s3://<bucket>/raw/world_bank/year=YYYY/month=MM/day=DD/data.json
```

Supports all AWS authentication methods (environment variables, `~/.aws/credentials`, IAM instance roles).

### 3. Spark Processing (`scripts/spark_job.py`)

The PySpark job performs the full ETL in a single run:

1. **Read** — loads raw JSON from S3 across all date partitions using a wildcard path
2. **Clean** — selects nested fields, renames columns, casts types (`year` → `INT`, `gdp_usd` → `DOUBLE`), filters null GDP values
3. **Validate** — runs five data quality checks (see [Data Quality](#data-quality))
4. **Write processed** — saves cleaned data as Parquet partitioned by `year` to `s3://<bucket>/processed/world_bank/`
5. **Aggregate** — computes `AVG(gdp_usd)` and year count per country
6. **Write analytics** — saves aggregated Parquet to `s3://<bucket>/analytics/world_bank/`
7. **Bulk load** — writes cleaned data to the `stg_gdp` staging table via JDBC
8. **SQL pipeline** — populates dimension tables and `fact_gdp`, then truncates staging

### 4. Cleanup

Removes `raw_*.json` files from `logs/` older than 7 days to prevent unbounded disk growth.

---

## Data Quality

Five checks run automatically on every Spark job execution:

| # | Severity | Check | Threshold |
|---|---|---|---|
| 1 | CRITICAL | Null rate in `gdp_usd` | ≤ 5% |
| 2 | WARNING | Suspicious GDP values | $1M – $30T range |
| 3 | WARNING | Duplicate `(country_iso3, year)` pairs | None expected |
| 4 | WARNING | Row count vs previous run | Drop ≤ 20% |
| 5 | CRITICAL | Year range validity | 1960 – current year |

CRITICAL failures halt the pipeline. WARNINGS are logged but execution continues. Results are persisted to `logs/run_metadata.json` for trend monitoring.

---

## Database Schema

The PostgreSQL data warehouse uses a star schema optimised for analytical queries.

```
                    ┌──────────────────┐
                    │   dim_country    │
                    │──────────────────│
                    │ country_key (PK) │
                    │ country_name     │
                    │ region           │
                    │ income_level     │
                    └────────┬─────────┘
                             │
┌──────────────┐    ┌────────▼─────────┐    ┌─────────────────────┐
│   dim_time   │    │    fact_gdp      │    │   dim_indicator     │
│──────────────│    │──────────────────│    │─────────────────────│
│ time_key(PK) │◀───│ measurement_id   │───▶│ indicator_key (PK)  │
│ year         │    │ country_key (FK) │    │ indicator_name      │
│ decade       │    │ time_key (FK)    │    └─────────────────────┘
│ era          │    │ indicator_key(FK)│
└──────────────┘    │ gdp_usd          │
                    │ loaded_at        │
                    └──────────────────┘
```

**Staging table** (`stg_gdp`) acts as a transient landing zone — populated by Spark JDBC bulk load, then read by `load_pipeline.sql` to populate dimensions and facts before being truncated.

---

## Power BI Dashboard

The Power BI dashboard connects directly to PostgreSQL and visualises GDP data across the full schema. Key views include:

- **Global GDP Rankings** — country-level GDP rankings with time-series drill-down
- **Regional Trends** — GDP aggregated by World Bank region and income level groupings
- **Decade Analysis** — GDP evolution grouped by decade using `dim_time.decade` and `dim_time.era`
- **Country Comparison** — side-by-side GDP trend lines across multiple selected countries

The dashboard PDF export is available at [`World Bank Dashboard.pdf`](World%20Bank%20Dashboard.pdf).

---

## Prerequisites

| Requirement | Version |
|---|---|
| Docker Desktop | Latest |
| Docker Compose | v2+ |
| AWS Account | S3 access required |
| Python | 3.10+ (for local runs) |

Minimum Docker resources recommended: **4 GB RAM**, **2 CPUs**, **10 GB disk**.

---

## Quick Start (Docker)

### 1. Clone and configure environment

```bash
git clone <repo-url>
cd "World Bank"
cp .env.example .env
```

Edit `.env` with your credentials:

```env
# AWS
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_DEFAULT_REGION=eu-west-1
S3_BUCKET_NAME=your-bucket-name

# PostgreSQL
POSTGRES_DATABASE=worldbank
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_SSLMODE=prefer

# Airflow
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
```

### 2. Build and start services

```bash
docker compose up --build -d
```

This starts: PostgreSQL, Redis, Airflow webserver, scheduler, worker, and triggerer.

### 3. Initialise the database schema

```bash
docker compose exec postgres psql -U airflow -d airflow -f /opt/airflow/sql/init_postgres.sql
```

### 4. Access the Airflow UI

Open [http://localhost:8080](http://localhost:8080) and log in with the credentials set in `.env` (default: `airflow` / `airflow`).

Enable and trigger the `worldbank_pipeline` DAG.

### 5. Monitor with Celery Flower (optional)

```bash
docker compose --profile flower up -d
```

Open [http://localhost:5555](http://localhost:5555) to monitor Celery workers.

---

## Running Locally (without Docker)

### Install dependencies

```bash
python -m venv .venv
.venv\Scripts\activate        # Windows
# source .venv/bin/activate   # Linux/macOS
pip install -r requirements.txt
```

### Run the full pipeline

```bash
python main.py
```

This executes extract → S3 upload → Spark ETL → PostgreSQL load in sequence outside of Airflow.

---

## Configuration

All runtime settings are managed in [config/config.yaml](config/config.yaml). Values support `${ENV_VAR}` substitution with optional defaults using `${ENV_VAR:-default}`.

| Key | Description | Default |
|---|---|---|
| `s3.bucket` | S3 bucket name | `$S3_BUCKET_NAME` |
| `s3.region` | AWS region | `eu-west-1` |
| `api.per_page` | Records per API page | `1000` |
| `postgres.host` | PostgreSQL hostname | `postgres` |
| `postgres.sslmode` | SSL mode (`prefer`/`require`) | `prefer` |
| `spark.master` | Spark master URL | `local[*]` |
| `spark.jdbc_driver_jar` | Path to PostgreSQL JDBC JAR | Spark install path |

---

## Project Structure

```
World Bank/
├── config/
│   └── config.yaml              # Pipeline configuration with env var expansion
├── dags/
│   └── pipeline_dag.py          # Airflow DAG (daily schedule, CeleryExecutor)
├── scripts/
│   ├── extract.py               # World Bank API extraction with pagination + retry
│   ├── s3_upload.py             # S3 upload with date partitioning + AES-256
│   └── spark_job.py             # PySpark ETL: clean, validate, write, load
├── sql/
│   ├── init_postgres.sql        # Schema creation (staging + star schema)
│   └── load_pipeline.sql        # Idempotent dim/fact population + staging truncate
├── tests/
│   └── scripts_tests.py         # 40+ pytest tests (unit + integration)
├── utils/
│   ├── config_loader.py         # YAML loader with env var expansion
│   └── logging_config.py        # Configurable logging setup
├── main.py                      # Standalone pipeline runner (without Airflow)
├── conftest.py                  # Shared pytest fixtures (Spark session, logger)
├── pyproject.toml               # pytest configuration
├── requirements.txt             # Python dependencies
├── Dockerfile                   # Airflow + Java 17 + Spark 3.5.0 image
├── docker-compose.yaml          # Local dev stack
└── World Bank Dashboard.pdf     # Power BI dashboard export
```

---

## Testing

Run the full test suite:

```bash
pytest tests/scripts_tests.py -v
```

Test coverage by module:

| Module | Tests |
|---|---|
| `extract.py` | Pagination, retry logic, error handling, config validation |
| `spark_job.py` (clean) | Column renaming, type casting, null filtering |
| `spark_job.py` (validate) | All 5 quality checks, error conditions, report structure |
| `s3_upload.py` | Path parsing, encryption, file validation, error handling |
| `config_loader.py` | YAML parsing, env var expansion with defaults |

---

## Environment Variables Reference

| Variable | Required | Description |
|---|---|---|
| `AWS_ACCESS_KEY_ID` | Yes | AWS access key |
| `AWS_SECRET_ACCESS_KEY` | Yes | AWS secret key |
| `AWS_DEFAULT_REGION` | No | AWS region (default: `eu-west-1`) |
| `S3_BUCKET_NAME` | Yes | Target S3 bucket |
| `POSTGRES_DATABASE` | Yes | PostgreSQL database name |
| `POSTGRES_USER` | Yes | PostgreSQL username |
| `POSTGRES_PASSWORD` | Yes | PostgreSQL password |
| `POSTGRES_SSLMODE` | No | SSL mode (default: `prefer`) |
| `AIRFLOW_UID` | No | Airflow user ID (default: `50000`) |
| `SPARK_JOB_PATH` | No | Override Spark job path in DAG |
