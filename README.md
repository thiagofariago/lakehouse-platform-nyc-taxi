# NYC Taxi Lakehouse Platform

A production-grade data lakehouse built on open-source tooling. Ingests NYC Taxi trip data through a fully containerized pipeline — from raw parquet files to analytics-ready fact tables — using Spark, Iceberg, Trino, dbt, and Airflow.

---

## Architecture

```
                        NYC TLC Public API
                    (parquet files via HTTPS)
                              |
                              v
                    +-------------------+
                    |   Apache Spark    |
                    |   (Ingestion)     |
                    +--------+----------+
                             |
                             v
  +----------+      +----------------+      +-----------+
  |  Hive    |<---->|     MinIO      |<---->|   Trino   |
  | Metastore|      | (S3 Storage)   |      | (Query    |
  | (catalog)|      |                |      |  Engine)  |
  +----------+      +----------------+      +-----------+
       ^                                         ^
       |              +---------+                |
       +--------------+   dbt   +----------------+
                       | (Transform)
                       +---------+
                              ^
                              |
                    +-------------------+
                    |  Apache Airflow   |
                    |  (Orchestration)  |
                    +-------------------+
```

### Data Flow

```
RAW (Iceberg)          STAGING (dbt)           INTERMEDIATE (dbt)       MARTS (dbt)
──────────────         ─────────────           ──────────────────       ───────────
yellow_trips    -->    stg_yellow_trips   -->   int_trips_unified  -->  fct_trips
green_trips     -->    stg_green_trips    -->   int_trips_enriched -->  fct_trips_daily
fhv_trips       -->    stg_fhv_trips      -->   int_trips_cleaned  -->  fct_trips_monthly
fhvhv_trips     -->    stg_fhvhv_trips    |
                                          |
Spark writes           Standardize,       |    Union all types,        High-quality
parquet to             cast types,             calculate speed,        trips only,
Iceberg tables         generate IDs,           duration, cost,         daily/monthly
(partitioned           filter nulls,           quality flags,          aggregations
by year/month)         incremental load        temporal features
```

---

## Stack

| Component | Technology | Role |
|-----------|-----------|------|
| Storage | MinIO | S3-compatible object storage |
| Table Format | Apache Iceberg | ACID transactions, time travel, schema evolution |
| Metastore | Hive Metastore + PostgreSQL | Centralized metadata catalog |
| Ingestion | Apache Spark 3.5 | Distributed data processing |
| Query Engine | Trino 438 | Distributed SQL over Iceberg tables |
| Transformation | dbt 1.7 + dbt-trino | SQL-based modeling and testing |
| Orchestration | Apache Airflow 2.8 | Pipeline scheduling and monitoring |

---

## Getting Started

### Prerequisites

- Docker + Docker Compose (v2)
- 8 GB RAM minimum
- Available ports: `8080-8085`, `9000-9001`, `5432-5433`, `9083`, `7077`

### Setup

```bash
git clone <repo-url> && cd lakehouse-platform-nyc-taxi

# Create environment config
cp .env.example .env

# Start all services
docker compose up -d

# Verify everything is healthy
make health
```

Startup takes 2-3 minutes. Wait for all health checks to pass before running the pipeline.

### Run the Pipeline

**Option A — Airflow UI:**

Open [http://localhost:8081](http://localhost:8081) (admin / admin), find `nyc_taxi_pipeline`, and trigger it.

**Option B — CLI:**

```bash
make airflow-trigger
```

### Access the UIs

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | [localhost:8081](http://localhost:8081) | admin / admin |
| Trino | [localhost:8080](http://localhost:8080) | — |
| MinIO Console | [localhost:9001](http://localhost:9001) | minioadmin / minioadmin123 |
| Spark Master | [localhost:8083](http://localhost:8083) | — |

---

## Pipeline Tasks

The DAG `nyc_taxi_pipeline` runs six sequential tasks:

| # | Task | What it does |
|---|------|-------------|
| 1 | `ingest_spark` | Downloads parquet files from NYC TLC API, writes to Iceberg tables in `raw` schema via Spark |
| 2 | `dbt_staging` | Installs dbt packages, then standardizes raw data — casts types, generates surrogate keys, filters invalid records |
| 3 | `dbt_intermediate` | Unifies all taxi types, calculates trip duration/speed/cost, adds temporal features and quality flags |
| 4 | `dbt_marts` | Builds analytics-ready fact tables: `fct_trips`, `fct_trips_daily`, `fct_trips_monthly` |
| 5 | `dbt_test` | Runs 37 data quality tests (not_null, accepted_values, range checks) across all layers |
| 6 | `log_completion` | Logs pipeline summary |

---

## Project Structure

```
.
├── airflow/
│   ├── Dockerfile              # Airflow image with dbt, Spark, Docker CLI
│   ├── dags/
│   │   └── nyc_taxi_pipeline.py  # Main DAG definition
│   └── requirements.txt        # Python dependencies
│
├── dbt/
│   ├── dbt_project.yml         # dbt project config
│   ├── profiles.yml            # Trino connection profile
│   ├── packages.yml            # dbt_utils dependency
│   ├── macros/
│   │   └── get_custom_schema.sql  # Schema routing macro
│   ├── models/
│   │   ├── staging/            # 4 models — one per taxi type (incremental)
│   │   ├── intermediate/       # 3 models — unify, enrich, clean (views)
│   │   └── marts/              # 3 models — fact tables (tables)
│   └── tests/
│       └── marts/              # Custom singular tests
│
├── scripts/
│   └── nyc_taxi/
│       ├── ingest_spark_bulk.py     # PySpark ingestion script
│       └── run_spark_ingest_bulk.sh # Spark-submit wrapper
│
├── spark/
│   ├── Dockerfile              # Spark image with Iceberg + AWS JARs
│   └── conf/
│       └── spark-defaults.conf # Spark session defaults
│
├── metastore/
│   ├── Dockerfile              # Hive Metastore image
│   ├── metastore-site.xml      # Metastore config (PostgreSQL + S3)
│   └── core-site.xml           # Hadoop S3A filesystem config
│
├── trino/
│   ├── catalog/
│   │   ├── iceberg.properties  # Iceberg connector (primary)
│   │   └── hive.properties     # Hive connector
│   ├── coordinator/            # Coordinator node config
│   └── worker/                 # Worker node config
│
├── docker-compose.yml          # All services definition
├── Makefile                    # Shortcut commands
├── .env.example                # Environment variables template
└── .github/workflows/ci.yml   # CI: lint + Docker build
```

---

## Component Details

### MinIO (Object Storage)

S3-compatible storage that replaces AWS S3 for local development. All Iceberg data files and metadata live here under the `lakehouse` bucket.

### Hive Metastore

Central metadata catalog backed by PostgreSQL. Tracks all databases, tables, partitions, and schemas. Both Spark (writes) and Trino (reads/writes) connect to it via Thrift protocol on port 9083.

### Apache Spark

Handles data ingestion. The bulk ingestion script:
1. Downloads parquet files from the NYC TLC public CDN
2. Uploads to MinIO as temporary files
3. Reads with Spark for distributed processing
4. Adds metadata columns (`year`, `month`, `loaded_at`)
5. Writes to Iceberg tables partitioned by `year/month`

Configurable via environment variables in `run_spark_ingest_bulk.sh`:

```bash
NYC_TAXI_YEAR=2023
NYC_TAXI_MONTH=02
NYC_TAXI_COLORS=yellow,green,fhv,fhvhv
NYC_TAXI_OVERWRITE=false
```

### Trino (Query Engine)

Distributed SQL engine that queries Iceberg tables. Runs as a coordinator + worker(s) topology. dbt uses Trino as its execution backend for all transformations.

Scale workers as needed:

```bash
make scale-trino-workers WORKERS=3
```

### dbt (Transformations)

Three-layer modeling approach:

**Staging** — Incremental models that standardize raw data. Each taxi type gets its own model with consistent column naming, type casting, and surrogate key generation.

**Intermediate** — Views that unify all taxi types into a single schema, calculate derived metrics (speed, duration, cost), and flag data quality issues.

**Marts** — Materialized Iceberg tables optimized for analytics. `fct_trips` (trip-level), `fct_trips_daily` (daily aggregates), `fct_trips_monthly` (monthly aggregates with percentage distributions).

### Airflow (Orchestration)

Manages the end-to-end pipeline execution. The DAG runs on-demand (no schedule), with retry logic (2 retries, 5-minute delay). The Airflow container includes Docker CLI to submit Spark jobs to the Spark cluster.

---

## Querying Data

### Trino CLI

```bash
docker exec -it trino-coordinator trino --catalog iceberg --schema marts

trino> SELECT taxi_type, COUNT(*) as trips FROM fct_trips GROUP BY taxi_type;
trino> SELECT * FROM fct_trips_daily ORDER BY trip_date DESC LIMIT 10;
```

### SQL Client (DBeaver, DataGrip)

| Setting | Value |
|---------|-------|
| Host | `localhost` |
| Port | `8080` |
| Catalog | `iceberg` |
| Schema | `marts` |
| User | `admin` |
| Driver | Trino JDBC (`drivers/trino-jdbc-438.jar` included) |

---

## Useful Commands

```bash
# Service management
make up                    # Start all services
make down                  # Stop all services
make restart               # Restart all services
make status                # Show container status
make health                # Check service health

# Pipeline
make airflow-trigger       # Trigger the DAG
make airflow-list-dags     # List available DAGs

# dbt (runs inside Airflow container)
make dbt-run               # Run all models
make dbt-test              # Run all tests
make dbt-docs              # Generate documentation

# Logs
make logs                  # All services
make logs-airflow          # Airflow only
make logs-trino            # Trino only
make logs-spark            # Spark only

# Trino
make trino-cli             # Open Trino shell

# Cleanup
make clean                 # Stop + remove volumes
make clean-all             # Full reset (volumes + images + logs)
```

---

## Troubleshooting

| Problem | Solution |
|---------|---------|
| Old data from previous runs | `docker compose down -v` to remove persisted volumes |
| dbt exits with code 2, no output | Permission issue on `dbt/logs` — set `log-path` to `/tmp/dbt-logs` in `dbt_project.yml` |
| `NoSuchObjectException: database raw` | The `raw` database must be created before ingestion — handled automatically by the ingestion script |
| `Schema 'marts' does not exist` | Run `dbt run --select marts` before running tests |
| Airflow Jinja `TemplateNotFound` | `bash_command` in BashOperator must end with a trailing space |

---

## Requirements

- Docker Engine 20.10+
- Docker Compose v2
- 8 GB RAM minimum (16 GB recommended)
- ~5 GB disk space for images + data

---

## License

MIT
