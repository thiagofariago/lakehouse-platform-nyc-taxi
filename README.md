# Distributed Lakehouse

Modern data lakehouse template with **Trino** + **Iceberg** + **MinIO** + **dbt** + **Airflow**.

Clone, customize, and run your own data pipeline in minutes.

---

## Stack

- **Query Engine:** Trino (distributed SQL)
- **Table Format:** Apache Iceberg (ACID, time travel)
- **Storage:** MinIO (S3-compatible)
- **Transformation:** dbt (analytics engineering)
- **Orchestration:** Apache Airflow
- **Metastore:** Hive Metastore (PostgreSQL)

---

## Quick Start

```bash
# Clone
git clone <repo> my-lakehouse
cd my-lakehouse

# Configure
cp .env.example .env

# Start
docker compose up -d

# Access
# Airflow:  http://localhost:8081
# Trino UI:  http://localhost:8080
# MinIO:     http://localhost:9001 (minioadmin/minioadmin)
```

---

## Customize Your Pipeline

### 1. Create ingestion script

Edit `scripts/ingest_your_data.py`:

```python
def extract_data():
    # TODO: Connect to your data source
    # - API: requests.get(...)
    # - Database: pd.read_sql(...)
    # - Files: pd.read_csv(...)
    return df
```

### 2. Define table schema

Edit `scripts/bootstrap_tables.py`:

```sql
CREATE TABLE hive.raw.my_table (
    id BIGINT,
    name VARCHAR,
    created_at TIMESTAMP(3)
    -- Your columns here
)
```

### 3. Create dbt models

`dbt/models/staging/stg_my_data.sql`:
```sql
SELECT * FROM {{ source('raw', 'my_table') }}
WHERE id IS NOT NULL
```

`dbt/models/marts/fct_my_metrics.sql`:
```sql
SELECT
    date,
    COUNT(*) as total
FROM {{ ref('stg_my_data') }}
GROUP BY date
```

### 4. Configure Airflow DAG

Edit `airflow/dags/my_pipeline.py` with your tasks.

---

## Usage

### Query data (Trino CLI)

```bash
docker exec -it trino-coordinator trino --catalog iceberg --schema marts

trino> SELECT * FROM fct_my_metrics LIMIT 10;
```

### Query data (SQL Client)

Connect **DBeaver/DataGrip**:
- Host: `localhost`
- Port: `8080`
- Catalog: `iceberg`
- Schema: `marts`
- User: `admin`
- Driver: Trino JDBC (`drivers/trino-jdbc-438.jar`)

### Run pipeline

```bash
docker exec airflow-webserver airflow dags trigger my_pipeline
```

---

## Architecture

```
Raw Layer (Hive)         Staging (dbt)          Marts (Iceberg)
────────────────         ─────────────          ───────────────
hive.raw.my_table   →    stg_my_data     →      fct_my_metrics
Parquet (immutable)      Views                  Tables (ACID)
```

**Why 2 catalogs?**
- `hive` catalog: Raw data (simple Parquet)
- `iceberg` catalog: Marts (ACID transactions, time travel)

Both use the same Hive Metastore, but Iceberg adds advanced features.

---

## Common Commands

```bash
# Container management
docker compose ps
docker compose down
docker compose logs -f trino-coordinator

# Trino CLI
docker exec -it trino-coordinator trino

# dbt
docker exec airflow-webserver bash -c "cd /opt/airflow/dbt && dbt run --profiles-dir ."
docker exec airflow-webserver bash -c "cd /opt/airflow/dbt && dbt test --profiles-dir ."

# Airflow
docker exec airflow-webserver airflow dags list
docker exec airflow-webserver airflow dags trigger <dag_id>
```

---

## Requirements

- Docker + Docker Compose
- 8GB RAM minimum
- Ports: 8080, 8081, 9000, 9001, 5432, 5433, 9083

---

## License

MIT
