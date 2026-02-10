"""
NYC Taxi Data Pipeline - Airflow DAG

Pipeline completo de ingestão e transformação de dados do NYC Taxi usando:
- Spark + Iceberg (ingestão com ACID)
- dbt (transformações)
- Trino (query engine)

Schedule: Manual (on-demand)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ============================================================================
# Configurações da DAG
# ============================================================================

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='nyc_taxi_pipeline',
    default_args=default_args,
    description='NYC Taxi pipeline: Spark ingestion + dbt transformations',
    schedule_interval=None,  # Manual execution
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['nyc_taxi', 'spark', 'iceberg', 'dbt', 'production'],
    max_active_runs=1,
)


# ============================================================================
# Task Functions
# ============================================================================

def log_pipeline_completion(**context):
    """Log final da pipeline"""
    print("\n" + "=" * 80)
    print("NYC TAXI PIPELINE COMPLETED SUCCESSFULLY")
    print("=" * 80)
    print(f"Execution date: {context['ds']}")
    print(f"Logical date: {context['logical_date']}")
    print("\nData available at:")
    print("  - Raw: iceberg.raw.*_trips")
    print("  - Staging: iceberg.staging.stg_nyc_taxi__*")
    print("  - Intermediate: iceberg.intermediate.int_*")
    print("\nQuery with Trino or dbt!")


# ============================================================================
# Tasks Definition
# ============================================================================

with dag:

    # Task 1: Ingestão Spark (yellow taxi 2023-02)
    ingest = BashOperator(
        task_id='ingest_spark',
        bash_command='docker exec spark-master bash /opt/scripts/nyc_taxi/run_spark_ingest_bulk.sh',
    )

    # Task 2: dbt - Staging models
    dbt_staging = BashOperator(
        task_id='dbt_staging',
        bash_command='cd /opt/airflow/dbt && dbt run --select staging --profiles-dir . --target dev',
    )

    # Task 3: dbt - Intermediate models
    dbt_intermediate = BashOperator(
        task_id='dbt_intermediate',
        bash_command='cd /opt/airflow/dbt && dbt run --select intermediate --profiles-dir . --target dev',
    )

    # Task 4: dbt - Tests
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/dbt && dbt test --profiles-dir . --target dev',
    )

    # Task 5: Log completion
    log_completion = PythonOperator(
        task_id='log_completion',
        python_callable=log_pipeline_completion,
        provide_context=True,
    )

    # Pipeline: Spark → dbt staging → dbt intermediate → dbt test → log
    ingest >> dbt_staging >> dbt_intermediate >> dbt_test >> log_completion
