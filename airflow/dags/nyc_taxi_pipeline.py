"""
NYC Taxi Data Pipeline - Airflow DAG

Spark ingestion + dbt transformations over Iceberg tables.
Schedule: Manual (on-demand)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

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
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['nyc_taxi', 'spark', 'iceberg', 'dbt', 'production'],
    max_active_runs=1,
)


def log_pipeline_completion(**context):
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


with dag:

    ingest = BashOperator(
        task_id='ingest_spark',
        bash_command='docker exec spark-master bash /opt/scripts/nyc_taxi/run_spark_ingest_bulk.sh ',
    )

    dbt_staging = BashOperator(
        task_id='dbt_staging',
        bash_command='cd /opt/airflow/dbt && dbt deps --profiles-dir . && dbt run --select staging --profiles-dir . --target dev ',
    )

    dbt_intermediate = BashOperator(
        task_id='dbt_intermediate',
        bash_command='cd /opt/airflow/dbt && dbt run --select intermediate --profiles-dir . --target dev ',
    )

    dbt_marts = BashOperator(
        task_id='dbt_marts',
        bash_command='cd /opt/airflow/dbt && dbt run --select marts --profiles-dir . --target dev ',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/dbt && dbt test --profiles-dir . --target dev ',
    )

    dbt_docs = BashOperator(
        task_id='dbt_docs',
        bash_command='docker exec airflow-webserver bash -c "cd /opt/airflow/dbt && dbt deps --profiles-dir . && dbt docs generate --profiles-dir . --target dev && pkill -f \'python3 -m http.server 8082\' || true && cd /tmp/dbt-target && nohup python3 -m http.server 8082 --bind 0.0.0.0 > /tmp/dbt-docs-serve.log 2>&1 & sleep 2 && echo \'dbt docs available at http://localhost:8082\'" ',
    )

    log_completion = PythonOperator(
        task_id='log_completion',
        python_callable=log_pipeline_completion,
        provide_context=True,
    )

    ingest >> dbt_staging >> dbt_intermediate >> dbt_marts >> dbt_test >> dbt_docs >> log_completion
