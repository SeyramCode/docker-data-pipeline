"""Daily batch: MySQL -> bronze -> silver -> Postgres raw -> dbt build (gold marts)."""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

SPARK = "cd /opt/pipeline/spark_jobs && /opt/spark-venv/bin/python"
DBT = "/opt/dbt-venv/bin/dbt"
DBT_ARGS = "--project-dir /opt/pipeline/dbt --profiles-dir /opt/pipeline/dbt"

with DAG(
    dag_id="batch_pipeline",
    schedule=os.environ.get("PIPELINE_SCHEDULE", "@daily"),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["batch", "medallion"],
    doc_md=__doc__,
) as dag:
    mysql_to_bronze = BashOperator(
        task_id="mysql_to_bronze",
        bash_command=f"{SPARK} bronze.py --batch-id '{{{{ run_id }}}}'",
    )
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=f"{SPARK} silver.py",
    )
    silver_to_warehouse = BashOperator(
        task_id="silver_to_warehouse",
        bash_command=f"{SPARK} load_warehouse.py",
    )
    # `build` runs models and tests in dependency order; a failing test stops the run
    # before bad data reaches downstream marts.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{DBT} build {DBT_ARGS}",
    )

    mysql_to_bronze >> bronze_to_silver >> silver_to_warehouse >> dbt_build
