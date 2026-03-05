"""Airflow DAG: dbt deps → dbt run → dbt test, scheduled daily."""

from datetime import datetime

from airflow.decorators import dag
from airflow.operators.bash import BashOperator

# dbt project dir mounted as volume into the Airflow container
DBT_DIR = "/opt/airflow/dbt"


@dag(
    dag_id="dbt_and_load",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["dbt", "project2"],
)
def dbt_and_load() -> None:
    """Run dbt pipeline: install deps, execute models, run tests."""
    # install dbt package dependencies
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"dbt deps --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
    )

    # materialise staging + mart models
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
    )

    # assert not_null / unique constraints
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
    )

    dbt_deps >> dbt_run >> dbt_test


# instantiate DAG so Airflow discovers it
dag = dbt_and_load()
