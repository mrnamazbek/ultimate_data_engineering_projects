# Project 2 – Data Warehouse + dbt + Airflow

## Goal
Build a simple data warehouse on Postgres using dbt for transformations,
orchestrated by Apache Airflow.

## Stack
- **Postgres 15** – data warehouse
- **dbt-postgres 1.8** – SQL transformations
- **Apache Airflow 2.9** – orchestration (LocalExecutor)

## How to run

### Full stack
```bash
# From monorepo root:
docker-compose up --build postgres airflow-webserver airflow-scheduler
# Then open http://localhost:8085 (admin/admin) and trigger the dbt_and_load DAG
```

### Run dbt manually
```bash
docker-compose run --rm dbt-runner
# or locally:
cd projects/project2-dw-dbt-airflow/dbt_project
dbt deps && dbt run --profiles-dir .
```

## Trigger DAG manually
In the Airflow UI, click the ▶ button next to `dbt_and_load`, or:
```bash
docker-compose exec airflow-webserver airflow dags trigger dbt_and_load
```

## Validation SQL
```sql
-- Connect: docker-compose exec postgres psql -U deuser -d dedb

-- Staging view
SELECT * FROM stg_events LIMIT 5;

-- Mart table
SELECT * FROM daily_event_metrics ORDER BY event_date DESC LIMIT 10;

-- Expected: rows grouped by date and event_type with counts
```

## Run tests
```bash
pip install pytest apache-airflow==2.9.1
pytest projects/project2-dw-dbt-airflow/tests/test_dag.py -v
```

## Trade-offs
- **LocalExecutor** is suitable for a single-node dev setup; switch to
  CeleryExecutor / KubernetesExecutor for production scale.
- **dbt on Airflow**: using `BashOperator` keeps dependencies minimal; consider
  `DbtCloudRunJobOperator` or Cosmos for richer integration.
- **No incremental models**: all models are `view` or `table` for simplicity;
  add `incremental` strategy for large datasets.
