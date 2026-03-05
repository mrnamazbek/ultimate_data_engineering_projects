# Project 2 – Data Warehouse + dbt + Airflow 3.1

## Goal
Build a simple data warehouse on Postgres using dbt for transformations,
orchestrated by Apache Airflow 3.1.6.

## Stack
- **Postgres 15** – data warehouse
- **dbt-postgres 1.8** – SQL transformations
- **Apache Airflow 3.1.6** – orchestration (LocalExecutor, `@dag` decorator)
- **Apache Superset 6.0** – BI dashboards (shared service, port 8088)

## Why Airflow 3.1?
Airflow 3.x introduced the `@dag` / `@task` decorator API as the recommended
authoring style and moved to a cleaner auth manager architecture. Version 3.1.6
is the first fully-patched 3.1.x release (proxy credential leak CVE fixed).
Compared to 2.x: simpler DAG code, improved task SDK, better type hints.

## How to run

### Full stack
```bash
# From monorepo root:
docker-compose up --build postgres airflow-webserver airflow-scheduler
# Open http://localhost:8085 (admin/admin) and trigger dbt_and_load
```

### One-time Airflow init (first run)
```bash
docker-compose --profile init up airflow-init
```

### Run dbt manually
```bash
docker-compose --profile dbt run --rm dbt-runner
# or locally:
cd projects/project2-dw-dbt-airflow/dbt_project
dbt deps && dbt run --profiles-dir .
```

## Trigger DAG manually
In the Airflow UI, click ▶ next to `dbt_and_load`, or:
```bash
docker-compose exec airflow-webserver airflow dags trigger dbt_and_load
```

## Validation SQL
```sql
-- connect: docker-compose exec postgres psql -U deuser -d dedb

-- staging view
SELECT * FROM stg_events LIMIT 5;

-- mart table
SELECT * FROM daily_event_metrics ORDER BY event_date DESC LIMIT 10;

-- data volume snapshot
SELECT * FROM volume_stats;
```

## Superset BI
1. Open http://localhost:8088 (admin/admin)
2. Add Postgres database: `postgresql://deuser:depassword@postgres:5432/dedb`
3. Explore `daily_event_metrics` and `volume_stats` in SQL Lab or as charts

## Run tests
```bash
pip install pytest apache-airflow==3.1.6
pytest projects/project2-dw-dbt-airflow/tests/test_dag.py -v
```

## Trade-offs
- **LocalExecutor** suits single-node dev; use CeleryExecutor for production.
- **dbt on Airflow**: `BashOperator` keeps deps minimal; Cosmos gives richer
  DAG-level dbt task visibility.
- **No incremental models**: use `incremental` strategy for large datasets.
