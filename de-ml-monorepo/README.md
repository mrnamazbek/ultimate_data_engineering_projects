# de-ml-monorepo

Three end-to-end data engineering / ML projects running locally with a single command.

## Quick start

```bash
cp .env.example .env
docker-compose up --build
```

## Projects

| # | Name | Stack | Port(s) |
|---|------|-------|---------|
| 1 | Kafka + Spark Streaming | Kafka, PySpark, MinIO, Postgres | 9092, 8080 |
| 2 | DW + dbt + Airflow | Postgres, dbt, Airflow | 8085, 5432 |
| 3 | ML Deploy (FastAPI) | FastAPI, Docker, K8s manifests | 8000 |

## Services

| Service | URL |
|---------|-----|
| Airflow UI | http://localhost:8085 |
| MinIO Console | http://localhost:9001 |
| Spark Master UI | http://localhost:8080 |
| FastAPI | http://localhost:8000/docs |
| Postgres | localhost:5432 |
| Kafka | localhost:9092 |

## Requirements

- Docker ≥ 24 & Docker Compose v2
- 8 GB RAM recommended
- Python 3.10+ (for local dev / tests)

## Running tests locally

```bash
pip install -r projects/project1-kafka-spark/requirements.txt
pytest projects/project1-kafka-spark/tests/

pip install -r projects/project2-dw-dbt-airflow/requirements.txt
pytest projects/project2-dw-dbt-airflow/tests/

pip install -r projects/project3-ml-deploy/requirements.txt
pytest projects/project3-ml-deploy/tests/
```

See each project's `README.md` for more detail.
