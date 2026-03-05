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
| 2 | DW + dbt + Airflow | Postgres, dbt, Airflow 3.1.6 | 8085, 5432 |
| 3 | ML Deploy (FastAPI) | FastAPI, Docker, K8s manifests | 8000 |

## Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8085 | admin / admin |
| Superset BI | http://localhost:8088 | admin / admin |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Spark Master UI | http://localhost:8080 | — |
| FastAPI | http://localhost:8000/docs | — |
| Postgres | localhost:5432 | deuser / depassword |
| Kafka | localhost:9092 | — |

## Requirements

- Docker ≥ 24 & Docker Compose v2
- 8 GB RAM recommended
- Python 3.10+ (for local dev / tests)

## Airflow version note

Airflow **3.1.6** is used (latest stable 3.x as of this writing). It is the
first fully-patched 3.1.x release (CVE for proxy credential leak fixed in
3.1.6). The `@dag` decorator style is used throughout, which is the
recommended pattern for Airflow 3.x.

Earlier snapshots used 2.9.1 → 2.11.1 for CVE fixes; 3.1.6 supersedes both.

## Data volume check

```bash
# Quick row-count report across all pipeline tables
POSTGRES_HOST=localhost python data/volume_report.py
```

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
