# RUNBOOK

Operational runbook for the de-ml-monorepo.

## Start / Stop

```bash
# start all services
docker-compose up --build -d

# one-time Airflow DB + user init (first run only)
docker-compose --profile init up airflow-init

# stop and keep volumes
docker-compose down

# destroy volumes too
docker-compose down -v
```

## Health checks

```bash
# Kafka topic list
docker-compose exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092

# Postgres
docker-compose exec postgres psql -U deuser -d dedb -c "\dt"

# MinIO buckets
docker-compose exec minio mc ls local/

# Airflow
curl http://localhost:8085/health

# Superset
curl http://localhost:8088/health

# FastAPI
curl http://localhost:8000/health
```

## Superset BI (http://localhost:8088)

Login: admin / admin

### Connect Superset to the data warehouse
1. Open **Settings → Database Connections → + Database**
2. Choose **PostgreSQL**
3. Enter: `postgresql://deuser:depassword@postgres:5432/dedb`
4. Save and click **Test Connection**

### Suggested datasets / dashboards

| Project | SQL (paste in SQL Lab) |
|---------|------------------------|
| 1 – Streaming | `SELECT * FROM event_agg ORDER BY window_start DESC LIMIT 100` |
| 1 – Raw volume | `SELECT date, count(*) FROM raw_events GROUP BY date ORDER BY date DESC` |
| 2 – Daily metrics | `SELECT * FROM daily_event_metrics ORDER BY event_date DESC LIMIT 50` |
| 2 – Volume stats | `SELECT * FROM volume_stats` |
| 3 – Predictions | `SELECT * FROM prediction_log ORDER BY created_at DESC LIMIT 100` |

## Data volume analytics

```bash
# Quick row-count report from any machine with Postgres access
POSTGRES_HOST=localhost python data/volume_report.py
```

Expected output after the streaming pipeline has run for a few minutes:
```
Table                          Rows
-------------------------------------
raw_events                      >= 60
event_agg                       >=  1
stg_events                      >= 60
daily_event_metrics             >=  4
```

## Project 1 – verify streaming pipeline

```sql
-- connect: docker-compose exec postgres psql -U deuser -d dedb
SELECT * FROM event_agg ORDER BY window_start DESC LIMIT 10;
```

## Project 2 – trigger dbt manually

```bash
docker-compose run --rm dbt-runner dbt run --project-dir /dbt --profiles-dir /dbt
```

## Project 3 – smoke test prediction endpoint

```bash
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{"user_id": 1, "item_id": 50}'
```

## Common issues

| Symptom | Fix |
|---------|-----|
| Kafka producer can't connect | Wait 30 s for Kafka ready; check `scripts/wait-for.sh` |
| Airflow DB not initialised | `docker-compose --profile init up airflow-init` |
| Superset blank / 500 error | Wait 60 s for first-run init to complete |
| MinIO bucket missing | Bucket auto-created by spark_job on first write |
| model.pkl not found | Run `docker-compose --profile train run --rm trainer` |
