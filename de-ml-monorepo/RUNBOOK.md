# RUNBOOK

Operational runbook for the de-ml-monorepo.

## Start / Stop

```bash
# start all services
docker-compose up --build -d

# stop and remove containers (keep volumes)
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

# FastAPI
curl http://localhost:8000/health
```

## Project 1 – verify streaming pipeline

```sql
-- connect to Postgres and run:
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
| Kafka producer can't connect | Wait 30 s for Kafka to be ready; check `wait-for.sh` in scripts/ |
| Airflow DB not initialised | `docker-compose run --rm airflow-init` |
| MinIO bucket missing | Bucket is auto-created by spark_job on first write |
| model.pkl not found | Run `docker-compose run --rm trainer` first |
