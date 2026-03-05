# Checklist

## Project 1 – Kafka + Spark
- [ ] Kafka producer sends events to `events` topic
- [ ] Spark job consumes topic and writes parquet to MinIO
- [ ] Spark job writes aggregates to Postgres `event_agg`
- [ ] Unit tests pass (producer mock)
- [ ] Integration smoke test passes (10 s producer + Spark local)

## Project 2 – DW + dbt + Airflow
- [ ] dbt staging models run without errors
- [ ] dbt marts `daily_event_metrics` populated
- [ ] dbt tests (not_null, unique) pass
- [ ] Airflow DAG `dbt_and_load` visible in UI
- [ ] DAG can be triggered manually and succeeds

## Project 3 – ML Deploy
- [ ] Trainer script creates `model.pkl`
- [ ] FastAPI `/predict` returns a score
- [ ] FastAPI `/health` returns 200
- [ ] Unit inference tests pass
- [ ] Docker image builds and container responds to POST `/predict`
- [ ] K8s manifests are valid YAML (kubeval / dry-run)

## CI
- [ ] ruff / flake8 passes on all Python files
- [ ] black --check passes
- [ ] pytest passes for all three projects
- [ ] Docker images build in CI
- [ ] Smoke integration test passes

## Infra
- [ ] docker-compose up --build completes without errors
- [ ] All service health checks green
- [ ] `.env.example` contains all required variables
