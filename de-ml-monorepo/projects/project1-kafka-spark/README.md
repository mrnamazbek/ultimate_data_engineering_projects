# Project 1 – Kafka + Spark Streaming

## Goal
Ingest synthetic events via Kafka, process with PySpark Structured Streaming,
write raw parquet to MinIO (partitioned by date), and write windowed aggregates
to Postgres.

## Stack
- **Kafka** (bitnami/kafka:3.6) – message broker
- **PySpark 3.5** – structured streaming
- **MinIO** – S3-compatible object storage (raw parquet sink)
- **Postgres 15** – aggregation sink (`event_agg` table)

## How to run

```bash
# From monorepo root:
docker-compose up --build zookeeper kafka postgres minio spark-master spark-worker kafka-producer spark-job
```

## Verify

### Check aggregates in Postgres
```sql
-- docker-compose exec postgres psql -U gemini -d dedb
SELECT * FROM event_agg ORDER BY window_start DESC LIMIT 10;
```
Expected: rows appear within ~1 minute of the producer starting.

### Check parquet files in MinIO
```bash
docker-compose exec minio mc ls local/raw-events/events/
```

## Run tests
```bash
pip install -r requirements.txt
pytest tests/ -v
```

## Trade-offs / Limitations
- **Spark vs Flink**: Spark micro-batch has higher latency (seconds) than Flink
  (milliseconds). For sub-second SLA, Flink is a better fit.
- **exactly-once**: Kafka + Spark provides at-least-once by default. To achieve
  exactly-once, enable idempotent writes + transactional Kafka sink.
- **Checkpointing**: stored on MinIO; if MinIO is wiped, Spark re-reads from
  earliest offsets.
- **Schema evolution**: changing `EVENT_SCHEMA` requires resetting checkpoints.
