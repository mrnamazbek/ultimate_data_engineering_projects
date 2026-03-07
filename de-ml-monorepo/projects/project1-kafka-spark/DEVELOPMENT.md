# Project 1 – Development Guide
## Real-time Streaming Pipeline: Kafka → PySpark → MinIO + Postgres

> **Goal:** Understand, build, and operate a real-time event-streaming pipeline
> from scratch, step by step — from the very first `docker-compose up` to
> querying aggregates and exploring BI dashboards in Superset.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Data Flow Diagram](#2-data-flow-diagram)
3. [Directory Structure](#3-directory-structure)
4. [Step 0 — Prerequisites](#4-step-0--prerequisites)
5. [Step 1 — Bootstrap the stack](#5-step-1--bootstrap-the-stack)
6. [Step 2 — Kafka Producer](#6-step-2--kafka-producer)
7. [Step 3 — Spark Streaming Job](#7-step-3--spark-streaming-job)
8. [Step 4 — MinIO (raw lake)](#8-step-4--minio-raw-lake)
9. [Step 5 — Postgres (aggregates)](#9-step-5--postgres-aggregates)
10. [Step 6 — Superset BI dashboards](#10-step-6--superset-bi-dashboards)
11. [Step 7 — Testing](#11-step-7--testing)
12. [Step 8 — CI/CD pipeline](#12-step-8--cicd-pipeline)
13. [Configuration reference](#13-configuration-reference)
14. [Troubleshooting](#14-troubleshooting)
15. [Extension ideas](#15-extension-ideas)

---

## 1. Architecture Overview

```
┌──────────────────────────────────────────────────────────────┐
│                       Project 1 Stack                        │
│                                                              │
│  ┌─────────────┐  JSON  ┌────────────┐  subscribe           │
│  │  Producer   │───────▶│   Kafka    │◀─────────────────┐   │
│  │ (Python)    │        │  :9092     │                  │   │
│  └─────────────┘        └─────┬──────┘                  │   │
│                               │ readStream               │   │
│                         ┌─────▼──────┐                  │   │
│                         │  PySpark   │                  │   │
│                         │ Structured │                  │   │
│                         │ Streaming  │                  │   │
│                         └──┬─────┬───┘                  │   │
│                            │     │                      │   │
│              parquet sink  │     │  foreachBatch sink   │   │
│           ┌────────────────┘     └──────────────────┐   │   │
│           ▼                                         ▼   │   │
│  ┌────────────────┐                       ┌─────────────┐   │
│  │  MinIO         │                       │  Postgres   │   │
│  │  raw-events/   │                       │  event_agg  │   │
│  │  events/       │                       │  table      │   │
│  │  date=YYYY-MM  │                       └─────────────┘   │
│  └────────────────┘                                         │
│                                                              │
│  ┌─────────────────────────────────────────────────────┐    │
│  │  Apache Superset  :8088  (shared with Project 2)    │    │
│  │  Dashboards: event volume · agg trends · raw counts  │    │
│  └─────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────┘
```

---

## 2. Data Flow Diagram

```mermaid
flowchart LR
    subgraph Producer["🐍 Kafka Producer"]
        P1[make_event&#40;id&#41;]
        P2[KafkaProducer.send&#40;&#41;]
        P1 --> P2
    end

    subgraph Kafka["🔀 Kafka Broker :9092"]
        T[Topic: events]
    end

    subgraph Spark["⚡ PySpark Streaming"]
        S1[readStream from Kafka]
        S2[from_json → parse schema]
        S3[Sink 1: parquet append]
        S4[Sink 2: foreachBatch]
        S5[window&#40;'1 minute'&#41;.count&#40;&#41;]
        S1 --> S2
        S2 --> S3
        S2 --> S4
        S4 --> S5
    end

    subgraph MinIO["🪣 MinIO s3a://raw-events"]
        M1["events/date=YYYY-MM-DD/*.parquet"]
    end

    subgraph Postgres["🐘 Postgres"]
        PG1[event_agg table]
    end

    subgraph BI["📊 Superset :8088"]
        BI1[event_agg dashboard]
        BI2[raw volume chart]
    end

    P2 -->|JSON bytes| T
    T -->|Kafka source| S1
    S3 -->|S3A write| M1
    S5 -->|JDBC append| PG1
    PG1 --> BI1
    PG1 --> BI2
```

---

## 3. Directory Structure

```
project1-kafka-spark/
├── services/
│   └── kafka_producer/
│       ├── producer.py        # event generator + Kafka sender
│       ├── requirements.txt   # kafka-python
│       └── Dockerfile
├── spark_job/
│   ├── streaming_job.py       # PySpark Structured Streaming
│   └── Dockerfile
├── tests/
│   ├── test_producer.py       # unit tests – event generation
│   ├── test_spark_job.py      # unit tests – JDBC config
│   └── test_integration_smoke.py  # smoke – requires live stack
├── requirements.txt           # dev / test deps
└── README.md
```

---

## 4. Step 0 — Prerequisites

| Requirement | Version | Check |
|-------------|---------|-------|
| Docker + Compose v2 | ≥ 24 | `docker compose version` |
| Python (local dev) | 3.10+ | `python --version` |
| 8 GB free RAM | — | `free -h` |

Copy the env file once from the monorepo root:

```bash
cd de-ml-monorepo
cp .env.example .env
```

All config lives in `.env` — never hard-code credentials in code.

---

## 5. Step 1 — Bootstrap the Stack

```bash
# Start only the services needed for Project 1
docker compose up --build \
  zookeeper kafka postgres minio \
  spark-master spark-worker \
  kafka-producer spark-job
```

**Service readiness order:**

```
Zookeeper (2181)
    └── Kafka (9092)           ← producer waits for healthy check
            └── kafka-producer
Postgres (5432)                ← spark-job waits for healthy check
MinIO (9000/9001)              ← spark-job waits for healthy check
Spark Master (8080/7077)
    └── spark-worker
            └── spark-job
```

Healthcheck logs — confirm services are ready:

```
kafka          | [KafkaServer] started
minio          | MinIO Object Storage Server
postgres       | database system is ready to accept connections
kafka-producer | Producing to kafka:9092/events at 1.0 eps
```

---

## 6. Step 2 — Kafka Producer

**File:** `services/kafka_producer/producer.py`

### What it does

1. Reads config from env vars (`KAFKA_BROKER`, `KAFKA_TOPIC`, `PRODUCER_RATE`)
2. Creates a `KafkaProducer` with JSON serialisation
3. Loops forever, building a synthetic event and sending it to Kafka

### Event schema

```json
{
  "id":      42,
  "ts":      "2024-06-01T12:00:00+00:00",
  "value":   "click",
  "user_id": 317
}
```

| Field | Type | Description |
|-------|------|-------------|
| `id` | int | Auto-incrementing event counter |
| `ts` | ISO-8601 UTC | Event timestamp |
| `value` | str | One of: click, view, purchase, scroll |
| `user_id` | int 1–1000 | Simulated user identifier |

### Tune the rate

```bash
# Send 5 events per second
PRODUCER_RATE=5 docker compose up kafka-producer
```

### Inspect messages manually

```bash
docker compose exec kafka \
  kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic events \
  --from-beginning \
  --max-messages 5
```

---

## 7. Step 3 — Spark Streaming Job

**File:** `spark_job/streaming_job.py`

### Build steps inside the job

```
1. build_spark()          → SparkSession configured with S3A + Kafka jars
2. readStream(kafka)      → raw DataFrame: key, value (bytes), offset, …
3. from_json(value)       → typed DataFrame: id, ts, value, user_id
4. Sink 1 (raw parquet)   → s3a://raw-events/events/ partitioned by date
5. Sink 2 (aggregates)    → foreachBatch → window(1 min).count() → JDBC
6. awaitTermination()     → block until both queries stop
```

### Checkpoint locations

Checkpoints are written to MinIO so the job survives restarts:

```
s3a://raw-events/checkpoints/raw/   ← for the parquet sink
s3a://raw-events/checkpoints/agg/   ← for the aggregate sink
```

> **Caution:** If you wipe MinIO, delete checkpoints too, or Spark will
> attempt to resume from an offset that no longer exists in Kafka.

### Trigger interval

The job uses the default micro-batch trigger (continuous mode can be enabled
for lower latency — see [Extension ideas](#15-extension-ideas)).

---

## 8. Step 4 — MinIO (Raw Lake)

MinIO is an S3-compatible object store — it holds the **raw parquet lake**.

### Explore via console

Open **http://localhost:9001** → login `gemini / claude`

Navigate to `raw-events → events →` to see date-partitioned Parquet files:

```
raw-events/
└── events/
    ├── date=2024-06-01/
    │   └── part-00000-….parquet
    └── date=2024-06-02/
        └── part-00000-….parquet
```

### Read a Parquet file with PySpark

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("read").getOrCreate()
df = spark.read.parquet("s3a://raw-events/events/")
df.show(5)
```

---

## 9. Step 5 — Postgres (Aggregates)

The `event_agg` table is created by `scripts/init-db.sh` on first Postgres start.

```sql
-- connect
docker compose exec postgres psql -U gemini -d dedb

-- check aggregates
SELECT window_start, window_end, event_count
FROM event_agg
ORDER BY window_start DESC
LIMIT 10;
```

Expected output (after ~1 minute of producer running):

```
      window_start      |       window_end        | event_count
------------------------+-------------------------+-------------
 2024-06-01 12:01:00    | 2024-06-01 12:02:00     |          60
 2024-06-01 12:00:00    | 2024-06-01 12:01:00     |          60
```

### Data volume report

```bash
# From monorepo root
POSTGRES_HOST=localhost python data/volume_report.py
```

```
Table                          Rows
-------------------------------------
raw_events                      120
event_agg                         2
stg_events                      120
daily_event_metrics               4
```

---

## 10. Step 6 — Superset BI Dashboards

Superset (port 8088) is shared by all three projects.

### Connect Superset to Postgres

1. Open **http://localhost:8088** → admin / admin
2. **Settings → Database Connections → + Database → PostgreSQL**
3. SQLAlchemy URI: `postgresql://gemini:claude@postgres:5432/dedb`
4. Click **Test Connection → Save**

### Suggested charts for Project 1

**Chart 1 — Event counts per minute (line chart)**

```sql
SELECT
    window_start,
    event_count
FROM event_agg
ORDER BY window_start;
```

**Chart 2 — Rolling 5-minute event volume (bar chart)**

```sql
SELECT
    DATE_TRUNC('minute', window_start) AS ts,
    SUM(event_count) AS total
FROM event_agg
GROUP BY 1
ORDER BY 1 DESC
LIMIT 30;
```

**Chart 3 — Data lake row count (big number)**

```sql
SELECT COUNT(*) AS raw_event_rows FROM raw_events;
```

---

## 11. Step 7 — Testing

### Unit tests (no Docker required)

```bash
pip install -r requirements.txt
pytest tests/test_producer.py tests/test_spark_job.py -v
```

| Test | What it verifies |
|------|-----------------|
| `test_make_event_fields` | Event dict has required keys |
| `test_make_event_unique_ids` | Sequential IDs are unique |
| `test_run_sends_events` | `producer.send` is called `n` times |
| `test_jdbc_url_format` | JDBC URL is a valid postgres string |
| `test_jdbc_props_keys` | JDBC props contain user/password/driver |

### Integration smoke test (requires live stack)

```bash
pytest tests/test_integration_smoke.py -v
```

This test:
1. Connects to Kafka and checks the `events` topic exists
2. Connects to Postgres and checks `event_agg` has rows
3. Connects to MinIO and checks the `raw-events` bucket has objects

---

## 12. Step 8 — CI/CD Pipeline

The GitHub Actions workflow (`.github/workflows/ci.yml`) runs on every push:

```
┌─────────────────────────────────────────────────┐
│                 CI Pipeline                     │
│                                                 │
│  lint  ──────────────────────────────────────┐  │
│  (ruff + black)                              │  │
│                                              ▼  │
│  test-project1  ──▶  test-project2  ──▶  test-project3  │
│  (unit)              (unit)              (unit) │
│                                              │  │
│                                              ▼  │
│              build-images (Docker)           │  │
│              (all 5 Dockerfiles)             │  │
│                                              │  │
│                                              ▼  │
│            smoke-integration                 │  │
│      (Postgres + Kafka + MinIO live)         │  │
└─────────────────────────────────────────────────┘
```

All secrets (`POSTGRES_PASSWORD`, MinIO keys, etc.) come from env vars — no
credentials are ever committed to the repository.

---

## 13. Configuration Reference

All settings are read from environment variables with safe defaults:

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKER` | `kafka:9092` | Kafka bootstrap server |
| `KAFKA_TOPIC` | `events` | Topic name |
| `PRODUCER_RATE` | `1` | Events per second |
| `MINIO_ENDPOINT` | `minio:9000` | MinIO host:port |
| `MINIO_ACCESS_KEY` | `gemini` | MinIO access key |
| `MINIO_SECRET_KEY` | `claude` | MinIO secret key |
| `MINIO_BUCKET` | `raw-events` | Target bucket |
| `POSTGRES_HOST` | `postgres` | Postgres host |
| `POSTGRES_PORT` | `5432` | Postgres port |
| `POSTGRES_USER` | `gemini` | DB user |
| `POSTGRES_PASSWORD` | `claude` | DB password |
| `POSTGRES_DB` | `dedb` | Database name |

---

## 14. Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| Producer exits immediately | Kafka not ready | Increase healthcheck retries; check Zookeeper |
| Spark job exits with S3 error | MinIO bucket missing | Bucket is auto-created on first write; check MinIO logs |
| `event_agg` is empty after 2 min | Watermark or checkpoint issue | Delete `s3a://raw-events/checkpoints/` and restart spark-job |
| Parquet files not appearing | Micro-batch hasn't flushed yet | Wait 10–30 s; check Spark UI at http://localhost:8080 |
| Kafka `leader not available` | Kafka still starting | Wait 30 s; healthcheck ensures readiness |

---

## 15. Extension Ideas

| Idea | Description |
|------|-------------|
| **Flink instead of Spark** | Sub-second latency, event-time windowing |
| **Schema Registry** | Avro + Confluent Schema Registry for schema evolution |
| **Exactly-once delivery** | Enable Kafka idempotent producer + transactional sink |
| **Continuous trigger** | `trigger(processingTime='1 second')` for lower latency |
| **Delta Lake** | Replace plain Parquet with Delta for ACID + time-travel |
| **Grafana + Prometheus** | Operational metrics (consumer lag, throughput) |
| **Real events** | Replace synthetic producer with Debezium CDC from a real DB |
