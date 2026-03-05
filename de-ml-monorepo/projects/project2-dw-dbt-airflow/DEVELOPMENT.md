# Project 2 – Development Guide
## Data Warehouse + dbt + Airflow 3.1.6

> **Goal:** Understand, build, and operate an ELT data warehouse pipeline
> using Postgres as the warehouse, dbt for SQL transformations, and
> Apache Airflow 3.1.6 for daily orchestration — from initial setup to
> Superset BI dashboards.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Data Lineage Diagram](#2-data-lineage-diagram)
3. [Airflow DAG Diagram](#3-airflow-dag-diagram)
4. [Directory Structure](#4-directory-structure)
5. [Step 0 — Prerequisites](#5-step-0--prerequisites)
6. [Step 1 — Bootstrap the Stack](#6-step-1--bootstrap-the-stack)
7. [Step 2 — Postgres Warehouse Design](#7-step-2--postgres-warehouse-design)
8. [Step 3 — dbt Transformations](#8-step-3--dbt-transformations)
9. [Step 4 — Airflow DAG](#9-step-4--airflow-dag)
10. [Step 5 — Data Volume Analytics](#10-step-5--data-volume-analytics)
11. [Step 6 — Superset BI Dashboards](#11-step-6--superset-bi-dashboards)
12. [Step 7 — Testing](#12-step-7--testing)
13. [Step 8 — CI/CD Pipeline](#13-step-8--cicd-pipeline)
14. [Configuration Reference](#14-configuration-reference)
15. [Troubleshooting](#15-troubleshooting)
16. [Extension Ideas](#16-extension-ideas)

---

## 1. Architecture Overview

```
┌──────────────────────────────────────────────────────────────────┐
│                        Project 2 Stack                           │
│                                                                  │
│  ┌──────────────────┐          ┌────────────────────────────┐    │
│  │   Apache Airflow  │          │       dbt-postgres          │    │
│  │   3.1.6           │  trigger │                            │    │
│  │   @daily DAG      │─────────▶│  1. dbt deps               │    │
│  │   dbt_and_load    │          │  2. dbt run                │    │
│  │                   │          │  3. dbt test               │    │
│  └──────────────────┘          └──────────┬─────────────────┘    │
│                                           │ SQL                  │
│                                           ▼                      │
│  ┌──────────────────────────────────────────────────────────┐    │
│  │                  Postgres 15 Data Warehouse               │    │
│  │                                                          │    │
│  │  raw_events (source)                                     │    │
│  │      └── stg_events     (staging – cast + filter)        │    │
│  │              └── daily_event_metrics  (mart – agg)       │    │
│  │              └── volume_stats         (mart – row counts) │    │
│  └──────────────────────────────────────────────────────────┘    │
│                                           │                      │
│                                           ▼                      │
│  ┌─────────────────────────────────────────────────────────┐     │
│  │  Apache Superset  :8088                                  │     │
│  │  daily_event_metrics · volume_stats · event trends       │     │
│  └─────────────────────────────────────────────────────────┘     │
└──────────────────────────────────────────────────────────────────┘
```

---

## 2. Data Lineage Diagram

```mermaid
flowchart TD
    subgraph Sources["📥 Sources"]
        SRC1[(raw_events\nPostgres table)]
        SRC2[(event_agg\nPostgres table)]
    end

    subgraph Staging["🔄 Staging Layer"]
        STG[stg_events\n• cast ts → timestamp\n• rename value → event_type\n• add event_date\n• filter null ts]
    end

    subgraph Marts["📦 Mart Layer"]
        MART1[daily_event_metrics\n• GROUP BY event_date, event_type\n• COUNT&#40;*&#41; as event_count]
        MART2[volume_stats\n• COUNT&#40;*&#41; per table\n• for Superset monitoring]
    end

    subgraph BI["📊 Superset Dashboards"]
        DASH1[Daily Event Trends]
        DASH2[Volume Health Monitor]
    end

    SRC1 -->|source&#40;'raw','raw_events'&#41;| STG
    SRC2 -->|source&#40;'raw','event_agg'&#41;| MART2
    STG  -->|ref&#40;'stg_events'&#41;| MART1
    STG  -->|ref&#40;'stg_events'&#41;| MART2
    MART1 --> DASH1
    MART2 --> DASH2
```

---

## 3. Airflow DAG Diagram

```mermaid
flowchart LR
    subgraph DAG["📅 dbt_and_load  &#40;@daily, catchup=False&#41;"]
        T1["dbt_deps\n&#40;BashOperator&#41;\ndbt deps --project-dir /opt/airflow/dbt"]
        T2["dbt_run\n&#40;BashOperator&#41;\ndbt run --project-dir /opt/airflow/dbt"]
        T3["dbt_test\n&#40;BashOperator&#41;\ndbt test --project-dir /opt/airflow/dbt"]
        T1 --> T2 --> T3
    end
```

The DAG uses the **`@dag` decorator** — the recommended Airflow 3.x authoring style.
Each task is a `BashOperator` that delegates to the `dbt` CLI inside the container.

---

## 4. Directory Structure

```
project2-dw-dbt-airflow/
├── airflow/
│   └── dags/
│       └── dbt_and_load.py        # Airflow @dag decorator DAG
├── dbt_project/
│   ├── dbt_project.yml            # project name, version, model paths
│   ├── profiles.yml               # Postgres connection (reads env vars)
│   ├── Dockerfile                 # dbt-runner image
│   └── models/
│       ├── staging/
│       │   ├── stg_events.sql     # clean + type-cast raw events
│       │   └── schema.yml         # source + staging column tests
│       └── marts/
│           ├── daily_event_metrics.sql   # daily aggregation mart
│           ├── volume_stats.sql          # row-count monitoring model
│           └── schema.yml               # mart column tests
├── tests/
│   ├── test_dag.py                # AST + optional live-import tests
│   └── test_integration_smoke.py  # smoke – requires live stack
└── requirements.txt
```

---

## 5. Step 0 — Prerequisites

| Requirement | Version | Check |
|-------------|---------|-------|
| Docker + Compose v2 | ≥ 24 | `docker compose version` |
| Python (local dev) | 3.10+ | `python --version` |
| 8 GB free RAM | — | `free -h` |

```bash
cd de-ml-monorepo
cp .env.example .env
```

---

## 6. Step 1 — Bootstrap the Stack

```bash
# First-time only: initialise Airflow DB and create admin user
docker compose --profile init up airflow-init

# Then start all Project 2 services
docker compose up --build postgres airflow-webserver airflow-scheduler superset
```

**Service startup order:**

```
Postgres (5432) [healthcheck]
    ├── Airflow Init  ← runs once, then exits
    ├── Airflow Webserver  (localhost:8085)
    ├── Airflow Scheduler
    └── Superset (localhost:8088)
```

Login: **http://localhost:8085** → admin / admin

---

## 7. Step 2 — Postgres Warehouse Design

```sql
-- Source table (written by Project 1 Spark job or test data)
CREATE TABLE IF NOT EXISTS raw_events (
    id      SERIAL PRIMARY KEY,
    ts      TIMESTAMP NOT NULL,
    value   TEXT,                  -- click | view | purchase | scroll
    user_id INTEGER
);

-- Aggregation table (written by Spark streaming)
CREATE TABLE IF NOT EXISTS event_agg (
    window_start TIMESTAMP NOT NULL,
    window_end   TIMESTAMP NOT NULL,
    event_count  BIGINT    NOT NULL,
    PRIMARY KEY (window_start, window_end)
);
```

dbt creates **views/tables on top** of these sources — never modifying them.

### Load test data

```sql
-- Insert sample rows for dbt development
INSERT INTO raw_events (ts, value, user_id)
SELECT
    NOW() - (interval '1 second' * generate_series),
    (ARRAY['click','view','purchase','scroll'])[floor(random()*4)+1],
    floor(random()*1000)::int
FROM generate_series(1, 500);
```

---

## 8. Step 3 — dbt Transformations

### 8.1 profiles.yml — Postgres connection

```yaml
de_project2:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('POSTGRES_HOST', 'localhost') }}"
      port: "{{ env_var('POSTGRES_PORT', '5432') | int }}"
      user: "{{ env_var('POSTGRES_USER', 'deuser') }}"
      pass: "{{ env_var('POSTGRES_PASSWORD', 'depassword') }}"
      dbname: "{{ env_var('POSTGRES_DB', 'dedb') }}"
      schema: public
      threads: 1
```

All credentials come from env vars — no secrets in code.

### 8.2 Staging model: `stg_events.sql`

```sql
-- clean and type-cast raw_events
select
    id,
    ts::timestamp   as event_ts,
    value           as event_type,
    user_id,
    ts::date        as event_date
from {{ source('raw', 'raw_events') }}
where ts is not null
```

### 8.3 Mart model: `daily_event_metrics.sql`

```sql
-- daily event counts by type
select
    event_date,
    event_type,
    count(*) as event_count
from {{ ref('stg_events') }}
group by event_date, event_type
order by event_date desc, event_count desc
```

### 8.4 Monitoring model: `volume_stats.sql`

```sql
-- row-count snapshot; used in Superset for pipeline health
select 'raw_events'          as table_name, count(*) as row_count
  from {{ source('raw', 'raw_events') }}
union all
select 'event_agg',                         count(*)
  from {{ source('raw', 'event_agg') }}
union all
select 'stg_events',                        count(*)
  from {{ ref('stg_events') }}
union all
select 'daily_event_metrics',              count(*)
  from {{ ref('daily_event_metrics') }}
order by table_name
```

### 8.5 Run dbt manually

```bash
# Inside dbt-runner container
docker compose --profile dbt run --rm dbt-runner \
  bash -c "dbt deps && dbt run && dbt test"

# Locally (needs dbt-postgres installed)
cd dbt_project
dbt deps && dbt run --profiles-dir . && dbt test --profiles-dir .
```

---

## 9. Step 4 — Airflow DAG

**File:** `airflow/dags/dbt_and_load.py`

### Why the `@dag` decorator?

Airflow 3.x recommends the decorator API over the `with DAG(...):` context
manager because:

- Cleaner function-level encapsulation
- Better IDE type-checking and auto-complete
- Compatible with the new Task SDK (`@task` decorator)

```python
@dag(dag_id="dbt_and_load", schedule="@daily", catchup=False)
def dbt_and_load() -> None:
    dbt_deps = BashOperator(task_id="dbt_deps", bash_command="dbt deps …")
    dbt_run  = BashOperator(task_id="dbt_run",  bash_command="dbt run …")
    dbt_test = BashOperator(task_id="dbt_test", bash_command="dbt test …")
    dbt_deps >> dbt_run >> dbt_test

dag = dbt_and_load()   # instantiate so Airflow discovers it
```

### Trigger the DAG manually

```bash
# Via CLI
docker compose exec airflow-webserver \
  airflow dags trigger dbt_and_load

# Via UI: http://localhost:8085 → click ▶ on dbt_and_load
```

### View task logs

In the Airflow UI: DAGs → dbt_and_load → click a run → click a task → Logs

---

## 10. Step 5 — Data Volume Analytics

### dbt `volume_stats` model

After `dbt run`, query volume stats from any Postgres client:

```sql
-- docker compose exec postgres psql -U deuser -d dedb
SELECT * FROM volume_stats ORDER BY table_name;
```

```
    table_name         | row_count
-----------------------+-----------
 daily_event_metrics   |         4
 event_agg             |         2
 raw_events            |       500
 stg_events            |       500
```

### Standalone volume report script

```bash
# From monorepo root — works with any accessible Postgres
POSTGRES_HOST=localhost python data/volume_report.py
```

---

## 11. Step 6 — Superset BI Dashboards

### Connect to Postgres

1. **http://localhost:8088** → admin / admin
2. **Settings → Database Connections → + Database → PostgreSQL**
3. URI: `postgresql://deuser:depassword@postgres:5432/dedb`
4. Test Connection → Save

### Suggested Datasets & Charts

**Dataset 1: `daily_event_metrics`**

| Chart type | SQL / Column | Title |
|------------|-------------|-------|
| Line chart | `event_date` (x), `event_count` (y), group by `event_type` | Daily events by type |
| Bar chart | `event_date` (x), `SUM(event_count)` (y) | Total events per day |
| Table | all columns | Events table |

**Dataset 2: `volume_stats`**

| Chart type | SQL | Title |
|------------|-----|-------|
| Big Number | `SELECT SUM(row_count) FROM volume_stats` | Total rows in warehouse |
| Table | `SELECT * FROM volume_stats` | Pipeline health snapshot |
| Bar chart | `table_name` (x), `row_count` (y) | Rows per table |

**SQL Lab — ad-hoc queries:**

```sql
-- Event funnel
SELECT
    event_type,
    COUNT(*) AS events,
    COUNT(DISTINCT user_id) AS unique_users
FROM stg_events
GROUP BY event_type
ORDER BY events DESC;

-- Purchase conversion rate
SELECT
    event_date,
    SUM(CASE WHEN event_type = 'purchase' THEN event_count END) * 1.0 /
    NULLIF(SUM(event_count), 0) AS purchase_rate
FROM daily_event_metrics
GROUP BY event_date
ORDER BY event_date;
```

---

## 12. Step 7 — Testing

### Unit tests (no Docker required)

```bash
pip install pytest apache-airflow==3.1.6 \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.6/constraints-3.10.txt"

pytest tests/test_dag.py -v
```

| Test | What it verifies |
|------|-----------------|
| `test_dag_id_in_source` | `dbt_and_load` is the dag_id |
| `test_task_ids_in_source` | All three task IDs present in source |
| `test_catchup_false_in_source` | `catchup=False` is set |
| `test_daily_schedule_in_source` | Schedule is `@daily` |
| `test_dag_file_is_valid_python` | File parses without SyntaxError |
| `test_live_dag_id` | Live Airflow import check (skipped if not installed) |
| `test_live_dag_tasks` | Live task IDs match expected set |
| `test_live_dag_no_catchup` | Live `dag.catchup is False` |

### Integration smoke test

```bash
pytest tests/test_integration_smoke.py -v
```

Verifies:
1. Postgres is reachable and `raw_events` table exists
2. `stg_events` view has rows
3. `daily_event_metrics` table has rows

---

## 13. Step 8 — CI/CD Pipeline

```
┌────────────────────────────────────────────────────────────┐
│                      CI Flow (project2)                    │
│                                                            │
│  push/PR ──▶  lint (ruff + black)                         │
│                        │                                   │
│                        ▼                                   │
│          test-project2 (pytest test_dag.py)                │
│                        │                                   │
│                        ▼                                   │
│          build-images (docker build dbt-runner)            │
│                        │                                   │
│                        ▼                                   │
│          smoke-integration (Postgres live check)           │
└────────────────────────────────────────────────────────────┘
```

---

## 14. Configuration Reference

| Variable | Default | Used by |
|----------|---------|---------|
| `POSTGRES_HOST` | `postgres` | Airflow, dbt |
| `POSTGRES_PORT` | `5432` | Airflow, dbt |
| `POSTGRES_USER` | `deuser` | Airflow, dbt |
| `POSTGRES_PASSWORD` | `depassword` | Airflow, dbt |
| `POSTGRES_DB` | `dedb` | Airflow, dbt |
| `AIRFLOW__CORE__FERNET_KEY` | *(change me)* | Airflow encryption |
| `AIRFLOW__WEBSERVER__SECRET_KEY` | *(change me)* | Airflow web sessions |
| `AIRFLOW_ADMIN_USER` | `admin` | Airflow init |
| `AIRFLOW_ADMIN_PASSWORD` | `admin` | Airflow init |
| `SUPERSET_SECRET_KEY` | *(change me)* | Superset sessions |

---

## 15. Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| Airflow UI blank / 500 | DB not initialised | Run `--profile init up airflow-init` |
| DAG not visible in UI | File syntax error | Check `airflow dags list` in container |
| `dbt run` fails: relation not found | Source table missing | Insert test data into `raw_events` |
| Superset can't connect to Postgres | Wrong host (use `postgres`, not `localhost`) | Use docker service name `postgres` in URI |
| `dbt test` fails: not_null | Null `ts` in raw_events | Filter at source or backfill |

---

## 16. Extension Ideas

| Idea | Description |
|------|-------------|
| **dbt Cosmos** | Visualise each dbt model as a separate Airflow task |
| **Incremental models** | `{{ config(materialized='incremental') }}` for large tables |
| **CeleryExecutor** | Scale Airflow workers across multiple nodes |
| **dbt Docs** | `dbt docs generate && dbt docs serve` for a live lineage UI |
| **Great Expectations** | Add data quality checks at the source layer |
| **dbt Snapshots** | Track slowly changing dimensions (SCD Type 2) |
| **Redshift / BigQuery** | Swap `profiles.yml` target to move to cloud DWH |
