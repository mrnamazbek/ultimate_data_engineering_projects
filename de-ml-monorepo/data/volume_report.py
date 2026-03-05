"""Print row counts for all pipeline tables – quick data-volume health check."""

import os

import psycopg2
import psycopg2.sql

# connect using env vars (same defaults as docker-compose)
conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=int(os.getenv("POSTGRES_PORT", "5432")),
    user=os.getenv("POSTGRES_USER", "deuser"),
    password=os.getenv("POSTGRES_PASSWORD", "depassword"),
    dbname=os.getenv("POSTGRES_DB", "dedb"),
)

# tables tracked across all three projects
TABLES = [
    "raw_events",  # project1 – ingested by Spark
    "event_agg",  # project1 – windowed aggregates
    "stg_events",  # project2 – dbt staging view
    "daily_event_metrics",  # project2 – dbt mart
]

print(f"{'Table':<25} {'Rows':>10}")
print("-" * 37)
with conn.cursor() as cur:
    for table in TABLES:
        # use Identifier to safely compose the table name
        query = psycopg2.sql.SQL("SELECT COUNT(*) FROM {}").format(
            psycopg2.sql.Identifier(table)
        )
        try:
            cur.execute(query)
            (count,) = cur.fetchone()
        except psycopg2.Error as exc:
            count = f"error: {exc.pgcode}"
            conn.rollback()
        print(f"{table:<25} {str(count):>10}")

conn.close()
