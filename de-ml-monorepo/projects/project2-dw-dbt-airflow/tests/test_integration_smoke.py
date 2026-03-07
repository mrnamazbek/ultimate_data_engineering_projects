"""
Integration smoke test for dbt: run dbt and query the mart.

Requires:
  - Running Postgres with raw_events data (docker-compose)
  - dbt installed

Skipped when INTEGRATION=1 is not set.
"""

import os
import subprocess

import pytest
import psycopg2

INTEGRATION = os.getenv("INTEGRATION", "0") == "1"
pytestmark = pytest.mark.skipif(not INTEGRATION, reason="integration tests disabled")

DBT_DIR = os.path.join(os.path.dirname(__file__), "..", "dbt_project")


def test_dbt_run():
    """dbt run should exit 0."""
    result = subprocess.run(
        ["dbt", "run", "--project-dir", DBT_DIR, "--profiles-dir", DBT_DIR],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_mart_populated():
    """daily_event_metrics view should exist after dbt run."""
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "namazbek"),
        password=os.getenv("POSTGRES_PASSWORD", "bekzhanov"),
        dbname=os.getenv("POSTGRES_DB", "dedb"),
    )
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM daily_event_metrics;")
        (count,) = cur.fetchone()
    conn.close()
    # After dbt run the mart/view should be queryable (count may be 0 if no data)
    assert count >= 0
