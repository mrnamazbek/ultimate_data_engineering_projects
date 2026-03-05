#!/usr/bin/env bash
# init-db.sh: initialize additional Postgres objects on first start.
# Runs automatically via docker-entrypoint-initdb.d.

set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  -- Airflow uses the same DB; nothing extra needed by default.

  -- Project 1: aggregation sink table
  CREATE TABLE IF NOT EXISTS event_agg (
      window_start TIMESTAMP NOT NULL,
      window_end   TIMESTAMP NOT NULL,
      event_count  BIGINT    NOT NULL,
      PRIMARY KEY (window_start, window_end)
  );

  -- Project 2: staging raw events (dbt source)
  CREATE TABLE IF NOT EXISTS raw_events (
      id      SERIAL PRIMARY KEY,
      ts      TIMESTAMP NOT NULL,
      value   TEXT,
      user_id INTEGER
  );
EOSQL
