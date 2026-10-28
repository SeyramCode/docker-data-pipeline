#!/bin/bash
# Runs once, on first start with an empty data volume.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<EOSQL
CREATE DATABASE airflow;
CREATE DATABASE warehouse;
REVOKE ALL ON DATABASE airflow FROM PUBLIC;
CREATE ROLE superset_ro LOGIN PASSWORD '${SUPERSET_RO_PASSWORD}';
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname warehouse <<EOSQL
CREATE SCHEMA IF NOT EXISTS raw;      -- silver tables loaded by Spark
CREATE SCHEMA IF NOT EXISTS staging;  -- dbt staging views
CREATE SCHEMA IF NOT EXISTS marts;    -- dbt gold tables, read by Superset

GRANT CONNECT ON DATABASE warehouse TO superset_ro;
GRANT USAGE ON SCHEMA marts TO superset_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA marts TO superset_ro;
ALTER DEFAULT PRIVILEGES FOR ROLE "${POSTGRES_USER}" IN SCHEMA marts
    GRANT SELECT ON TABLES TO superset_ro;
EOSQL
