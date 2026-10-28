# Batch Pipeline in Docker

A batch data pipeline that plugs into the MySQL and Superset you already run.

Full documentation (PDF): [docs/Batch_Pipeline_Documentation.pdf](docs/Batch_Pipeline_Documentation.pdf)

```
 ┌──────────────┐                      THIS STACK                                ┌──────────────┐
 │ YOUR MySQL   │   ┌────────────────────────────────────────────────────────┐   │ YOUR Superset│
 │ (source)     │──▶│  Spark ──▶ Delta Lake ──▶ Postgres ──▶ dbt (marts)     │──▶│ (reads marts)│
 └──────────────┘   │  extract    bronze/silver   raw schema   gold tables   │   └──────────────┘
                    │  ▲                                                     │
                    │  └──────────── Airflow (schedules + retries) ──────────┘
                    └────────────────────────────────────────────────────────┘
```

| Layer  | Where                     | What                                         |
|--------|---------------------------|----------------------------------------------|
| Bronze | Delta Lake (`lake` volume) | Raw MySQL extracts, append-only, tagged by batch |
| Silver | Delta Lake (`lake` volume) | Trimmed, lower-cased, deduplicated by primary key |
| Raw    | Postgres `warehouse.raw`  | Silver copied in for dbt                     |
| Gold   | Postgres `warehouse.marts`| dbt models + tests, read by Superset         |

**Containers:** `postgres`, `airflow-init` (one-off), `airflow-webserver`, `airflow-scheduler`.
Spark runs in local mode inside the scheduler, and dbt runs there too. Everything ships in
a single custom image (`Dockerfile`) built for both amd64 and arm64.

## Requirements

- Docker Engine 20.10+ with Compose v2 (Linux), or Docker Desktop (macOS/Windows)
- About 6 GB of RAM available to Docker
- Network access from Docker to your MySQL

## Quick start

```bash
cp .env.example .env        # fill in MySQL credentials and passwords
# edit pipeline/config/tables.yml and the dbt models to match your tables
docker compose up -d --build
```

Open Airflow at http://localhost:8080 and log in with `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD`.
Then unpause the **batch_pipeline** DAG, or trigger it manually.

## Connecting your MySQL

1. Create a read-only user:
   ```sql
   CREATE USER 'pipeline_ro'@'%' IDENTIFIED BY 'change-me';
   GRANT SELECT ON shop.* TO 'pipeline_ro'@'%';
   ```
2. Set `MYSQL_HOST`:
   - MySQL on the same machine as Docker: `host.docker.internal`. On Linux, MySQL must
     listen on an address the containers can reach. If `bind-address = 127.0.0.1`,
     change it to `0.0.0.0` or the docker bridge IP, and firewall the port.
   - MySQL in another Docker container: put both on a shared network, or use its published port.
   - Remote MySQL: its hostname or IP.
3. MariaDB also works with the bundled MySQL driver.

## Connecting your Superset

Add a database in Superset with this SQLAlchemy URI:

```
postgresql://superset_ro:<SUPERSET_RO_PASSWORD>@<docker-host>:5433/warehouse
```

`<docker-host>` is `localhost` if Superset runs directly on the same host, or
`host.docker.internal` if Superset runs in its own container on that host.
The `superset_ro` user can only read the `marts` schema, and it automatically gets access to
new mart tables.

## Configuring tables

`pipeline/config/tables.yml` lists the tables to extract. The example has `customers` and `orders`:

```yaml
tables:
  - name: orders            # lake + warehouse name
    source: orders          # MySQL table (optional, defaults to name)
    primary_key: id         # dedupe key for silver
    incremental_column: updated_at   # optional: only pull new/changed rows
```

The dbt models in `pipeline/dbt/models` are examples written for those two tables. Replace
them with models for your own schema. `dbt build` runs every model and test, so the DAG
stops if a test fails.

The `./pipeline` folder is mounted into the containers, so changes to the config, DAGs,
Spark jobs or dbt models take effect on the next run without a rebuild.

### ERPNext / Frappe as the source

`tables.yml` includes a commented-out example for `tabSales Invoice`. Doctype tables use
`name` as the primary key and `modified` as the incremental column.

## How reruns behave

- **Bronze:** each run writes under the Airflow `run_id`. Re-running the same run replaces
  that batch only, so no duplicates are created. Incremental pulls start from the highest
  value loaded by earlier batches.
- **Silver and raw:** rebuilt from bronze each run, so they are idempotent.
- **Retries:** 2 per task, 5 minutes apart. `max_active_runs=1` stops runs from overlapping.

## Running pieces manually

```bash
docker compose exec airflow-scheduler bash -c \
  "cd /opt/pipeline/spark_jobs && /opt/spark-venv/bin/python silver.py"

docker compose exec airflow-scheduler \
  /opt/dbt-venv/bin/dbt build --project-dir /opt/pipeline/dbt --profiles-dir /opt/pipeline/dbt
```

## Shipping the image elsewhere

```bash
docker buildx build --platform linux/amd64,linux/arm64 \
  -t <registry>/batch-pipeline:1.0 --push .
```

On the target machine, copy `docker-compose.yml`, `.env`, `postgres/` and `pipeline/`. Then
set `PIPELINE_IMAGE=<registry>/batch-pipeline:1.0` and run `docker compose up -d --no-build`.
To run only from the code baked into the image, remove the `./pipeline` mount from the compose file.

## Resetting

```bash
docker compose down        # stop, keep data
docker compose down -v     # stop and DELETE lake, warehouse and Airflow metadata
```

The Postgres init script (`postgres/init`) runs only on the first start with an empty volume.

## Known limits

- Hard deletes in MySQL are not detected. Use soft-delete flags or CDC (Debezium) if you need them.
- If a source table's columns change, run `DROP TABLE raw.<name> CASCADE;` once and re-run. Then `dbt build` recreates the dependent views.
- Spark runs single-node. That's fine up to tens of millions of rows. Beyond that, move Spark to a cluster.
