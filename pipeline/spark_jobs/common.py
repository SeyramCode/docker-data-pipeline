"""Shared helpers for the Spark jobs: session, config, connection options."""
import glob
import os
import re

import yaml
from pyspark.sql import SparkSession

LAKE_ROOT = os.environ.get("LAKE_ROOT", "/opt/lake")
TABLES_FILE = os.environ.get("PIPELINE_TABLES_FILE", "/opt/pipeline/config/tables.yml")
JARS_DIR = "/opt/spark-jars"
TABLE_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")


def env(name, default=None):
    value = os.environ.get(name, default)
    if value in (None, ""):
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def get_spark(app_name):
    jars = sorted(glob.glob(f"{JARS_DIR}/*.jar"))
    return (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.jars", ",".join(jars))
        .config("spark.driver.extraClassPath", ":".join(jars))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def load_tables():
    with open(TABLES_FILE) as f:
        tables = (yaml.safe_load(f) or {}).get("tables") or []
    if not tables:
        raise SystemExit(f"No tables configured in {TABLES_FILE}")
    for t in tables:
        if not TABLE_NAME_RE.match(t.get("name", "")):
            raise SystemExit(f"Invalid table name {t.get('name')!r}: use lowercase letters, digits, _")
    return tables


def as_list(value):
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def lake_path(layer, table_name):
    return f"{LAKE_ROOT}/{layer}/{table_name}"


def mysql_options():
    params = os.environ.get("MYSQL_JDBC_PARAMS", "")
    url = f"jdbc:mysql://{env('MYSQL_HOST')}:{env('MYSQL_PORT', '3306')}/{env('MYSQL_DATABASE')}"
    return {
        "url": f"{url}?{params}" if params else url,
        "user": env("MYSQL_USER"),
        "password": env("MYSQL_PASSWORD"),
        "driver": "com.mysql.cj.jdbc.Driver",
    }


def warehouse_options():
    url = (
        f"jdbc:postgresql://{env('WAREHOUSE_HOST')}:{env('WAREHOUSE_PORT', '5432')}"
        f"/{env('WAREHOUSE_DB')}?stringtype=unspecified"
    )
    return {
        "url": url,
        "user": env("POSTGRES_USER"),
        "password": env("POSTGRES_PASSWORD"),
        "driver": "org.postgresql.Driver",
    }
