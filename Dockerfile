# One image for the whole pipeline: Airflow (orchestration), PySpark + Delta Lake
# (extract / lake), and dbt (warehouse transforms). Spark and dbt live in their own
# virtualenvs so their dependencies never fight with Airflow's.
#
# Multi-arch: every base layer used here is published for linux/amd64 and linux/arm64.
#   docker buildx build --platform linux/amd64,linux/arm64 -t <registry>/batch-pipeline:1.0 --push .

ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.11
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG SPARK_VERSION=3.5.3
ARG DELTA_VERSION=3.3.0
ARG MYSQL_JDBC_VERSION=8.4.0
ARG POSTGRES_JDBC_VERSION=42.7.4
ARG DBT_POSTGRES_VERSION=1.9.0

USER root

# Java for Spark. The JDK path contains the CPU arch, so expose a stable symlink.
RUN mkdir -p /usr/share/man/man1 \
 && apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl procps \
 && ln -s "/usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture)" /usr/lib/jvm/java-17 \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17

# Bake the JVM jars into the image so nothing is downloaded at runtime.
RUN mkdir -p /opt/spark-jars && cd /opt/spark-jars \
 && curl -fsSLO "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar" \
 && curl -fsSLO "https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar" \
 && curl -fsSLO "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/${MYSQL_JDBC_VERSION}/mysql-connector-j-${MYSQL_JDBC_VERSION}.jar" \
 && curl -fsSLO "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"

# Isolated Python environments for Spark jobs and dbt.
RUN python -m venv /opt/spark-venv \
 && /opt/spark-venv/bin/pip install --no-cache-dir \
      "pyspark==${SPARK_VERSION}" "delta-spark==${DELTA_VERSION}" "pyyaml==6.0.2" \
 && python -m venv /opt/dbt-venv \
 && /opt/dbt-venv/bin/pip install --no-cache-dir "dbt-postgres==${DBT_POSTGRES_VERSION}"

# Pipeline code is baked in, so the image runs on its own; compose can bind-mount
# ./pipeline over it for editing without rebuilds.
COPY --chown=airflow:root pipeline /opt/pipeline
# Pre-create the lake dir so a fresh named volume inherits airflow ownership.
RUN mkdir -p /opt/lake && chown -R airflow:root /opt/lake

ENV AIRFLOW__CORE__DAGS_FOLDER=/opt/pipeline/dags \
    AIRFLOW__CORE__LOAD_EXAMPLES=false \
    LAKE_ROOT=/opt/lake \
    PIPELINE_TABLES_FILE=/opt/pipeline/config/tables.yml \
    DBT_PROFILES_DIR=/opt/pipeline/dbt \
    DBT_TARGET_PATH=/tmp/dbt/target \
    DBT_LOG_PATH=/tmp/dbt/logs \
    DBT_SEND_ANONYMOUS_USAGE_STATS=false \
    PYTHONDONTWRITEBYTECODE=1

USER airflow
