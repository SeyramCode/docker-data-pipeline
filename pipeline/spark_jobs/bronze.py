"""MySQL -> Delta bronze (append-only raw extracts).

Each run is tagged with a batch id. Re-running the same batch replaces only that
batch's rows, so retries and manual re-runs never duplicate data.
"""
import argparse
import re

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from common import get_spark, lake_path, load_tables, mysql_options

# Delta rejects these characters in column names.
INVALID_COL_CHARS = re.compile(r"[ ,;{}()\n\t=]")


def last_watermark(spark, path, column, batch_id):
    if not column or not DeltaTable.isDeltaTable(spark, path):
        return None
    row = (
        spark.read.format("delta").load(path)
        .where(F.col("_batch_id") != batch_id)
        .agg(F.max(INVALID_COL_CHARS.sub("_", column)).alias("wm"))
        .first()
    )
    return row["wm"] if row else None


def source_query(table, watermark):
    cols = table.get("columns")
    select = ", ".join(f"`{c}`" for c in cols) if cols else "*"
    sql = f"SELECT {select} FROM `{table.get('source', table['name'])}`"
    if watermark is not None:
        # >= rather than > so rows sharing the boundary timestamp are not missed;
        # silver deduplicates the overlap.
        sql += f" WHERE `{table['incremental_column']}` >= '{watermark}'"
    return sql


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-id", required=True)
    args = parser.parse_args()
    batch_id = args.batch_id.replace("'", "")

    spark = get_spark("mysql_to_bronze")
    for table in load_tables():
        path = lake_path("bronze", table["name"])
        watermark = last_watermark(spark, path, table.get("incremental_column"), batch_id)
        sql = source_query(table, watermark)
        print(f"[bronze] {table['name']}: {sql}")

        df = (
            spark.read.format("jdbc")
            .options(**mysql_options())
            .option("query", sql)
            .option("fetchsize", "10000")
            .load()
        )
        df = df.toDF(*[INVALID_COL_CHARS.sub("_", c) for c in df.columns])
        df = df.withColumn("_ingested_at", F.current_timestamp()).withColumn("_batch_id", F.lit(batch_id))

        writer = df.write.format("delta").option("mergeSchema", "true")
        if DeltaTable.isDeltaTable(spark, path):
            writer.mode("overwrite").option("replaceWhere", f"_batch_id = '{batch_id}'").save(path)
        else:
            writer.mode("append").save(path)
        print(f"[bronze] {table['name']}: wrote {df.count()} rows")

    spark.stop()


if __name__ == "__main__":
    main()
