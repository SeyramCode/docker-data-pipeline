"""Delta bronze -> Delta silver (cleaned, typed, deduplicated).

Silver is rebuilt from bronze on every run, so it is always consistent with bronze
and safe to re-run.
"""
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from common import as_list, get_spark, lake_path, load_tables

META_COLS = {"_ingested_at", "_batch_id"}


def clean(df, table):
    df = df.toDF(*[c.lower() for c in df.columns])
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType) and field.name not in META_COLS:
            df = df.withColumn(field.name, F.trim(F.col(field.name)))

    pk = [c.lower() for c in as_list(table.get("primary_key"))]
    if pk:
        order = []
        if table.get("incremental_column"):
            order.append(F.col(table["incremental_column"].lower()).desc_nulls_last())
        order.append(F.col("_ingested_at").desc())
        latest = Window.partitionBy(*pk).orderBy(*order)
        df = df.withColumn("_rn", F.row_number().over(latest)).where("_rn = 1").drop("_rn")
        for col in pk:
            df = df.where(F.col(col).isNotNull())
    else:
        df = df.dropDuplicates([c for c in df.columns if c not in META_COLS])

    return df.drop("_batch_id")


def main():
    spark = get_spark("bronze_to_silver")
    for table in load_tables():
        bronze = spark.read.format("delta").load(lake_path("bronze", table["name"]))
        silver = clean(bronze, table)
        (
            silver.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(lake_path("silver", table["name"]))
        )
        print(f"[silver] {table['name']}: {silver.count()} rows")
    spark.stop()


if __name__ == "__main__":
    main()
