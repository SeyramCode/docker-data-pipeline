"""Delta silver -> Postgres `raw` schema (input for dbt).

Uses TRUNCATE + INSERT so dbt views that depend on these tables are not dropped.
If a source table's columns change, drop the raw table once (CASCADE) and re-run.
"""
from common import get_spark, lake_path, load_tables, warehouse_options


def main():
    spark = get_spark("silver_to_warehouse")
    for table in load_tables():
        df = spark.read.format("delta").load(lake_path("silver", table["name"]))
        (
            df.write.format("jdbc")
            .options(**warehouse_options())
            .option("dbtable", f"raw.{table['name']}")
            .option("truncate", "true")
            .option("batchsize", "10000")
            .mode("overwrite")
            .save()
        )
        print(f"[warehouse] raw.{table['name']}: loaded")
    spark.stop()


if __name__ == "__main__":
    main()
