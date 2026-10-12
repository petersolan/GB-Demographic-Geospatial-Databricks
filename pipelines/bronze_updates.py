"""Job step: ingest new registration update files into bronze with Auto Loader (incremental).

Auto Loader (cloudFiles) lists the landing folder and ingests only files its checkpoint hasn't
seen, so each file is loaded exactly once, however often the job runs. availableNow processes
the backlog and stops, which suits a scheduled batch job. The schema is fixed: a file with
other columns fails instead of silently changing the table.

No column mask on this table: Delta streaming reads aren't supported on masked tables, and
silver reads it as a stream. Bronze is restricted by schema grants instead (pipeline only).
"""

from pyspark.sql import functions as F

from gbdemo import incremental, reconcile
from gbdemo.runtime import check_schema, get_spark, parse_args, parse_layout


def main() -> None:
    args, layout = parse_args(), parse_layout()
    spark = get_spark()
    check_schema(spark, layout.catalog, layout.bronze_schema)
    table = layout.bronze("registration_updates")
    before = spark.table(table).count() if spark.catalog.tableExists(table) else 0

    query = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", True)
        .schema(incremental.EVENT_SCHEMA)
        .load(f"{layout.landing_path}/registration_updates")
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
            F.current_timestamp().alias("_ingested_at"),
            F.lit(args.run_id).alias("_run_id"),
        )
        .writeStream.option("checkpointLocation", layout.checkpoint_path("bronze_registration_updates"))
        .trigger(availableNow=True)
        .toTable(table)
    )
    query.awaitTermination()

    after = spark.table(table)
    reconcile.Recorder(spark, layout.silver("reconciliation"), args.run_id)(
        "bronze_updates",
        args.month or "all",
        {
            "rows_before": before,
            "rows_ingested": after.count() - before,
            "rows_after": after.count(),
            "files_total": after.select("_source_file").distinct().count(),
        },
    )
    print(f"[bronze_updates] {table}: {after.count() - before:,} new rows")


if __name__ == "__main__":
    main()
