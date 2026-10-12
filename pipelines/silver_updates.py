"""Job step: apply new registration updates to silver.patient_history (SCD2) with MERGE.

Reads bronze.registration_updates as a stream, so each run sees only rows added since the last
(the checkpoint records the Delta version reached). Each micro-batch is validated, deduplicated
and pseudonymised, then applied with one MERGE (gbdemo.incremental). Batches reconcile:
events in = duplicates + quarantined + already applied + no-ops + applied, or the run stops.
After the stream: history invariants (one current row per patient, no gaps or overlaps) and
point-in-time counts at each month end.
"""

from pyspark.sql import functions as F

from gbdemo import incremental, reconcile
from gbdemo.runtime import check_schema, get_spark, parse_args, parse_layout

SECRET_SCOPE, SECRET_KEY = "gbdemo", "pseudonym_key"
GP_PRACTICE_SETTING = "RO76"


def make_handler(layout, key: str, run_id: str, release: str, snapshot: str):
    """foreachBatch function: runs once per micro-batch with that batch's new bronze rows."""
    history = layout.silver("patient_history")

    def handle(batch, batch_id: int) -> None:
        spark = batch.sparkSession  # the batch's own session (Spark Connect runs this server side)
        if batch.isEmpty():
            return
        postcodes = spark.table(layout.silver("postcode")).where(F.col("nhspd_release") == release)
        practices = spark.table(layout.silver("gp_practice")).where(
            (F.col("ods_snapshot") == snapshot)
            & (F.col("status") == "ACTIVE")
            & (F.col("prescribing_setting") == GP_PRACTICE_SETTING)
        )
        staged, q1, c1 = incremental.stage(batch, postcodes, practices, key)
        source, q2, c2 = incremental.plan(spark.table(history), staged)
        counts = {**c1, **c2}
        reconcile.assert_equal(
            "updates: events in = duplicates + quarantined + already applied + no-ops + applied",
            counts["events_in"],
            counts["duplicates_removed"]
            + counts["quarantined_validation"]
            + counts["already_applied"]
            + counts["quarantined_apply"]
            + counts["no_change"]
            + counts["applied"],
        )
        incremental.merge(spark, history, source)
        (
            q1.unionByName(q2)
            .withColumn("batch_id", F.lit(batch_id))
            .withColumn("_run_id", F.lit(run_id))
            .withColumn("quarantined_at", F.current_timestamp())
            .write.format("delta")
            .mode("append")
            .saveAsTable(layout.silver("registration_update_quarantine"))
        )
        reconcile.record(
            spark, layout.silver("reconciliation"), run_id, "silver_updates", f"batch-{batch_id}", counts
        )

    return handle


def main() -> None:
    args, layout = parse_args(), parse_layout()
    spark = get_spark()
    check_schema(spark, layout.catalog, layout.silver_schema)
    log = reconcile.Recorder(spark, layout.silver("reconciliation"), args.run_id)

    from databricks.sdk.runtime import dbutils  # only available on Databricks

    key = dbutils.secrets.get(SECRET_SCOPE, SECRET_KEY)
    history = layout.silver("patient_history")
    if not spark.catalog.tableExists(history):  # first run: start from the clean registrations
        incremental.seed_history(spark.table(layout.silver("registrations"))).write.format(
            "delta"
        ).saveAsTable(history)
        # change data feed: downstream tables can read only the rows each MERGE changed
        spark.sql(f"ALTER TABLE {history} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
        log("silver_updates", "seed", {"seeded_patients": spark.table(history).count()})

    release = spark.table(layout.silver("postcode")).agg(F.max("nhspd_release")).first()[0]
    snapshot = (
        spark.table(layout.silver("gp_practice")).agg(F.max_by("ods_snapshot", "snapshot_date")).first()[0]
    )
    query = (
        spark.readStream.table(layout.bronze("registration_updates"))
        .writeStream.foreachBatch(make_handler(layout, key, args.run_id, release, snapshot))
        .option("checkpointLocation", layout.checkpoint_path("silver_patient_history"))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()

    h = spark.table(history)
    check = incremental.check_history(h)
    reconcile.assert_equal(
        "history: patients without exactly one current row", 0, check["patients_without_one_current_row"]
    )
    reconcile.assert_equal("history: version chain gaps or overlaps", 0, check["broken_chain_rows"])
    months = sorted(
        r.extract_month
        for r in spark.table(layout.bronze("registration_updates"))
        .select("extract_month")
        .distinct()
        .collect()
        if r.extract_month
    )
    as_of = {}
    for month in months:  # registered patients at the end of each month, from the history
        end = spark.sql(f"SELECT last_day(to_date('{month}-01')) AS d").first().d.isoformat()
        as_of[f"registered_as_of_{end.replace('-', '_')}"] = incremental.registered_as_of(h, end).count()
    log("silver_updates", args.month or "all", {**check, **as_of})
    print(f"[silver_updates] {history}: {check}")


if __name__ == "__main__":
    main()
