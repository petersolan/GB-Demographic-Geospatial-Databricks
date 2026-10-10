"""Job step: standardise and enrich postcodes and GP practices (silver).

1. icb_lookup: ICB ODS code -> GSS code + name, versioned (SCD2) by edition
2. postcode:   typed NHSPD per release, ICB attached as of the release date, dq_flags
3. gp_practice (+ gp_practice_quarantine): ODS practices matched to the NHSPD release in force
Every step records metrics to silver.reconciliation and fails if row counts don't balance.
"""
from pyspark.sql import functions as F

from gbdemo import reconcile, silver
from gbdemo.bronze import write_version
from gbdemo.runtime import check_schema, get_spark, parse_args, parse_layout


def flag_counts(df) -> dict:
    """{'flag_<name>': rows} for every dq flag present."""
    rows = df.select(F.explode("dq_flags").alias("f")).groupBy("f").count().collect()
    return {f"flag_{r.f}": r["count"] for r in rows}


def main() -> None:
    args, layout = parse_args(), parse_layout()
    spark = get_spark()
    check_schema(spark, layout.catalog, layout.silver_schema)
    recon = layout.silver("reconciliation")

    def log(step: str, version: str, metrics: dict) -> None:
        reconcile.record(spark, recon, args.run_id, step, version, metrics)
        shown = ", ".join(f"{k}={v:,}" for k, v in sorted(metrics.items()))
        print(f"  {step} {version}: {shown}", flush=True)

    # 1. versioned ICB code lookup (small: rebuilt in full each run)
    lookup = silver.icb_lookup(spark.table(layout.bronze("icb_codes")))
    (lookup.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
     .saveAsTable(layout.silver("icb_lookup")))
    lookup = spark.table(layout.silver("icb_lookup"))
    log("icb_lookup", "all", {"rows_out": lookup.count(), "current_rows": lookup.where("is_current").count()})

    # 2. NHSPD, one release at a time (idempotent per release)
    nhspd = spark.table(layout.bronze("nhspd"))
    postcode_table = layout.silver("postcode")
    for (release,) in sorted(nhspd.select("_version").distinct().collect()):
        src = nhspd.where(F.col("_version") == release)
        out = (silver.flag_nhspd(silver.enrich_icb(silver.typed_nhspd(src), lookup))
               .withColumn("_run_id", F.lit(args.run_id)))
        write_version(out, postcode_table, release, column="nhspd_release")
        written = spark.table(postcode_table).where(F.col("nhspd_release") == release)
        rows_in, rows_out = src.count(), written.count()
        reconcile.assert_equal(f"postcode {release} rows", rows_in, rows_out)
        log("postcode", release, {"rows_in": rows_in, "rows_out": rows_out,
                                  "icb_matched": written.where("icb_gss IS NOT NULL").count(),
                                  **flag_counts(written)})

    # 3. GP practices: latest ODS snapshot, matched to the NHSPD release in force on that date
    ods = spark.table(layout.bronze("ods_epraccur"))
    snapshot = ods.agg(F.max("_version")).first()[0]
    practices = silver.typed_practices(ods.where(F.col("_version") == snapshot))
    matched = (silver.match_practices(practices, spark.table(postcode_table))
               .withColumn("_run_id", F.lit(args.run_id)))
    write_version(matched, layout.silver("gp_practice"), snapshot, column="ods_snapshot")
    written = spark.table(layout.silver("gp_practice")).where(F.col("ods_snapshot") == snapshot)
    rows_in, rows_out = practices.count(), written.count()
    reconcile.assert_equal(f"gp_practice {snapshot} rows (join fan-out check)", rows_in, rows_out)

    quarantine = written.where("is_blocked").select(
        "practice_code", "practice_name", "postcode_raw", "postcode", "status", "dq_flags",
        "ods_snapshot", "_run_id")
    write_version(quarantine, layout.silver("gp_practice_quarantine"), snapshot, column="ods_snapshot")
    log("gp_practice", snapshot, {
        "rows_in": rows_in, "rows_out": rows_out,
        "located": written.where("easting IS NOT NULL").count(),
        "quarantined": written.where("is_blocked").count(),
        **flag_counts(written),
    })
    print(f"[silver_postcodes] done, run {args.run_id}")


if __name__ == "__main__":
    main()
