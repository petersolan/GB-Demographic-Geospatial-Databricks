"""Job step: generate the synthetic GP registration list (bronze, as if received from GP systems).

Writes bronze.gp_registrations (raw, synthetic NHS numbers, protected by a column mask) and
bronze.synthetic_truth (the injected defects, used only to measure the pipeline).
"""

from pyspark.sql import functions as F

from gbdemo import reconcile, synthetic
from gbdemo.bronze import with_lineage
from gbdemo.runtime import check_schema, get_spark, parse_args, parse_layout


def protect_nhs_number(spark, table: str, owner: str) -> None:
    """Column mask: real (synthetic) NHS numbers only for the pipeline identity / PII readers."""
    fn = table.rsplit(".", 1)[0] + ".mask_nhs_number"
    spark.sql(f"""
        CREATE OR REPLACE FUNCTION {fn}(n STRING) RETURN
          CASE WHEN current_user() = '{owner}' OR is_account_group_member('gbdemo_pii_readers') THEN n
               ELSE concat('*******', right(n, 3)) END""")
    spark.sql(f"ALTER TABLE {table} ALTER COLUMN nhs_number SET MASK {fn}")


def main() -> None:
    args, layout = parse_args(), parse_layout()
    spark = get_spark()
    check_schema(spark, layout.catalog, layout.bronze_schema)
    log = reconcile.Recorder(spark, layout.silver("reconciliation"), args.run_id)
    lads = [x.strip() for x in args.synthetic_lads.split(",") if x.strip()]
    owner = spark.sql("SELECT current_user() AS u").first().u

    census = spark.table(layout.bronze("census_postcode_population")).select(
        "postcode", F.col("total").alias("residents")
    )
    practices_all = spark.table(layout.silver("gp_practice"))
    snapshot, snapshot_date = practices_all.agg(
        F.max_by("ods_snapshot", "snapshot_date"), F.max("snapshot_date")
    ).first()
    practices = practices_all.where(F.col("ods_snapshot") == snapshot)
    regs, truth = synthetic.generate(
        spark, census, spark.table(layout.silver("postcode")), practices, lads, str(snapshot_date)
    )

    raw, truth_table = layout.bronze("gp_registrations"), layout.bronze("synthetic_truth")
    if spark.catalog.tableExists(raw):  # a masked column can't be overwritten: drop, write, re-apply
        spark.sql(f"ALTER TABLE {raw} ALTER COLUMN nhs_number DROP MASK")
    out = with_lineage(
        regs.withColumn("file_path", F.lit("generated")),
        "synthetic_registrations",
        synthetic.SALT,
        args.run_id,
    )
    out.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(raw)
    truth.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(truth_table)
    protect_nhs_number(spark, raw, owner)

    t = spark.table(truth_table)
    log(
        "synthetic_registrations",
        synthetic.SALT,
        {
            "registrations": spark.table(raw).count(),
            "people": t.where("NOT is_duplicate").count(),
            "ghost_registrations": t.where("is_ghost AND NOT is_duplicate").count(),
            "duplicate_registrations": t.where("is_duplicate").count(),
            **reconcile.value_counts(t.where("defect IS NOT NULL"), "defect", "defect_"),
        },
    )
    print(f"[synthetic_registrations] LADs {lads}, reference date {snapshot_date}, run {args.run_id}")


if __name__ == "__main__":
    main()
