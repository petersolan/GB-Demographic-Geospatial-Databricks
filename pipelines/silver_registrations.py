"""Job step: validate, quarantine, deduplicate and pseudonymise GP registrations (silver).

The pseudonymisation key comes from the Databricks secret scope gbdemo/pseudonym_key
(scripts/create_pseudonym_key.py). Silver holds patient_id, never the NHS number.
Results are measured against bronze.synthetic_truth (the defects we injected).

Counts are taken from the written tables, and the truth metrics in one aggregation: serverless has
no .cache(), so every count of a lazy DataFrame would recompute its joins and the HMAC UDF.
"""

from pyspark.sql import functions as F

from gbdemo import clustering, nhs_number, reconcile, registrations
from gbdemo.runtime import check_schema, get_spark, parse_args, parse_layout

SECRET_SCOPE, SECRET_KEY = "gbdemo", "pseudonym_key"
EXPECTED_BLOCKED = ("blank_postcode", "invalid_format", "unknown_postcode", "bad_check_digit")
PLAUSIBILITY = ("postcode_no_census_residents", "far_from_practice")


def truth_metrics(truth, silver_regs, quarantine, key: str) -> dict:
    """Validation measured against the injected defects, in one pass over the truth table.

    truth_expected_quarantine / truth_caught: injected blocking defects, and how many were quarantined
    typo_*: 'unknown_postcode' typos that formed a real postcode, so validation accepted them
    correct_*: people with no injected defect, and how many a plausibility rule flagged by mistake
    """
    # silver has one row per patient and the quarantine ids are made distinct: no join fan-out
    accepted = silver_regs.select("patient_id", "dq_flags", F.lit(True).alias("in_silver"))
    quarantined = quarantine.select("patient_id").distinct().withColumn("in_quarantine", F.lit(True))
    t = (
        truth.select(
            nhs_number.pseudonymise_col(F.col("nhs_number_recorded"), key).alias("patient_id"), "defect"
        )
        .join(accepted, "patient_id", "left")
        .join(quarantined, "patient_id", "left")
    )
    expected = F.col("defect").isin(*EXPECTED_BLOCKED)
    typo = (F.col("defect") == "unknown_postcode") & F.col("in_silver")
    correct = F.col("defect").isNull() & F.col("in_silver")
    plausible = F.arrays_overlap("dq_flags", F.array(*[F.lit(x) for x in PLAUSIBILITY]))

    def rows(cond):
        return F.count(F.when(cond, 1))

    def people(cond):  # a person registered twice has two truth rows: count them once
        return F.count_distinct(F.when(cond, F.col("patient_id")))

    metrics = {
        "truth_expected_quarantine": rows(expected),
        "truth_caught": rows(expected & F.col("in_quarantine")),
        "typo_accepted": rows(typo),
        "typo_flagged_plausibility": rows(typo & plausible),
        "correct_records": people(correct),
        "correct_records_flagged": people(correct & plausible),
        # per flag: typos caught vs correct records flagged (precision of each rule)
        **{f"typo_flagged_{f}": rows(typo & F.array_contains("dq_flags", f)) for f in PLAUSIBILITY},
        **{f"correct_flagged_{f}": people(correct & F.array_contains("dq_flags", f)) for f in PLAUSIBILITY},
    }
    row = t.agg(*[c.alias(name) for name, c in metrics.items()]).first()
    return row.asDict()


def main() -> None:
    args, layout = parse_args(), parse_layout()
    spark = get_spark()
    check_schema(spark, layout.catalog, layout.silver_schema)
    log = reconcile.Recorder(spark, layout.silver("reconciliation"), args.run_id)

    from databricks.sdk.runtime import dbutils  # only available on Databricks

    key = dbutils.secrets.get(SECRET_SCOPE, SECRET_KEY)

    postcodes = spark.table(layout.silver("postcode"))
    release = postcodes.agg(F.max("nhspd_release")).first()[0]
    practices_all = spark.table(layout.silver("gp_practice"))
    snapshot, snapshot_date = practices_all.agg(
        F.max_by("ods_snapshot", "snapshot_date"), F.max("snapshot_date")
    ).first()
    regs = spark.table(layout.bronze("gp_registrations"))
    clean, quarantine = registrations.clean(
        regs,
        postcodes.where(F.col("nhspd_release") == release),
        practices_all.where(F.col("ods_snapshot") == snapshot),
        key,
        str(snapshot_date),
        census=spark.table(layout.bronze("census_postcode_population")).select(
            "postcode", F.col("total").alias("residents")
        ),
    )

    for df, name in ((clean, "registrations"), (quarantine, "registration_quarantine")):
        (
            df.withColumn("nhspd_release", F.lit(release))
            .withColumn("_run_id", F.lit(args.run_id))
            .write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(layout.silver(name))
        )
    # analysts' extracts are by area: cluster so an area's rows sit in few files as the table grows
    clustering.cluster(spark, layout.silver("registrations"), ["lad_code", "lsoa21_code"])

    silver_regs = spark.table(layout.silver("registrations"))
    silver_quarantine = spark.table(layout.silver("registration_quarantine"))
    counts = registrations.counts(regs, silver_regs, silver_quarantine)
    reconcile.assert_equal(
        "registrations: rows in = clean + quarantined + duplicates removed",
        counts["rows_in"],
        counts["clean"] + counts["quarantined"] + counts["duplicates_removed"],
    )

    # measure against the injected defects (only possible because the data is synthetic)
    truth = spark.table(layout.bronze("synthetic_truth"))
    log(
        "silver_registrations",
        release,
        {
            **counts,
            **truth_metrics(truth, silver_regs, silver_quarantine, key),
            **reconcile.value_counts(silver_quarantine, "dq_flags", "q_"),
            **reconcile.value_counts(silver_regs, "dq_flags", "flag_"),
        },
    )
    print(f"[silver_registrations] NHSPD {release}, ODS {snapshot}, run {args.run_id}")


if __name__ == "__main__":
    main()
