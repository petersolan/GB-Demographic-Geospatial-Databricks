"""Job step: validate, quarantine, deduplicate and pseudonymise GP registrations (silver).

The pseudonymisation key comes from the Databricks secret scope gbdemo/pseudonym_key
(scripts/create_pseudonym_key.py). Silver holds patient_id, never the NHS number.
Results are measured against bronze.synthetic_truth (the defects we injected).
"""

from pyspark.sql import functions as F

from gbdemo import clustering, nhs_number, reconcile, registrations
from gbdemo.runtime import check_schema, get_spark, parse_args, parse_layout

SECRET_SCOPE, SECRET_KEY = "gbdemo", "pseudonym_key"
EXPECTED_BLOCKED = ("blank_postcode", "invalid_format", "unknown_postcode", "bad_check_digit")
PLAUSIBILITY = ("postcode_no_census_residents", "far_from_practice")


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
    clean, quarantine, counts = registrations.clean(
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
    reconcile.assert_equal(
        "registrations: rows in = clean + quarantined + duplicates removed",
        counts["rows_in"],
        counts["clean"] + counts["quarantined"] + counts["duplicates_removed"],
    )

    # measure against the injected defects (only possible because the data is synthetic)
    truth = spark.table(layout.bronze("synthetic_truth"))
    expected = truth.where(F.col("defect").isin(*EXPECTED_BLOCKED)).withColumn(
        "patient_id", nhs_number.pseudonymise_col(F.col("nhs_number_recorded"), key)
    )
    q = spark.table(layout.silver("registration_quarantine")).select("patient_id").distinct()
    caught = expected.join(q, "patient_id").count()

    # plausibility vs truth: typos that passed validation, and correct records flagged by mistake
    silver_regs = spark.table(layout.silver("registrations"))
    plausible_flag = F.arrays_overlap("dq_flags", F.array(*[F.lit(x) for x in PLAUSIBILITY]))
    truth_ids = truth.withColumn("patient_id", nhs_number.pseudonymise_col(F.col("nhs_number_recorded"), key))
    accepted_typos = truth_ids.where(F.col("defect") == "unknown_postcode").join(silver_regs, "patient_id")
    correct = (
        truth_ids.where(F.col("defect").isNull())
        .select("patient_id")
        .distinct()
        .join(silver_regs, "patient_id")
    )
    log(
        "silver_registrations",
        release,
        {
            **counts,
            "truth_expected_quarantine": expected.count(),
            "truth_caught": caught,
            "typo_accepted": accepted_typos.count(),
            "typo_flagged_plausibility": accepted_typos.where(plausible_flag).count(),
            "correct_records": correct.count(),
            "correct_records_flagged": correct.where(plausible_flag).count(),
            # per flag: typos caught vs correct records flagged (precision of each rule)
            **{
                f"typo_flagged_{f}": accepted_typos.where(F.array_contains("dq_flags", f)).count()
                for f in PLAUSIBILITY
            },
            **{
                f"correct_flagged_{f}": correct.where(F.array_contains("dq_flags", f)).count()
                for f in PLAUSIBILITY
            },
            **reconcile.value_counts(spark.table(layout.silver("registration_quarantine")), "dq_flags", "q_"),
            **reconcile.value_counts(spark.table(layout.silver("registrations")), "dq_flags", "flag_"),
        },
    )
    print(f"[silver_registrations] NHSPD {release}, ODS {snapshot}, run {args.run_id}")


if __name__ == "__main__":
    main()
