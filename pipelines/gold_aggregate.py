"""Job step: disclosure-controlled aggregates (gold).

Writes gold.lsoa_registered_vs_resident, gold.msoa_registered_vs_resident, gold.practice_profile
and gold.icb_summary, plus gold.lsoa_calibration and gold.practice_calibration (the synthetic list
against NHS England's published registrations, gbdemo.calibration). Only protected values are
written (small counts suppressed, others rounded to 5), so gold is safe to share; exact counts stay
in the restricted silver layer.

Reproducible: every input is read pinned to its current Delta version, and the input and output
versions are recorded with the run's metrics (gbdemo.lineage). Each table gets its catalog
comments from gbdemo.catalog_docs, the same text as docs/data_dictionary.md.
"""

from pyspark.sql import functions as F

from gbdemo import calibration, catalog_docs, disclosure, gold, lineage, reconcile
from gbdemo.runtime import check_schema, get_spark, parse_args, parse_layout


def main() -> None:
    args, layout = parse_args(), parse_layout()
    spark = get_spark()
    check_schema(spark, layout.catalog, layout.gold_schema)
    log = reconcile.Recorder(spark, layout.silver("reconciliation"), args.run_id)
    lads = [x.strip() for x in args.synthetic_lads.split(",") if x.strip()]

    pinned = lineage.PinnedReader(spark)
    regs = pinned.read(layout.silver("registrations"), "registrations")
    release = regs.select("nhspd_release").first()[0]
    postcodes = pinned.read(layout.silver("postcode"), "postcode").where(F.col("nhspd_release") == release)
    census = pinned.read(layout.bronze("census_postcode_population"), "census_postcode_population").select(
        "postcode", F.col("total").alias("residents")
    )
    lsoa_names = pinned.read(layout.silver("lsoa21_geo"), "lsoa21_geo").select(
        F.col("code").alias("lsoa21_code"), F.col("name").alias("lsoa21_name")
    )
    msoa_names = (
        pinned.read(layout.bronze("msoa21_boundary"), "msoa21_boundary")
        .select(F.col("msoa_cd").alias("msoa21_code"), F.col("msoa_nm").alias("msoa21_name"))
        .distinct()
    )
    practices = pinned.read(layout.silver("gp_practice"), "gp_practice")
    snapshot = practices.agg(F.max("ods_snapshot")).first()[0]
    icb_names = (
        pinned.read(layout.silver("icb_lookup"), "icb_lookup")
        .where("is_current")
        .select(F.col("icb_gss"), F.col("icb_name"))
        .distinct()
    )

    def save(df, name):
        (
            df.withColumn("nhspd_release", F.lit(release))
            .withColumn("_run_id", F.lit(args.run_id))
            .write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(layout.gold(name))
        )
        catalog_docs.apply(spark, layout.gold(name), catalog_docs.GOLD[name])
        version = pinned.wrote(layout.gold(name), name)
        return spark.sql(f"SELECT * FROM {layout.gold(name)} VERSION AS OF {version}")

    residents = gold.residents_by_lsoa(census, postcodes, lads)
    lsoa = save(
        gold.lsoa_registered_vs_resident(residents, gold.registered_by_lsoa(regs), lsoa_names),
        "lsoa_registered_vs_resident",
    )
    msoa = save(
        gold.msoa_registered_vs_resident(
            gold.residents_by_area(census, postcodes, lads, "msoa21_code"),
            gold.registered_by_area(regs, "msoa21_code"),
            msoa_names,
        ),
        "msoa_registered_vs_resident",
    )
    practice = save(
        gold.practice_profile(regs, practices.where(F.col("ods_snapshot") == snapshot)), "practice_profile"
    )
    residents_icb = (
        census.join(postcodes.select("postcode", "lad_code", "icb_gss"), "postcode")
        .where(F.col("lad_code").isin(lads))
        .groupBy("icb_gss")
        .agg(F.sum("residents").alias("residents"))
    )
    icb = save(gold.icb_summary(regs, residents_icb, icb_names), "icb_summary")

    # calibration against real NHS registrations (latest extract), over the same study LSOAs
    nhs_raw = pinned.read(layout.bronze("gp_registered_lsoa"), "gp_registered_lsoa")
    extract = nhs_raw.agg(F.max("_version")).first()[0]
    nhs_raw = nhs_raw.where(F.col("_version") == extract)
    nhs_patients = calibration.nhs_lsoa_patients(nhs_raw)
    study = residents.select("lsoa21_code").distinct()
    lsoa_cal = save(
        calibration.lsoa_calibration(
            residents,
            gold.registered_by_lsoa(regs),
            calibration.nhs_registered_by_lsoa(nhs_patients),
            calibration.postcodes_since_census(postcodes),
            lsoa_names,
        ).withColumn("nhs_extract", F.lit(extract)),
        "lsoa_calibration",
    )
    practice_cal = save(
        calibration.practice_calibration(
            regs, nhs_patients, study, practices.where(F.col("ods_snapshot") == snapshot)
        ).withColumn("nhs_extract", F.lit(extract)),
        "practice_calibration",
    )
    nhs_total = nhs_raw.agg(F.sum(F.col("number_of_patients").cast("long"))).first()[0]
    log(
        "gold_calibration",
        extract,
        {
            "nhs_patients_total": nhs_total,
            "nhs_patients_without_lsoa": nhs_total - nhs_patients.agg(F.sum("patients")).first()[0],
            "nhs_registered_study_area": (
                nhs_patients.join(study, "lsoa21_code").agg(F.sum("patients")).first()[0]
            ),
            "synthetic_registered_study_area": regs.join(study, "lsoa21_code").count(),
            # share of real study-area patients in an (LSOA, practice) pair the model also uses
            "overlap_share_bp": int(calibration.overlap_share(regs, nhs_patients, study) * 10_000),
            "lsoa_rows": lsoa_cal.count(),
            "practice_rows": practice_cal.count(),
        },
    )

    in_scope = regs.where(F.col("lad_code").isin(lads)).count()
    total = regs.count()
    log(
        "gold_aggregate",
        release,
        {
            "registrations": total,
            "registrations_outside_scope_lads": total - in_scope,  # mostly undetectable postcode typos
            "registrations_without_lsoa": regs.where("lsoa21_code IS NULL").count(),
            "census_residents_in_scope": residents.agg(F.sum("residents")).first()[0],
            "lsoa_rows": lsoa.count(),
            "lsoa_rows_with_suppression": lsoa.where("any_suppressed").count(),
            "msoa_rows": msoa.count(),
            "msoa_rows_with_suppression": msoa.where("any_suppressed").count(),
            "practice_band_cells": practice.count(),
            "practice_band_cells_suppressed": practice.where("suppressed").count(),
            "icb_rows": icb.count(),
            **pinned.versions(),  # input_version.<table>, output_version.<table>
        },
    )
    print(
        f"[gold_aggregate] NHSPD {release}; disclosure: suppress 1-{disclosure.THRESHOLD - 1}, "
        f"round to {disclosure.BASE}; run {args.run_id}"
    )


if __name__ == "__main__":
    main()
