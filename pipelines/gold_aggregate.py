"""Job step: disclosure-controlled aggregates (gold).

Writes gold.lsoa_registered_vs_resident, gold.practice_profile and gold.icb_summary.
Only protected values are written (small counts suppressed, others rounded to 5), so gold is
safe to share; exact counts stay in the restricted silver layer.
"""

from pyspark.sql import functions as F

from gbdemo import disclosure, gold, reconcile
from gbdemo.runtime import check_schema, get_spark, parse_args, parse_layout


def main() -> None:
    args, layout = parse_args(), parse_layout()
    spark = get_spark()
    check_schema(spark, layout.catalog, layout.gold_schema)
    log = reconcile.Recorder(spark, layout.silver("reconciliation"), args.run_id)
    lads = [x.strip() for x in args.synthetic_lads.split(",") if x.strip()]

    regs = spark.table(layout.silver("registrations"))
    release = regs.select("nhspd_release").first()[0]
    postcodes = spark.table(layout.silver("postcode")).where(F.col("nhspd_release") == release)
    census = spark.table(layout.bronze("census_postcode_population")).select(
        "postcode", F.col("total").alias("residents")
    )
    lsoa_names = spark.table(layout.silver("lsoa21_geo")).select(
        F.col("code").alias("lsoa21_code"), F.col("name").alias("lsoa21_name")
    )
    practices = spark.table(layout.silver("gp_practice"))
    snapshot = practices.agg(F.max("ods_snapshot")).first()[0]
    icb_names = (
        spark.table(layout.silver("icb_lookup"))
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
        return spark.table(layout.gold(name))

    residents = gold.residents_by_lsoa(census, postcodes, lads)
    lsoa = save(
        gold.lsoa_registered_vs_resident(residents, gold.registered_by_lsoa(regs), lsoa_names),
        "lsoa_registered_vs_resident",
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
            "practice_band_cells": practice.count(),
            "practice_band_cells_suppressed": practice.where("suppressed").count(),
            "icb_rows": icb.count(),
        },
    )
    print(
        f"[gold_aggregate] NHSPD {release}; disclosure: suppress 1-{disclosure.THRESHOLD - 1}, "
        f"round to {disclosure.BASE}; run {args.run_id}"
    )


if __name__ == "__main__":
    main()
