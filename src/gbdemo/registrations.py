"""Silver GP registrations: validate, quarantine, deduplicate, pseudonymise, minimise.

Job ad: "Apply access, pseudonymisation and disclosure controls to sensitive data";
"Automate data quality checks and reconcile counts before and after joins".

- NHS numbers are validated (Modulus 11), then replaced by patient_id = HMAC-SHA256(key, number);
  the key lives in a Databricks secret scope and is passed in, never stored in code.
- Records that can't be used safely are quarantined with every reason that applies.
- Duplicate registrations of one person keep the most recent registration.
- Data minimisation: silver keeps year of birth and a 5-year age band, not the date of birth.
- Reconciliation: rows in = clean + quarantined + duplicates removed (asserted by the job).
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from gbdemo import nhs_number
from gbdemo.postcodes import is_valid_postcode, normalise_postcode
from gbdemo.silver import flags

BLOCKING = (
    "invalid_nhs_number",
    "blank_postcode",
    "invalid_postcode_format",
    "postcode_not_in_nhspd",
    "postcode_no_location",
)


def age_band(age) -> F.Column:
    """5-year bands '00-04' ... '85-89', '90+'."""
    lo = F.least(F.floor(age / 5) * 5, F.lit(90))
    return F.when(lo >= 90, F.lit("90+")).otherwise(
        F.concat(F.lpad(lo.cast("string"), 2, "0"), F.lit("-"), F.lpad((lo + 4).cast("string"), 2, "0"))
    )


def clean(
    registrations: DataFrame, postcodes: DataFrame, practices: DataFrame, key: str, reference_date: str
) -> tuple[DataFrame, DataFrame, dict]:
    """Return (clean registrations, quarantine, counts) from raw registrations.

    postcodes: one NHSPD release (silver.postcode rows); practices: silver.gp_practice rows.
    """
    pc = postcodes.select(
        "postcode", "is_live", "easting", "lsoa21_code", "msoa21_code", "icb_gss", "lad_code", "grid_quality"
    )
    known = practices.select("practice_code", F.lit(True).alias("practice_known"))
    ref = F.lit(reference_date).cast("date")

    r = (
        registrations.withColumn("postcode_norm", normalise_postcode(F.col("postcode")))
        .withColumn("nhs_ok", nhs_number.is_valid_col(F.col("nhs_number")))
        .withColumn("patient_id", nhs_number.pseudonymise_col(F.col("nhs_number"), key))
        .join(pc.withColumnRenamed("postcode", "postcode_norm"), "postcode_norm", "left")
        .join(F.broadcast(known), "practice_code", "left")
    )

    blank = F.coalesce(F.trim(F.col("postcode")), F.lit("")) == ""
    in_nhspd = F.col("is_live").isNotNull()
    r = r.withColumn(
        "dq_flags",
        flags(
            (~F.col("nhs_ok"), "invalid_nhs_number"),
            (blank, "blank_postcode"),
            (~blank & ~is_valid_postcode(F.col("postcode_norm")), "invalid_postcode_format"),
            (is_valid_postcode(F.col("postcode_norm")) & ~in_nhspd, "postcode_not_in_nhspd"),
            (in_nhspd & (F.col("easting").isNull() | (F.col("grid_quality") == 9)), "postcode_no_location"),
            (in_nhspd & ~F.coalesce(F.col("is_live"), F.lit(True)), "postcode_terminated"),
            (F.col("practice_known").isNull(), "unknown_practice"),
            (~blank & (F.col("postcode_norm") != F.trim(F.col("postcode"))), "postcode_reformatted"),
        ),
    ).withColumn("is_blocked", F.arrays_overlap("dq_flags", F.array(*[F.lit(x) for x in BLOCKING])))

    quarantine = r.where("is_blocked").select(
        "patient_id",
        F.col("postcode").alias("postcode_recorded"),
        "practice_code",
        "registration_date",
        "dq_flags",
        F.current_timestamp().alias("quarantined_at"),
    )

    # duplicates: same person registered more than once -> keep the most recent registration
    ok = r.where("NOT is_blocked")
    w = Window.partitionBy("patient_id").orderBy(F.col("registration_date").desc(), F.col("practice_code"))
    ranked = ok.withColumn("_n", F.row_number().over(w)).withColumn(
        "_count", F.count("*").over(Window.partitionBy("patient_id"))
    )
    age = F.floor(F.months_between(ref, F.col("date_of_birth")) / 12).cast("int")
    clean_df = ranked.where("_n = 1").select(
        "patient_id",
        F.year("date_of_birth").alias("year_of_birth"),  # minimisation: no full date of birth
        age_band(age).alias("age_band"),
        "sex",
        F.col("postcode_norm").alias("postcode"),
        "lsoa21_code",
        "msoa21_code",
        "icb_gss",
        "lad_code",
        "practice_code",
        "registration_date",
        (F.col("_count") - 1).alias("duplicates_removed"),
        "dq_flags",
    )
    # each count comes from its own data path; deriving one as "the rest" would make the
    # balance check (rows_in = clean + quarantined + duplicates_removed) true by construction
    counts = {
        "rows_in": registrations.count(),
        "quarantined": quarantine.count(),
        "clean": clean_df.count(),
        "duplicates_removed": clean_df.agg(F.sum("duplicates_removed")).first()[0] or 0,
    }
    return clean_df, quarantine, counts
