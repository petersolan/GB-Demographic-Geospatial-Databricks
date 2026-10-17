"""Silver GP registrations: validate, quarantine, deduplicate, pseudonymise, minimise.

Job ad: "Apply access, pseudonymisation and disclosure controls to sensitive data";
"Automate data quality checks and reconcile counts before and after joins".

- NHS numbers are validated (Modulus 11), then replaced by patient_id = HMAC-SHA256(key, number);
  the key lives in a Databricks secret scope and is passed in, never stored in code.
- Records that can't be used safely are quarantined with every reason that applies.
- Duplicate registrations of one person keep the most recent registration.
- Data minimisation: silver keeps year of birth and a 5-year age band, not the date of birth.
- Reconciliation: rows in = clean + quarantined + duplicates removed (asserted by the job).

Plausibility (informational, never blocking): a typo can turn one real postcode into another real
one, which format and NHSPD checks can't see. Two checks are kept, measured against the synthetic
ground truth: a postcode where Census 2021 found nobody (only if it existed before Census day, as
new-builds would legitimately have no residents) and a home far from the practice.
A "large user" (business) postcode check was tried and removed: it flagged 2,548 correct records
for 79 typos (3% precision), mostly real residents of care homes, hospitals and student halls,
i.e. biased against a vulnerable group.
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
CENSUS_DAY = "2021-03-21"
FAR_FROM_PRACTICE_M = 15_000


def age_band(age) -> F.Column:
    """5-year bands '00-04' ... '85-89', '90+'."""
    lo = F.least(F.floor(age / 5) * 5, F.lit(90))
    return F.when(lo >= 90, F.lit("90+")).otherwise(
        F.concat(F.lpad(lo.cast("string"), 2, "0"), F.lit("-"), F.lpad((lo + 4).cast("string"), 2, "0"))
    )


def plausibility(r: DataFrame, in_nhspd, with_census: bool, with_xy: bool) -> list:
    """(condition, flag) pairs for the plausibility checks whose inputs are available."""
    checks = []
    if with_census and "date_introduced" in r.columns:
        existed = F.col("date_introduced") <= F.lit(CENSUS_DAY).cast("date")
        checks.append(
            (in_nhspd & existed & F.col("has_census_residents").isNull(), "postcode_no_census_residents")
        )
    if with_xy:
        dist = F.sqrt((F.col("easting") - F.col("pr_e")) ** 2 + (F.col("northing") - F.col("pr_n")) ** 2)
        checks.append((F.coalesce(dist > FAR_FROM_PRACTICE_M, F.lit(False)), "far_from_practice"))
    return checks


def clean(
    registrations: DataFrame,
    postcodes: DataFrame,
    practices: DataFrame,
    key: str,
    reference_date: str,
    census: DataFrame | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Return (clean registrations, quarantine) from raw registrations, both lazy.

    postcodes: one NHSPD release (silver.postcode rows); practices: silver.gp_practice rows;
    census (optional): postcode, residents. Plausibility checks run only for the inputs supplied.
    Count the results with counts() after writing them: counting these DataFrames directly
    recomputes every join and the HMAC for each count (serverless has no .cache()).
    """
    optional = [c for c in ("user_type", "northing", "date_introduced") if c in postcodes.columns]
    pc = postcodes.select(
        "postcode",
        "is_live",
        "easting",
        "lsoa21_code",
        "msoa21_code",
        "icb_gss",
        "lad_code",
        "grid_quality",
        *optional,
    )
    has_xy = {"easting", "northing"} <= set(practices.columns) and "northing" in optional
    known = practices.select(
        "practice_code",
        F.lit(True).alias("practice_known"),
        *([F.col("easting").alias("pr_e"), F.col("northing").alias("pr_n")] if has_xy else []),
    )
    ref = F.lit(reference_date).cast("date")

    r = (
        registrations.withColumn("postcode_norm", normalise_postcode(F.col("postcode")))
        .withColumn("nhs_ok", nhs_number.is_valid_col(F.col("nhs_number")))
        .withColumn("patient_id", nhs_number.pseudonymise_col(F.col("nhs_number"), key))
        .join(pc.withColumnRenamed("postcode", "postcode_norm"), "postcode_norm", "left")
        .join(F.broadcast(known), "practice_code", "left")
    )
    if census is not None:
        residential = (
            census.select(normalise_postcode(F.col("postcode")).alias("postcode_norm"), "residents")
            .where("residents > 0")
            .select("postcode_norm", F.lit(True).alias("has_census_residents"))
        )
        r = r.join(residential, "postcode_norm", "left")

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
            *plausibility(r, in_nhspd, census is not None, has_xy),
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
    return clean_df, quarantine


def counts(registrations: DataFrame, clean_df: DataFrame, quarantine: DataFrame) -> dict:
    """Rows in, clean, quarantined and duplicates removed, for the balance check.

    Each count comes from its own data path; deriving one as "the rest" would make the
    balance check (rows_in = clean + quarantined + duplicates_removed) true by construction.
    """
    clean_rows, duplicates = clean_df.agg(F.count(F.lit(1)), F.sum("duplicates_removed")).first()
    return {
        "rows_in": registrations.count(),
        "quarantined": quarantine.count(),
        "clean": clean_rows,
        "duplicates_removed": duplicates or 0,
    }
