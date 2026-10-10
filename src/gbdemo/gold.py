"""Gold layer: disclosure-controlled aggregates for analysts and publication.

Gold holds protected values only, so every gold table is safe to share; exact counts stay in the
restricted silver layer. Aggregation runs on exact counts and protection is applied last
(summing already-rounded numbers would compound rounding error).
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from gbdemo.disclosure import is_suppressed, protect, protect_ratio
from gbdemo.postcodes import normalise_postcode


def residents_by_lsoa(census: DataFrame, postcodes: DataFrame, lad_codes: list[str]) -> DataFrame:
    """Census 2021 residents per LSOA (2021), located with one NHSPD release (Full: terminated too)."""
    pc = postcodes.select("postcode", "lsoa21_code", "lad_code")
    return (
        census.select(normalise_postcode(F.col("postcode")).alias("postcode"), F.col("residents"))
        .join(pc, "postcode")
        .where(F.col("lad_code").isin(lad_codes))
        .groupBy("lsoa21_code", "lad_code")
        .agg(F.sum("residents").alias("residents"))
    )


def registered_by_lsoa(registrations: DataFrame) -> DataFrame:
    return registrations.groupBy("lsoa21_code").agg(F.count("*").alias("registered"))


def lsoa_registered_vs_resident(
    residents: DataFrame, registered: DataFrame, lsoa_names: DataFrame
) -> DataFrame:
    """Registered patients vs Census residents per LSOA ("list inflation"), protected."""
    exact = residents.join(registered, "lsoa21_code", "full").fillna(0, ["residents", "registered"])
    return exact.join(lsoa_names, "lsoa21_code", "left").select(
        "lsoa21_code",
        "lsoa21_name",
        "lad_code",
        protect(F.col("residents")).alias("residents"),
        protect(F.col("registered")).alias("registered"),
        protect_ratio(F.col("registered"), F.col("residents")).alias("registered_per_resident"),
        (is_suppressed(F.col("residents")) | is_suppressed(F.col("registered"))).alias("any_suppressed"),
    )


def practice_profile(registrations: DataFrame, practices: DataFrame) -> DataFrame:
    """Registered patients per practice and 5-year age band, protected (NHS list-size style)."""
    by_band = registrations.groupBy("practice_code", "age_band").agg(F.count("*").alias("n"))
    totals = registrations.groupBy("practice_code").agg(F.count("*").alias("list_size"))
    names = practices.select("practice_code", "practice_name", "postcode", "lsoa21_code")
    return (
        by_band.join(totals, "practice_code")
        .join(names, "practice_code", "left")
        .select(
            "practice_code",
            "practice_name",
            "postcode",
            "lsoa21_code",
            "age_band",
            protect(F.col("n")).alias("registered"),
            is_suppressed(F.col("n")).alias("suppressed"),
            protect(F.col("list_size")).alias("list_size"),
        )
    )


def icb_summary(registrations: DataFrame, residents_icb: DataFrame, icb_names: DataFrame) -> DataFrame:
    """Registered patients vs residents per ICB (edition in force for the NHSPD release), protected."""
    reg = registrations.groupBy("icb_gss").agg(F.count("*").alias("registered"))
    exact = residents_icb.join(reg, "icb_gss", "full").fillna(0, ["residents", "registered"])
    return exact.join(icb_names, "icb_gss", "left").select(
        "icb_gss",
        "icb_name",
        protect(F.col("residents")).alias("residents"),
        protect(F.col("registered")).alias("registered"),
        protect_ratio(F.col("registered"), F.col("residents")).alias("registered_per_resident"),
    )
