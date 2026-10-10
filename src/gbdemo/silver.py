"""Silver layer: typed, standardised, versioned and enriched reference data.

Spark 4 (and Databricks serverless) run in ANSI mode: CAST of a bad string raises an error.
Dirty inputs are therefore converted with try_cast / try_to_timestamp (null on failure) and the
failures are flagged and counted, so one bad value neither crashes the job nor vanishes silently.

Reference data (NHSPD) is never quarantined: it is the dictionary other records are checked
against, so every row stays and problems are listed in dq_flags. Records checked against it
(GP practices here, synthetic patients later) go to quarantine when unusable.
"""
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from gbdemo.postcodes import is_valid_postcode, normalise_postcode
from gbdemo.versioning import as_of, edition_start, scd2_from_editions

ENGLAND = "E92000001"


def _blank_to_null(c: Column) -> Column:
    return F.when(F.trim(c) != "", F.trim(c))


def try_int(c: Column) -> Column:
    return _blank_to_null(c).try_cast("int")


def try_date(c: Column, fmt: str) -> Column:
    """Parse a date string; null on failure (ANSI-safe). 'yyyyMM' values get day 01."""
    s = _blank_to_null(c)
    if fmt == "yyyyMM":
        s, fmt = F.concat(s, F.lit("01")), "yyyyMMdd"
    return F.to_date(F.try_to_timestamp(s, F.lit(fmt)))


def _failed(raw: Column, typed: Column) -> Column:
    """A value was present but could not be converted."""
    return _blank_to_null(raw).isNotNull() & typed.isNull()


def flags(*pairs: tuple[Column, str]) -> Column:
    """Array of flag names whose condition is true, e.g. ['no_grid_reference']."""
    return F.filter(F.array(*[F.when(cond, F.lit(name)) for cond, name in pairs]), lambda x: x.isNotNull())


# ---------------------------------------------------------------- NHSPD

def typed_nhspd(bronze: DataFrame) -> DataFrame:
    """Bronze NHSPD (all strings) -> typed rows keyed by the normalised postcode."""
    return bronze.select(
        normalise_postcode(F.col("pcds")).alias("postcode"),
        F.col("pcds").alias("postcode_raw"),
        F.col("_version").alias("nhspd_release"),
        edition_start(F.col("_version")).alias("release_date"),
        try_date(F.col("dointr"), "yyyyMM").alias("date_introduced"),
        try_date(F.col("doterm"), "yyyyMM").alias("date_terminated"),
        _blank_to_null(F.col("doterm")).isNull().alias("is_live"),
        try_int(F.col("oseast1m")).alias("easting"),
        try_int(F.col("osnrth1m")).alias("northing"),
        try_int(F.col("osgrdind")).alias("grid_quality"),
        try_int(F.col("usertype")).alias("user_type"),
        _blank_to_null(F.col("ctry")).alias("country_code"),
        _blank_to_null(F.col("oslaua")).alias("lad_code"),
        _blank_to_null(F.col("nhser")).alias("nhs_region_code"),
        _blank_to_null(F.col("sicbl")).alias("sub_icb_ods"),
        _blank_to_null(F.col("icb")).alias("icb_ods"),
        _blank_to_null(F.col("oa21")).alias("oa21_code"),
        _blank_to_null(F.col("lsoa21")).alias("lsoa21_code"),
        _blank_to_null(F.col("msoa21")).alias("msoa21_code"),
        (_failed(F.col("dointr"), try_date(F.col("dointr"), "yyyyMM"))
         | _failed(F.col("doterm"), try_date(F.col("doterm"), "yyyyMM"))).alias("_bad_date"),
        (_failed(F.col("oseast1m"), try_int(F.col("oseast1m")))
         | _failed(F.col("osnrth1m"), try_int(F.col("osnrth1m")))).alias("_bad_coordinate"),
        F.col("_run_id").alias("bronze_run_id"),
    )


def icb_lookup(bronze_codes: DataFrame) -> DataFrame:
    """ICB ODS code -> GSS code + name, one row per edition with SCD2 validity."""
    df = bronze_codes.select(
        F.col("icb_cdh").alias("icb_ods"), F.col("icb_cd").alias("icb_gss"),
        F.col("icb_nm").alias("icb_name"), F.col("_version").alias("edition"),
    )
    return scd2_from_editions(df, "edition")


def enrich_icb(postcodes: DataFrame, lookup: DataFrame) -> DataFrame:
    """Attach GSS code + name using the ICB edition in force on each NHSPD release date."""
    lk = lookup.alias("lk")
    joined = postcodes.alias("pc").join(
        F.broadcast(lk),
        (F.col("pc.icb_ods") == F.col("lk.icb_ods")) & as_of(F.col("pc.release_date"), lk),
        "left",
    )
    return joined.select(
        "pc.*", F.col("lk.icb_gss"), F.col("lk.icb_name"), F.col("lk.edition").alias("icb_edition")
    )


def flag_nhspd(df: DataFrame) -> DataFrame:
    english_live = (F.col("country_code") == ENGLAND) & F.col("is_live")
    return df.withColumn("dq_flags", flags(
        (~is_valid_postcode(F.col("postcode")), "invalid_format"),
        (F.col("easting").isNull() | (F.col("grid_quality") == 9), "no_grid_reference"),
        (F.col("_bad_date"), "unparseable_date"),
        (F.col("_bad_coordinate"), "unparseable_coordinate"),
        (english_live & F.col("icb_ods").isNotNull() & F.col("icb_gss").isNull(), "icb_code_unmatched"),
    )).drop("_bad_date", "_bad_coordinate")


# ---------------------------------------------------------------- GP practices (ODS)

def typed_practices(bronze: DataFrame) -> DataFrame:
    return bronze.select(
        F.col("organisation_code").alias("practice_code"),
        F.col("name").alias("practice_name"),
        F.col("postcode").alias("postcode_raw"),
        normalise_postcode(F.col("postcode")).alias("postcode"),
        F.col("status_code").alias("status"),
        try_date(F.col("open_date"), "yyyyMMdd").alias("open_date"),
        try_date(F.col("close_date"), "yyyyMMdd").alias("close_date"),
        _blank_to_null(F.col("high_level_health_geography")).alias("icb_ods"),
        _blank_to_null(F.col("commissioner")).alias("sub_icb_ods"),
        _blank_to_null(F.col("prescribing_setting")).alias("prescribing_setting"),
        F.col("_version").alias("ods_snapshot"),
        edition_start(F.col("_version")).alias("snapshot_date"),
    )


def match_practices(practices: DataFrame, postcodes: DataFrame) -> DataFrame:
    """Join practices to the NHSPD release in force on their snapshot date; add flags + blocking.

    blocking = cannot be placed on a map (bad format, unknown postcode, no grid reference);
    informational = usable but noteworthy (postcode terminated before the snapshot).
    """
    pc = postcodes.select("postcode", "nhspd_release", "release_date", "is_live", "date_terminated",
                          "easting", "northing", "grid_quality", "lsoa21_code", "msoa21_code",
                          F.col("icb_gss").alias("postcode_icb_gss"))
    # latest NHSPD release published on or before the snapshot date
    releases = postcodes.select("nhspd_release", "release_date").distinct()
    chosen = (practices.select("ods_snapshot", "snapshot_date").distinct()
              .join(releases, F.col("release_date") <= F.col("snapshot_date"))
              .groupBy("ods_snapshot").agg(F.max_by("nhspd_release", "release_date").alias("nhspd_release")))
    m = practices.join(chosen, "ods_snapshot", "left").join(pc, ["postcode", "nhspd_release"], "left")

    valid = is_valid_postcode(F.col("postcode"))
    found = F.col("release_date").isNotNull()
    m = m.withColumn("dq_flags", flags(
        (~valid, "invalid_format"),
        (valid & ~found, "postcode_not_in_nhspd"),
        (found & (F.col("easting").isNull() | (F.col("grid_quality") == 9)), "no_grid_reference"),
        (found & ~F.col("is_live"), "postcode_terminated"),
    ))
    blocking = F.array(*[F.lit(x) for x in ("invalid_format", "postcode_not_in_nhspd", "no_grid_reference")])
    return m.withColumn("is_blocked", F.arrays_overlap(F.col("dq_flags"), blocking))
