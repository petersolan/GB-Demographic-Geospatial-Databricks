"""Synthetic GP registration list.

All patient-level records in this project are synthetic. Registrations are generated
from Census 2021 postcode populations and real ODS GP practices, then deliberately
degraded (bad postcodes, terminated postcodes, blanks, duplicates) so the cleaning,
quarantine and reconciliation steps have realistic problems to solve.

Reproducible: every "random" choice is a hash of stable inputs (xxhash64 + salt), not rand(),
so the same records come out on any cluster and any partitioning.
Alongside the registrations a ground-truth table records each person's true postcode and
injected defect, so the pipeline's cleaning can be measured, which real data never allows.
"""

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from gbdemo import nhs_number
from gbdemo.postcodes import normalise_postcode
from gbdemo.reconcile import assert_equal

SALT = "gbdemo-synthetic-v1"
DEFAULT_RATES = {  # share of registrations given each defect (cumulative bands, in order)
    "blank_postcode": 0.002,
    "invalid_format": 0.0025,  # e.g. digit 0 typed as letter O in the inward code
    "unknown_postcode": 0.0025,  # plausible-looking postcode that doesn't exist
    "terminated_postcode": 0.003,
    "messy_format": 0.02,  # lower case / missing or extra spaces: fixable
    "bad_check_digit": 0.001,
}
LIST_INFLATION = 0.05  # ghost registrations: people registered but no longer resident
DUPLICATE_RATE = 0.003  # people registered twice (second practice)
# ODS lists every prescribing organisation; only role RO76 holds a registered list. Minor injury units,
# prisons, pharmacies and walk-in centres are also ACTIVE in epraccur but nobody registers there.
GP_PRACTICE_SETTING = "RO76"


def stable_uniform(*cols: Column, salt: str = SALT) -> Column:
    """Deterministic value in [0, 1) from a hash of the inputs (pmod avoids abs() overflow)."""
    return F.pmod(F.xxhash64(*cols, F.lit(salt)), F.lit(1_000_000)) / F.lit(1_000_000.0)


def residents_in_scope(census: DataFrame, postcodes: DataFrame, lad_codes: list[str]) -> DataFrame:
    """Census residents per postcode, located with the latest NHSPD release, in the given LADs."""
    latest = postcodes.agg(F.max("nhspd_release")).first()[0]
    pc = postcodes.where(
        (F.col("nhspd_release") == latest)
        & F.col("lad_code").isin(lad_codes)
        & F.col("easting").isNotNull()
        & (F.coalesce("grid_quality", F.lit(0)) != 9)
    )
    # sources format postcodes differently: normalise before joining, never assume a match
    return (
        census.select(normalise_postcode(F.col("postcode")).alias("postcode"), F.col("residents").cast("int"))
        .join(pc.select("postcode", "easting", "northing", "lad_code", "lsoa21_code", "is_live"), "postcode")
        .where("residents > 0")
    )


def nearest_practices(residents: DataFrame, practices: DataFrame, k: int = 3) -> DataFrame:
    """For each postcode, the k nearest active GP practices with inverse-distance weights (BNG metres)."""
    pr = practices.where(
        (F.col("status") == "ACTIVE")
        & (F.col("prescribing_setting") == GP_PRACTICE_SETTING)
        & F.col("easting").isNotNull()
    ).select(
        "practice_code", F.col("easting").alias("pe"), F.col("northing").alias("pn")
    )
    pairs = (
        residents.select("postcode", "easting", "northing")
        .crossJoin(F.broadcast(pr))
        .withColumn(
            "d", F.sqrt((F.col("easting") - F.col("pe")) ** 2 + (F.col("northing") - F.col("pn")) ** 2)
        )
    )
    w = Window.partitionBy("postcode").orderBy("d", "practice_code")
    top = (
        pairs.withColumn("rank", F.row_number().over(w))
        .where(F.col("rank") <= k)
        .withColumn("weight", 1 / F.greatest(F.col("d"), F.lit(100.0)))
    )  # floor at 100 m
    return top.groupBy("postcode").agg(
        F.sort_array(F.collect_list(F.struct("rank", "practice_code", "weight"))).alias("choices")
    )


def people(residents: DataFrame, inflation: float = LIST_INFLATION, salt: str = SALT) -> DataFrame:
    """One row per registered person: residents plus ghost registrations (list inflation)."""
    extra = F.floor(F.col("residents") * inflation + stable_uniform(F.col("postcode"), salt=salt + "infl"))
    return (
        residents.withColumn("n", F.col("residents") + extra.cast("int"))
        .withColumn("person_seq", F.explode(F.sequence(F.lit(1), F.col("n"))))
        .withColumn("is_ghost", F.col("person_seq") > F.col("residents"))
        .drop("n")
    )


def assign_nhs_numbers(
    spark: SparkSession, persons: DataFrame, bodies: int = nhs_number.TEST_BODIES
) -> DataFrame:
    """Give each person a unique, valid NHS number from the 999 test range.

    Person order is fixed by (postcode, person_seq) through per-postcode offsets (a window over
    postcodes, not people), then matched to the n-th valid test-range body.
    """
    counts = persons.groupBy("postcode").count()
    offsets = counts.withColumn("offset", F.sum("count").over(Window.orderBy("postcode")) - F.col("count"))
    ranked = (
        persons.join(offsets.select("postcode", "offset"), "postcode")
        .withColumn("person_index", F.col("offset") + F.col("person_seq") - 1)
        .drop("offset")
    )
    valid = (
        spark.range(bodies)
        .withColumn("nhs_number", nhs_number.synthetic_col(F.col("id")))
        .where(F.col("nhs_number").isNotNull())
        .withColumn("person_index", F.row_number().over(Window.orderBy("id")) - 1)
        .select("person_index", "nhs_number")
    )
    return ranked.join(valid, "person_index")


def choose_practice(choices: Column, u: Column) -> Column:
    """Pick from [(rank, practice, weight)...] with probability proportional to weight."""
    total = F.aggregate(choices, F.lit(0.0), lambda acc, c: acc + c["weight"])
    cum = F.transform(
        choices,
        lambda c, i: F.struct(
            c["practice_code"].alias("p"),
            F.aggregate(F.slice(choices, 1, i + 1), F.lit(0.0), lambda acc, x: acc + x["weight"]).alias("cw"),
        ),
    )
    return F.coalesce(F.filter(cum, lambda c: u * total <= c["cw"])[0]["p"], choices[0]["practice_code"])


def defect_for(u: Column, rates: dict) -> Column:
    """Map a uniform value to one defect name using cumulative bands (else null = clean)."""
    expr, edge = None, 0.0
    for name, rate in rates.items():
        edge += rate
        expr = F.when(u < edge, F.lit(name)) if expr is None else expr.when(u < edge, F.lit(name))
    return expr.otherwise(F.lit(None).cast("string"))


def generate(
    spark: SparkSession,
    census: DataFrame,
    postcodes: DataFrame,
    practices: DataFrame,
    lad_codes: list[str],
    reference_date: str,
    rates: dict | None = None,
    salt: str = SALT,
    bodies: int = nhs_number.TEST_BODIES,
) -> tuple[DataFrame, DataFrame]:
    """Return (registrations as received, ground truth). See module docstring."""
    rates = rates or DEFAULT_RATES
    residents = residents_in_scope(census, postcodes, lad_codes)
    wanted = people(residents, salt=salt)
    persons = assign_nhs_numbers(spark, wanted, bodies)
    # the join to valid test numbers would silently drop anyone beyond the ~909k available
    assert_equal("people given an NHS number (test range capacity)", wanted.count(), persons.count())

    choices = nearest_practices(residents, practices)
    u = lambda tag: stable_uniform(F.col("nhs_number"), salt=salt + tag)  # noqa: E731
    p = (
        persons.join(choices, "postcode")
        .withColumn("practice_code", choose_practice(F.col("choices"), u("practice")))
        .withColumn("defect", defect_for(u("defect"), rates))
        .withColumn("sex", F.when(u("sex") < 0.51, "F").otherwise("M"))
        .withColumn("age", F.floor(u("age") * 92).cast("int"))
        .withColumn(
            "date_of_birth",
            F.date_sub(
                F.add_months(F.lit(reference_date).cast("date"), -12 * F.col("age")),
                F.floor(u("dob") * 365).cast("int"),
            ),
        )
        .withColumn(
            "registration_date",
            F.date_sub(F.lit(reference_date).cast("date"), F.floor(u("reg") * 365 * 20).cast("int")),
        )
    )

    # terminated postcodes from the same postcode sector (outward code + first inward digit)
    latest = postcodes.agg(F.max("nhspd_release")).first()[0]
    sector = lambda c: F.regexp_extract(c, r"^(\S+ [0-9])", 1)  # noqa: E731
    term = postcodes.where((F.col("nhspd_release") == latest) & ~F.col("is_live")).select(
        F.col("postcode").alias("old_postcode"), sector(F.col("postcode")).alias("sector")
    )
    term = term.withColumn(
        "k", F.row_number().over(Window.partitionBy("sector").orderBy("old_postcode")) - 1
    ).withColumn("n_in_sector", F.count("*").over(Window.partitionBy("sector")))
    p = p.withColumn("sector", sector(F.col("postcode"))).withColumn(
        "pick", F.when(F.col("defect") == "terminated_postcode", F.lit(0))
    )
    p = p.join(
        term,
        (p.sector == term.sector)
        & (term.k == F.pmod(F.xxhash64(F.col("nhs_number"), F.lit(salt + "term")), term.n_in_sector))
        & p.pick.isNotNull(),
        "left",
    ).drop(term.sector)

    inward = F.substring(F.col("postcode"), -3, 3)
    outward = F.expr("substring(postcode, 1, length(postcode) - 4)")
    recorded = (
        F.when(F.col("defect") == "blank_postcode", F.lit(""))
        .when(
            F.col("defect") == "invalid_format",
            F.concat(outward, F.lit(" "), F.lit("O"), F.substring(inward, 2, 2)),
        )
        .when(
            F.col("defect") == "unknown_postcode",
            F.concat(outward, F.lit(" "), F.substring(inward, 1, 2), F.lit("Z")),
        )
        .when(
            (F.col("defect") == "terminated_postcode") & F.col("old_postcode").isNotNull(),
            F.col("old_postcode"),
        )
        .when(
            F.col("defect") == "messy_format",
            F.when(u("messy") < 0.5, F.lower(F.regexp_replace("postcode", " ", ""))).otherwise(
                F.concat(F.lit(" "), F.lower(F.col("postcode")), F.lit("  "))
            ),
        )
        .otherwise(F.col("postcode"))
    )
    last = F.substring(F.col("nhs_number"), 10, 1).cast("int")
    nhs_recorded = F.when(
        F.col("defect") == "bad_check_digit",
        F.concat(F.substring(F.col("nhs_number"), 1, 9), ((last + 1) % 10).cast("string")),
    ).otherwise(F.col("nhs_number"))
    # a terminated-postcode defect needs an old postcode in the same sector; otherwise none applied
    p = (
        p.withColumn(
            "defect",
            F.when(
                (F.col("defect") == "terminated_postcode") & F.col("old_postcode").isNull(),
                F.lit(None).cast("string"),
            ).otherwise(F.col("defect")),
        )
        .withColumn("postcode_recorded", recorded)
        .withColumn("nhs_number_recorded", nhs_recorded)
    )

    # duplicates: a small share also registered at their second-choice practice
    dup = (
        p.where(u("dup") < DUPLICATE_RATE)
        .withColumn("practice_code", F.coalesce(F.col("choices")[1]["practice_code"], F.col("practice_code")))
        .withColumn("registration_date", F.date_add(F.col("registration_date"), 30))
        .withColumn("is_duplicate", F.lit(True))
    )
    p = p.withColumn("is_duplicate", F.lit(False)).unionByName(dup)

    registrations = p.select(
        F.col("nhs_number_recorded").alias("nhs_number"),
        "date_of_birth",
        "sex",
        F.col("postcode_recorded").alias("postcode"),
        "practice_code",
        "registration_date",
        F.lit("SYNTHETIC").alias("source_system"),
    )
    truth = p.select(
        "nhs_number",
        "nhs_number_recorded",
        F.col("postcode").alias("true_postcode"),
        "postcode_recorded",
        "practice_code",
        "defect",
        "is_ghost",
        "is_duplicate",
        "lad_code",
        "lsoa21_code",
    )
    return registrations, truth
