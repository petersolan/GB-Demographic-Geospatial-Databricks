"""Versioned lookups and boundaries (slowly changing dimension, type 2).

Job ad: "Version lookups and boundaries so historical outputs stay reproducible".
Each lookup row carries valid_from / valid_to and the source release it came from,
so any output can be rebuilt exactly as it was for a given release.

Our sources are editions (full snapshots published on a date, e.g. ICB codes April 2023 and
April 2026), so validity comes from edition dates: an edition is valid from its start until the
day before the next edition; the newest is open-ended (valid_to null, is_current true).
"""
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

OPEN_END = "9999-12-31"


def edition_start(col: Column) -> Column:
    """'2023-04' or '2026-10-10' -> first day the edition applies (a DATE).

    try_to_timestamp (not to_date): under ANSI mode a failed to_date raises instead of
    returning null, and here one of the two formats is expected to fail.
    """
    fmt = F.lit("yyyy-MM-dd")
    return F.to_date(F.coalesce(F.try_to_timestamp(col, fmt),
                                F.try_to_timestamp(F.concat(col, F.lit("-01")), fmt)))


def scd2_from_editions(df: DataFrame, edition_col: str = "_version") -> DataFrame:
    """Add valid_from / valid_to / is_current to rows of stacked edition snapshots."""
    editions = df.select(edition_col).distinct().withColumn("valid_from", edition_start(F.col(edition_col)))
    # editions are a handful of rows, so an unpartitioned window is fine here
    w = Window.orderBy("valid_from")
    bounds = (
        editions.withColumn("valid_to", F.date_sub(F.lead("valid_from").over(w), 1))
        .withColumn("is_current", F.col("valid_to").isNull())
    )
    return df.join(F.broadcast(bounds), edition_col)


def as_of(date_col: Column, lookup: DataFrame) -> Column:
    """Join condition: date_col falls inside the lookup row's validity window."""
    valid_to = F.coalesce(lookup["valid_to"], F.lit(OPEN_END).cast("date"))
    return date_col.between(lookup["valid_from"], valid_to)
