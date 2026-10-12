"""Job step: write one month's synthetic registration update file to the landing volume.

Plays the part of the GP systems: a CSV of events (changes of address, practice transfers,
deregistrations, new registrations) with event ids and effective dates, including the problems
real feeds have: duplicate deliveries, typo'd postcodes and late-arriving events. From the second
month on, the previous month's file is delivered again, to prove reprocessing changes nothing.
Deterministic: generating a month twice gives the same file.
"""

from pathlib import Path

from pyspark.sql import functions as F

from gbdemo import incremental, nhs_number, reconcile
from gbdemo.runtime import check_schema, get_spark, parse_args, parse_layout

# test-range indices for new registrations: the baseline list uses indices from 0 (up to ~577k),
# so 950,000-999,999 is free; 5,000 per month gives 10 monthly slots (the range ends at 999,999)
FIRST_NEW_INDEX = 950_000
INDICES_PER_MONTH = 5_000
MONTH_SLOTS = 10


def previous(month: str) -> str:
    y, m = (int(x) for x in month.split("-"))
    return f"{y - (m == 1)}-{12 if m == 1 else m - 1:02d}"


def main() -> None:
    args, layout = parse_args(), parse_layout()
    if not args.month:
        raise ValueError("--month (YYYY-MM) is required")
    spark = get_spark()
    check_schema(spark, layout.catalog, layout.bronze_schema)
    lads = [x.strip() for x in args.synthetic_lads.split(",") if x.strip()]
    root = f"{layout.landing_path}/registration_updates"
    target = f"{root}/{args.month}"

    people = (
        spark.table(layout.bronze("gp_registrations"))
        .where(nhs_number.is_valid_col(F.col("nhs_number")))
        .select("nhs_number")
        .distinct()
    )
    postcodes = spark.table(layout.silver("postcode"))
    release = postcodes.agg(F.max("nhspd_release")).first()[0]
    movable = postcodes.where(
        (F.col("nhspd_release") == release)
        & F.col("lad_code").isin(lads)
        & F.col("is_live")
        & F.col("easting").isNotNull()
        & (F.coalesce(F.col("grid_quality"), F.lit(0)) != 9)
    ).select(F.col("postcode").alias("new_postcode"))
    practices = (
        spark.table(layout.silver("registrations"))
        .select(F.col("practice_code").alias("new_practice"))
        .distinct()
    )

    y, m = (int(x) for x in args.month.split("-"))
    first_new = FIRST_NEW_INDEX + ((y * 12 + m) % MONTH_SLOTS) * INDICES_PER_MONTH
    updates = incremental.generate_updates(people, movable, practices, args.month, first_new)
    updates.coalesce(1).write.mode("overwrite").option("header", True).csv(target)

    redelivered = 0
    prev = f"{root}/{previous(args.month)}"
    if Path(prev).exists():  # the previous file arrives again under a new name
        again = spark.read.option("header", True).schema(incremental.EVENT_SCHEMA).csv(prev)
        redelivered = again.count()
        again.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{target}_redelivered")

    written = spark.read.option("header", True).schema(incremental.EVENT_SCHEMA).csv(target)
    reconcile.Recorder(spark, layout.silver("reconciliation"), args.run_id)(
        "generate_updates",
        args.month,
        {
            "events_written": written.count(),
            "distinct_event_ids": written.select("event_id").distinct().count(),
            "previous_month_redelivered": redelivered,
            **reconcile.value_counts(written, "event_type", "type_"),
        },
    )
    print(f"[generate_updates] {args.month} -> {target}")


if __name__ == "__main__":
    main()
