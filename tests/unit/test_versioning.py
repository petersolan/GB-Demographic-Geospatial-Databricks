"""Editions -> SCD2 validity windows, and as-of joins pick the edition in force."""
import datetime as dt

from pyspark.sql import functions as F

from gbdemo.versioning import as_of, scd2_from_editions


def test_scd2_windows(spark):
    df = spark.createDataFrame([("QJK", "2023-04"), ("QJK", "2026-04"), ("QH8", "2023-04")],
                               "code string, ed string")
    out = {(r.code, r.ed): r for r in scd2_from_editions(df, "ed").collect()}
    old, new = out[("QJK", "2023-04")], out[("QJK", "2026-04")]
    assert old.valid_from == dt.date(2023, 4, 1) and old.valid_to == dt.date(2026, 3, 31)
    assert not old.is_current
    assert new.valid_from == dt.date(2026, 4, 1) and new.valid_to is None and new.is_current


def test_as_of_picks_edition_in_force(spark):
    codes = spark.createDataFrame([("A", "2023-04"), ("A", "2026-04")], "k string, ed string")
    lk = scd2_from_editions(codes, "ed")
    facts = spark.createDataFrame([("A", dt.date(2025, 8, 1)), ("A", dt.date(2026, 8, 1))],
                                  "k string, d date")
    got = {r.d: r.ed for r in facts.join(lk, (facts.k == lk.k) & as_of(F.col("d"), lk)).collect()}
    assert got == {dt.date(2025, 8, 1): "2023-04", dt.date(2026, 8, 1): "2026-04"}
