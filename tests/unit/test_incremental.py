"""Incremental updates: validation, dedup, SCD2 MERGE with effective dates, idempotency, point in time."""

import datetime as dt

import pytest
from pyspark.sql import functions as F

from gbdemo import incremental as inc
from gbdemo import nhs_number as nhs

KEY = "test-key"
NUMBERS = [n for n in (nhs.synthetic(i) for i in range(200)) if n][:6]
A, B, C, D, E, F_ = NUMBERS
RAW_COLS = [c.split()[0] for c in inc.EVENT_SCHEMA.split(", ")]


@pytest.fixture(scope="module")
def refs(spark):
    postcodes = spark.createDataFrame(
        [
            ("EX1 1AA", "L1", "I1", "D1", True, 1, 1),
            ("EX2 2BB", "L2", "I1", "D1", True, 1, 1),
            ("EX3 3CC", "L3", "I1", "D1", True, None, 9),  # no usable location
        ],
        "postcode string, lsoa21_code string, icb_gss string, lad_code string, is_live boolean, "
        "easting int, grid_quality int",
    )
    practices = spark.createDataFrame([("P1",), ("P2",)], "practice_code string")
    return postcodes, practices


def pid(spark, number):
    df = spark.createDataFrame([(number,)], "n string")
    return df.select(nhs.pseudonymise_col(F.col("n"), KEY).alias("p")).first().p


def raw(spark, rows):
    """Event rows as received (all strings), plus the source file column Auto Loader adds."""
    return spark.createDataFrame(rows, ", ".join(f"{c} string" for c in RAW_COLS)).withColumn(
        "_source_file", F.lit("updates.csv")
    )


def new_history(spark, name):
    seed = spark.createDataFrame(
        [(pid(spark, n), "EX1 1AA", "L1", "I1", "D1", "P1", dt.date(2020, 1, 1)) for n in (A, B, C)],
        "patient_id string, postcode string, lsoa21_code string, icb_gss string, lad_code string, "
        "practice_code string, registration_date date",
    )
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    inc.seed_history(seed).write.format("delta").saveAsTable(name)
    return name


def apply(spark, table, batch, refs):
    postcodes, practices = refs
    staged, q1, c1 = inc.stage(batch, postcodes, practices, KEY)
    source, q2, c2 = inc.plan(spark.table(table), staged)
    inc.merge(spark, table, source)
    return {**c1, **c2}


def test_stage_validates_and_deduplicates(spark, refs):
    batch = raw(
        spark,
        [
            ("e1", A, inc.MOVE, "2026-07-10", "ex2 2bb", None, "2026-07"),  # ok (messy postcode is fixed)
            ("e1", A, inc.MOVE, "2026-07-10", "ex2 2bb", None, "2026-07"),  # duplicate delivery
            ("e2", "9999999990", inc.MOVE, "2026-07-10", "EX2 2BB", None, "2026-07"),  # bad check digit
            ("e3", B, "BIRTHDAY", "2026-07-10", None, None, "2026-07"),  # unknown event type
            ("e4", B, inc.LEAVE, "2026-13-40", None, None, "2026-07"),  # bad date
            ("e5", C, inc.MOVE, "2026-07-10", "EX2 OBB", None, "2026-07"),  # typo: invalid postcode
            ("e6", C, inc.MOVE, "2026-07-10", "EX3 3CC", None, "2026-07"),  # postcode with no location
            ("e7", C, inc.TRANSFER, "2026-07-10", None, "P9", "2026-07"),  # unknown practice
        ],
    )
    staged, quarantine, counts = inc.stage(batch, *refs, KEY)
    assert counts == {"events_in": 8, "duplicates_removed": 1, "quarantined_validation": 6, "staged": 1}
    assert staged.first().postcode == "EX2 2BB" and staged.first().lsoa21_code == "L2"
    reasons = {r.event_id: r.dq_flags for r in quarantine.collect()}
    assert reasons == {
        "e2": ["invalid_nhs_number"],
        "e3": ["unknown_event_type"],
        "e4": ["invalid_effective_date"],
        "e5": ["invalid_postcode"],
        "e6": ["postcode_not_usable"],
        "e7": ["unknown_practice"],
    }
    assert "nhs_number" not in quarantine.columns  # pseudonymised before anything is stored


def test_scd2_merge_effective_dates_and_idempotency(spark, refs):
    table = new_history(spark, "hist_scd2")
    batch = raw(
        spark,
        [
            ("m1", A, inc.MOVE, "2026-07-10", "EX2 2BB", None, "2026-07"),
            (
                "t1",
                A,
                inc.TRANSFER,
                "2026-07-20",
                None,
                "P2",
                "2026-07",
            ),  # 2nd event, same patient, same batch
            ("l1", B, inc.LEAVE, "2026-07-15", None, None, "2026-07"),
            ("n1", C, inc.MOVE, "2026-07-12", "EX1 1AA", None, "2026-07"),  # same postcode: no-op
            ("r1", D, inc.NEW, "2026-07-05", "EX2 2BB", "P2", "2026-07"),
            ("u1", E, inc.MOVE, "2026-07-11", "EX2 2BB", None, "2026-07"),  # not registered: unknown patient
            ("x1", A, inc.MOVE, "2019-12-31", "EX2 2BB", None, "2026-07"),  # before current version: late
        ],
    )
    counts = apply(spark, table, batch, refs)
    assert counts["applied"] == 4 and counts["no_change"] == 1 and counts["quarantined_apply"] == 2
    assert counts["rows_closed"] == 2 and counts["already_applied"] == 0

    h = spark.table(table)
    a = [r.asDict() for r in h.where(F.col("patient_id") == pid(spark, A)).orderBy("valid_from").collect()]
    assert [
        (r["valid_from"], r["valid_to"], r["postcode"], r["practice_code"], r["is_current"]) for r in a
    ] == [
        (dt.date(2020, 1, 1), dt.date(2026, 7, 10), "EX1 1AA", "P1", False),
        (dt.date(2026, 7, 10), dt.date(2026, 7, 20), "EX2 2BB", "P1", False),
        (dt.date(2026, 7, 20), None, "EX2 2BB", "P2", True),
    ]
    b = h.where((F.col("patient_id") == pid(spark, B)) & F.col("is_current")).first()
    assert (b.is_active, b.valid_from, b.event_type) == (False, dt.date(2026, 7, 15), inc.LEAVE)
    assert h.where(F.col("patient_id") == pid(spark, C)).count() == 1  # no-op left it alone
    assert inc.check_history(h)["patients_without_one_current_row"] == 0
    assert inc.check_history(h)["broken_chain_rows"] == 0

    # point in time: the version valid on each date
    assert inc.registered_as_of(h, "2026-07-12").count() == 4  # A, B, C, D
    assert inc.registered_as_of(h, "2026-07-31").count() == 3  # B has left
    on_12th = inc.registered_as_of(h, "2026-07-12").where(F.col("patient_id") == pid(spark, A)).first()
    assert (on_12th.postcode, on_12th.practice_code) == ("EX2 2BB", "P1")

    # reprocessing the same file (re-delivery or a rerun) changes nothing
    before = sorted(map(tuple, h.orderBy("patient_id", "valid_from").collect()))
    again = apply(spark, table, batch, refs)
    assert again["already_applied"] == 4 and again["applied"] == 0 and again["rows_closed"] == 0
    assert sorted(map(tuple, spark.table(table).orderBy("patient_id", "valid_from").collect())) == before


def test_new_then_move_in_one_batch(spark, refs):
    table = new_history(spark, "hist_new_move")
    batch = raw(
        spark,
        [
            ("n1", F_, inc.NEW, "2026-08-02", "EX1 1AA", "P1", "2026-08"),
            ("m1", F_, inc.MOVE, "2026-08-20", "EX2 2BB", None, "2026-08"),
        ],
    )
    assert apply(spark, table, batch, refs)["applied"] == 2
    rows = spark.table(table).where(F.col("patient_id") == pid(spark, F_)).orderBy("valid_from").collect()
    assert [(r.postcode, r.is_current, r.valid_to) for r in rows] == [
        ("EX1 1AA", False, dt.date(2026, 8, 20)),
        ("EX2 2BB", True, None),
    ]


def test_generate_updates_is_deterministic_and_realistic(spark, monkeypatch):
    # higher problem rates so every injected problem appears in a small sample
    monkeypatch.setattr(inc, "DUPLICATE_RATE", 0.2)
    monkeypatch.setattr(inc, "BAD_POSTCODE_RATE", 0.5)
    monkeypatch.setattr(inc, "LATE_RATE", 0.5)
    people = (
        spark.range(0, 20000)
        .select(nhs.synthetic_col(F.col("id")).alias("nhs_number"))
        .where("nhs_number IS NOT NULL")
    )
    postcodes = spark.createDataFrame([(f"EX{i} 1AA",) for i in range(1, 30)], "new_postcode string")
    practices = spark.createDataFrame([("P1",), ("P2",), ("P3",)], "new_practice string")

    one = inc.generate_updates(people, postcodes, practices, "2026-08", first_new_index=950_000)
    two = inc.generate_updates(people, postcodes, practices, "2026-08", first_new_index=950_000)
    assert sorted(map(tuple, one.collect())) == sorted(map(tuple, two.collect()))
    assert {r.event_type for r in one.select("event_type").distinct().collect()} == set(inc.EVENT_TYPES)
    assert one.count() > one.select("event_id").distinct().count()  # duplicate deliveries
    assert one.where(F.col("postcode").contains(" O")).count() > 0  # typo'd postcodes
    assert one.where(F.col("effective_date") < "2026-08-01").count() > 0  # late arrivals
    assert (
        one.where(F.col("event_type") == inc.NEW)
        .select(nhs.is_valid_col(F.col("nhs_number")).alias("ok"))
        .where("ok")
        .count()
        > 0
    )


def test_new_registration_indices_must_stay_in_the_test_range(spark):
    # past index 999,999 numbers would wrap and repeat: fail loudly instead
    people = spark.range(0, 1000).select(nhs.synthetic_col(F.col("id")).alias("nhs_number"))
    postcodes = spark.createDataFrame([("EX1 1AA",)], "new_postcode string")
    practices = spark.createDataFrame([("P1",)], "new_practice string")
    with pytest.raises(ValueError, match="outside"):
        inc.generate_updates(people, postcodes, practices, "2026-07", first_new_index=999_999)
    assert inc.generate_updates(people, postcodes, practices, "2026-07", first_new_index=950_000).count() > 0
