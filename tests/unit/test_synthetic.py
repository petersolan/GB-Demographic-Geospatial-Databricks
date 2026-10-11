"""Synthetic registrations: reproducible, in scope, valid test-range numbers, defects as declared."""
import pytest

from gbdemo import nhs_number as nhs
from gbdemo import synthetic
from gbdemo.postcodes import POSTCODE_RE
from gbdemo.reconcile import ReconciliationError

EXETER, ELSEWHERE = "E07000041", "E07000045"
PC_COLS = ("nhspd_release string, postcode string, easting int, northing int, grid_quality int, "
           "lad_code string, lsoa21_code string, is_live boolean")
PRACTICE_COLS = "practice_code string, status string, prescribing_setting string, easting int, northing int"
RATES = {k: 0.08 for k in synthetic.DEFAULT_RATES}   # high rates so every defect appears


@pytest.fixture(scope="module")
def inputs(spark):
    census = spark.createDataFrame([
        ("EX1 1HS", 40), ("ex2 4nf", 30),             # second one deliberately messy: must normalise
        ("TQ1 1AA", 50),                              # outside the LAD scope
    ], "postcode string, residents int")
    postcodes = spark.createDataFrame([
        ("2026-08", "EX1 1HS", 292057, 92517, 1, EXETER, "E01034630", True),
        ("2026-08", "EX2 4NF", 291500, 91800, 1, EXETER, "E01020000", True),
        ("2026-08", "TQ1 1AA", 292000, 64000, 1, ELSEWHERE, "E01020200", True),
        ("2026-08", "EX1 1AB", 292100, 92600, 1, EXETER, "E01034630", False),  # terminated, same sector
        ("2026-08", "EX2 4AA", 291400, 91700, 1, EXETER, "E01020000", False),  # terminated, same sector
    ], PC_COLS)
    practices = spark.createDataFrame([
        ("L1", "ACTIVE", "RO76", 292000, 92500), ("L2", "ACTIVE", "RO76", 291600, 91900),
        ("L3", "ACTIVE", "RO76", 295000, 95000), ("L4", "ACTIVE", "RO76", 300000, 99000),
        ("L5", "INACTIVE", "RO76", 292050, 92510),   # nearest but closed
        ("Y1", "ACTIVE", "RO72", 292055, 92515),     # nearest of all, but a minor injury unit: no list
    ], PRACTICE_COLS)
    return census, postcodes, practices


def run(spark, inputs, bodies=5000):
    census, postcodes, practices = inputs
    return synthetic.generate(spark, census, postcodes, practices, [EXETER], "2026-10-01",
                              rates=RATES, bodies=bodies)


def test_reproducible(spark, inputs):
    a, _ = run(spark, inputs)
    b, _ = run(spark, inputs)
    key = ["nhs_number", "practice_code", "registration_date"]
    assert sorted(a.collect(), key=lambda r: [str(r[k]) for k in key]) == \
        sorted(b.collect(), key=lambda r: [str(r[k]) for k in key])


def test_scope_numbers_and_practices(spark, inputs):
    regs, truth = run(spark, inputs)
    t = truth.collect()
    people = [r for r in t if not r.is_duplicate]
    assert {r.true_postcode for r in t} == {"EX1 1HS", "EX2 4NF"}           # TQ1 is out of scope
    assert 70 <= len(people) <= 75                                          # 70 residents + ~5% ghosts
    assert any(r.is_ghost for r in people)
    assert len({r.nhs_number for r in people}) == len(people)               # unique per person
    assert all(r.nhs_number.startswith("999") and nhs.is_valid(r.nhs_number) for r in t)
    assert {r.practice_code for r in regs.collect()} <= {"L1", "L2", "L3"}   # 3 nearest open GP practices


def test_defects_do_what_they_say(spark, inputs):
    import re

    _, truth = run(spark, inputs)
    rows = truth.collect()
    by = {}
    for r in rows:
        by.setdefault(r.defect, []).append(r)
    assert set(by) >= {"blank_postcode", "invalid_format", "messy_format", "bad_check_digit"}
    assert all(r.postcode_recorded == "" for r in by["blank_postcode"])
    assert all(not re.match(POSTCODE_RE, r.postcode_recorded) for r in by["invalid_format"])
    assert all(r.postcode_recorded != r.true_postcode
               and r.postcode_recorded.replace(" ", "").upper() == r.true_postcode.replace(" ", "")
               for r in by["messy_format"])
    assert all(not nhs.is_valid(r.nhs_number_recorded) for r in by["bad_check_digit"])
    for r in by.get("terminated_postcode", []):
        assert r.postcode_recorded in {"EX1 1AB", "EX2 4AA"}
        assert r.postcode_recorded[:5] == r.true_postcode[:5]                # same sector
    assert all(r.postcode_recorded == r.true_postcode for r in by.get(None, []))


def test_capacity_guard_fails_loudly(spark, inputs):
    with pytest.raises(ReconciliationError, match="capacity"):
        run(spark, inputs, bodies=20)   # ~18 valid numbers for ~72 people
