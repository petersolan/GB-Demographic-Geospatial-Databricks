"""Registrations: validation flags, quarantine, dedup, pseudonymisation, minimisation, balance."""

import datetime as dt

import pytest

from gbdemo import nhs_number as nhs
from gbdemo import registrations

REG_COLS = (
    "nhs_number string, date_of_birth date, sex string, postcode string, practice_code string, "
    "registration_date date, source_system string"
)
PC_COLS = (
    "postcode string, is_live boolean, easting int, lsoa21_code string, msoa21_code string, "
    "icb_gss string, lad_code string, grid_quality int"
)
GOOD, OTHER, BAD = "9434765919", "4010232137", "9434765918"


@pytest.fixture(scope="module")
def result(spark):
    d = dt.date
    regs = spark.createDataFrame(
        [
            (GOOD, d(1980, 5, 1), "F", "EX1 1HS", "L1", d(2020, 1, 1), "SYNTHETIC"),  # clean
            (GOOD, d(1980, 5, 1), "F", "EX1 1HS", "L2", d(2023, 6, 1), "SYNTHETIC"),  # duplicate, newer
            (OTHER, d(2019, 3, 15), "M", " ex11hs ", "L1", d(2020, 1, 1), "SYNTHETIC"),  # messy but fixable
            (nhs.synthetic(1), d(1950, 1, 1), "M", "", "L1", d(2020, 1, 1), "SYNTHETIC"),  # blank
            (nhs.synthetic(2), d(1950, 1, 1), "M", "EX1 OHS", "L1", d(2020, 1, 1), "SYNTHETIC"),  # bad format
            (nhs.synthetic(3), d(1950, 1, 1), "M", "EX1 1HZ", "L1", d(2020, 1, 1), "SYNTHETIC"),  # unknown
            (nhs.synthetic(4), d(1950, 1, 1), "M", "EX1 1AB", "L1", d(2020, 1, 1), "SYNTHETIC"),  # terminated
            (BAD, d(1990, 1, 1), "F", "EX1 1HS", "L1", d(2020, 1, 1), "SYNTHETIC"),  # bad check digit
            (
                nhs.synthetic(5),
                d(1990, 1, 1),
                "F",
                "EX1 1HS",
                "ZZZ",
                d(2020, 1, 1),
                "SYNTHETIC",
            ),  # unknown practice
        ],
        REG_COLS,
    )
    postcodes = spark.createDataFrame(
        [
            ("EX1 1HS", True, 292057, "E01034630", "E02000001", "E54000037", "E07000041", 1),
            ("EX1 1AB", False, 292100, "E01034630", "E02000001", "E54000037", "E07000041", 1),
        ],
        PC_COLS,
    )
    practices = spark.createDataFrame([("L1",), ("L2",)], "practice_code string")
    clean, quarantine, counts = registrations.clean(regs, postcodes, practices, "test-key", "2026-10-01")
    return clean.collect(), quarantine.collect(), counts, clean.columns


def test_balance_and_counts(result):
    clean, quarantine, counts, _ = result
    assert counts == {"rows_in": 9, "quarantined": 4, "clean": 4, "duplicates_removed": 1}
    assert counts["rows_in"] == counts["clean"] + counts["quarantined"] + counts["duplicates_removed"]


def test_quarantine_reasons(result):
    _, quarantine, _, _ = result
    reasons = sorted(tuple(r.dq_flags) for r in quarantine)
    assert reasons == sorted(
        [
            ("blank_postcode",),
            ("invalid_postcode_format",),
            ("postcode_not_in_nhspd",),
            ("invalid_nhs_number",),
        ]
    )


def test_clean_rows(result):
    clean, _, _, columns = result
    by_pc = {}
    for r in clean:
        by_pc.setdefault(r.postcode, []).append(r)
    good = next(r for r in clean if r.practice_code == "L2")  # duplicate resolved to newest
    assert good.duplicates_removed == 1 and good.age_band == "45-49" and good.year_of_birth == 1980
    messy = next(r for r in clean if r.age_band == "05-09")
    assert messy.postcode == "EX1 1HS" and "postcode_reformatted" in messy.dq_flags
    term = next(r for r in clean if "postcode_terminated" in r.dq_flags)
    assert term.postcode == "EX1 1AB" and term.lsoa21_code == "E01034630"  # usable, just flagged
    assert any("unknown_practice" in r.dq_flags for r in clean)  # informational
    # pseudonymised + minimised: no NHS number, no date of birth anywhere in silver
    assert "nhs_number" not in columns and "date_of_birth" not in columns
    assert all(len(r.patient_id) == 64 and r.patient_id not in (GOOD, OTHER) for r in clean)
