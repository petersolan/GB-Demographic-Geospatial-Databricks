"""Plausibility flags: business postcodes, no Census residents (but not new-builds), far from practice."""

import datetime as dt

from gbdemo import nhs_number as nhs
from gbdemo import registrations

REG_COLS = (
    "nhs_number string, date_of_birth date, sex string, postcode string, practice_code string, "
    "registration_date date, source_system string"
)
PC_COLS = (
    "postcode string, is_live boolean, easting int, northing int, lsoa21_code string, msoa21_code string, "
    "icb_gss string, lad_code string, grid_quality int, user_type int, date_introduced date"
)


def test_plausibility_flags(spark):
    d = dt.date
    reg = lambda i, pc, prac="L1": (nhs.synthetic(i), d(1980, 1, 1), "F", pc, prac, d(2020, 1, 1), "SYN")  # noqa: E731
    regs = spark.createDataFrame(
        [
            reg(1, "EX1 1HS"),  # normal home postcode near the practice: no flags
            reg(2, "EX1 9ZZ"),  # business ("large user") postcode: no large-user flag any more
            reg(3, "EX1 1QQ"),  # existed before Census day, nobody lived there
            reg(4, "EX5 7NB"),  # new-build after the Census: no residents is expected, NOT flagged
            reg(5, "NE1 4ST"),  # Newcastle postcode at an Exeter practice
        ],
        REG_COLS,
    )
    postcodes = spark.createDataFrame(
        [
            ("EX1 1HS", True, 292057, 92517, "A", "M", "E54000037", "E07000041", 1, 0, d(1980, 1, 1)),
            ("EX1 9ZZ", True, 292100, 92600, "A", "M", "E54000037", "E07000041", 1, 1, d(1990, 1, 1)),
            ("EX1 1QQ", True, 292200, 92700, "A", "M", "E54000037", "E07000041", 1, 0, d(2001, 1, 1)),
            ("EX5 7NB", True, 296000, 93500, "B", "M", "E54000037", "E07000040", 1, 0, d(2023, 6, 1)),
            ("NE1 4ST", True, 424900, 564400, "C", "N", "E54000050", "E08000021", 1, 0, d(1980, 1, 1)),
        ],
        PC_COLS,
    )
    practices = spark.createDataFrame(
        [("L1", 292000, 92500)], "practice_code string, easting int, northing int"
    )
    census = spark.createDataFrame([("EX1 1HS", 40), ("NE1 4ST", 12)], "postcode string, residents int")

    clean, _, _ = registrations.clean(regs, postcodes, practices, "k", "2026-10-01", census=census)
    got = {r.postcode: sorted(set(r.dq_flags) - {"postcode_reformatted"}) for r in clean.collect()}

    assert got["EX1 1HS"] == []
    assert got["EX1 9ZZ"] == ["postcode_no_census_residents"]  # large-user rule removed (biased)
    assert got["EX1 1QQ"] == ["postcode_no_census_residents"]
    assert got["EX5 7NB"] == []  # new-build: not a false alarm
    assert got["NE1 4ST"] == ["far_from_practice"]
