"""Postcode standardisation: deterministic fixes only, then a format check."""
import pytest
from pyspark.sql import functions as F

from gbdemo.postcodes import is_valid_postcode, normalise_postcode

CASES = [
    ("ex1 1hs", "EX1 1HS"),        # lower case
    ("EX11HS", "EX1 1HS"),         # missing space
    ("  PL1   1AA ", "PL1 1AA"),   # extra spaces
    ("ex1-1hs", "EX1 1HS"),        # punctuation
    ("AB1  0AA", "AB1 0AA"),       # NHSPD fixed 8-character form (pcd2)
    ("EC1A1BB", "EC1A 1BB"),       # 4-character outward code
    ("W1A0AX", "W1A 0AX"),
    ("B11AA", "B1 1AA"),           # shortest real form
    ("", None),
    (None, None),
    ("1234", None),                # too short to be a postcode
    ("EX1 1HS EXTRA", None),       # too long
]


@pytest.mark.parametrize(("raw", "expected"), CASES)
def test_normalise_postcode(spark, raw, expected):
    df = spark.createDataFrame([(raw,)], "pc string")
    assert df.select(normalise_postcode(F.col("pc")).alias("n")).first().n == expected


@pytest.mark.parametrize(("pc", "valid"), [
    ("EX1 1HS", True), ("EC1A 1BB", True), ("B1 1AA", True),
    ("GIR 0AA", True),    # Girobank: real, live, breaks the normal pattern
    ("NPT 0AD", True),    # historic Newport area: 3-letter outward code, no digit
    ("ABC 0AD", False),   # but other 3-letter outward codes are not postcodes
    ("123 456", False),   # digits where letters belong
    ("EX1 HS1", False),   # inward code must be digit + 2 letters
    (None, False),
])
def test_is_valid_postcode(spark, pc, valid):
    df = spark.createDataFrame([(pc,)], "pc string")
    assert df.select(is_valid_postcode(F.col("pc")).alias("v")).first().v is valid
