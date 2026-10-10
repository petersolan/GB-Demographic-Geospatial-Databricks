"""NHS number check digits, the 999 test range, Spark/Python agreement and keyed pseudonyms."""
import hashlib
import hmac

import pytest
from pyspark.sql import functions as F

from gbdemo import nhs_number as nhs


@pytest.mark.parametrize(("number", "valid"), [
    ("9434765919", True),    # 9x10+4x9+3x8+4x7+7x6+6x5+5x4+9x3+1x2 = 299; 299 % 11 = 2; 11-2 = 9
    ("4010232137", True),    # weighted sum 92; 92 % 11 = 4; 11-4 = 7
    ("9434765918", False),   # wrong check digit
    ("943476591", False),    # too short
    ("94347659A9", False),   # not all digits
    (None, False),
])
def test_is_valid(number, valid):
    assert nhs.is_valid(number) is valid


def test_body_with_no_valid_check_digit():
    # 999000000: 9x10 + 9x9 + 9x8 = 243; 243 % 11 = 1; 11 - 1 = 10 -> can never be issued
    assert nhs.check_digit("999000000") is None
    assert nhs.synthetic(0) is None
    assert nhs.synthetic(1) == "9990000018"     # 243 + 1x2 = 245; 245 % 11 = 3; 11 - 3 = 8
    assert nhs.is_valid(nhs.synthetic(1))


def test_spark_matches_python(spark):
    df = spark.range(3000).withColumn("n", nhs.synthetic_col(F.col("id")))
    got = {r.id: r.n for r in df.collect()}
    assert got == {i: nhs.synthetic(i) for i in range(3000)}
    samples = ["9434765919", "9434765918", "abc", "999000001", "99900000189", None, nhs.synthetic(1)]
    df = spark.createDataFrame([(s,) for s in samples], "s string")
    out = df.select(nhs.is_valid_col(F.col("s")).alias("v"))
    assert [r.v for r in out.collect()] == [nhs.is_valid(s) for s in samples]


def test_pseudonymise_is_keyed_stable_and_null_safe(spark):
    df = spark.createDataFrame([("9434765919",), ("9434765919",), (None,)], "n string")
    a = [r.p for r in df.select(nhs.pseudonymise_col(F.col("n"), "key-a").alias("p")).collect()]
    b = [r.p for r in df.select(nhs.pseudonymise_col(F.col("n"), "key-b").alias("p")).collect()]
    assert a[0] == a[1] == hmac.new(b"key-a", b"9434765919", hashlib.sha256).hexdigest()
    assert len(a[0]) == 64 and a[0] != b[0]      # same person, different key -> unlinkable
    assert a[2] is None
