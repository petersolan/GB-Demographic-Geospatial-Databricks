"""Disclosure control: small counts suppressed, others rounded to 5, ratios protected."""

import pytest
from pyspark.sql import functions as F

from gbdemo.disclosure import protect, protect_ratio


@pytest.mark.parametrize(
    ("count", "published"),
    [
        (0, 0),  # zero is safe to publish
        (1, None),  # 1..9 suppressed
        (9, None),
        (10, 10),
        (12, 10),  # rounded to nearest 5
        (13, 15),
        (1234, 1235),
        (None, None),
    ],
)
def test_protect(spark, count, published):
    df = spark.createDataFrame([(count,)], "n long")
    assert df.select(protect(F.col("n")).alias("p")).first().p == published


def test_protect_ratio(spark):
    df = spark.createDataFrame([(110, 100), (5, 100), (100, 7), (50, 0), (1050, 1000)], "num long, den long")
    got = [r.r for r in df.select(protect_ratio(F.col("num"), F.col("den")).alias("r")).collect()]
    assert got == [1.1, None, None, None, 1.05]  # exact ratio, not from rounded counts
