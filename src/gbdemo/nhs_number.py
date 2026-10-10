"""NHS number utilities: Modulus 11 check digit, synthetic numbers, keyed pseudonyms.

Job ad: "Apply access, pseudonymisation and disclosure controls to sensitive data".
Synthetic numbers use the 999 test range so they can never collide with real patients.
Pseudonyms are HMAC-SHA256 with a key held in a Databricks secret scope, never in code.

Modulus 11: multiply the first 9 digits by weights 10..2, sum, take the remainder mod 11;
check digit = 11 - remainder (11 -> 0). A result of 10 means the 9 digits can never form a
valid NHS number (about 1 in 11 bodies).
"""

import hashlib
import hmac

import pandas as pd
from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType

WEIGHTS = range(10, 1, -1)  # 10, 9, ..., 2
TEST_PREFIX = "999"  # NHS-reserved test range: never a real patient
TEST_BODIES = 10**6  # 999 + 6 free digits -> one million 9-digit bodies


def check_digit(body: str) -> int | None:
    """Check digit for a 9-digit body, or None if no valid number can use this body."""
    if len(body) != 9 or not body.isdigit():
        raise ValueError(f"expected 9 digits, got {body!r}")
    c = 11 - sum(int(d) * w for d, w in zip(body, WEIGHTS, strict=True)) % 11
    return {11: 0, 10: None}.get(c, c)


def is_valid(nhs_number: str | None) -> bool:
    if not nhs_number or len(nhs_number) != 10 or not nhs_number.isdigit():
        return False
    return check_digit(nhs_number[:9]) == int(nhs_number[9])


def synthetic(index: int) -> str | None:
    """Test-range NHS number for index 0..999,999, or None if that body has no valid check digit."""
    if not 0 <= index < TEST_BODIES:
        raise ValueError(f"index {index} outside the {TEST_BODIES:,} test-range bodies")
    body = f"{TEST_PREFIX}{index:06d}"
    c = check_digit(body)
    return None if c is None else f"{body}{c}"


def valid_test_indices() -> list[int]:
    """All indices whose test-range body has a valid check digit (~909k of 1M)."""
    return [i for i in range(TEST_BODIES) if check_digit(f"{TEST_PREFIX}{i:06d}") is not None]


# ---------------------------------------------------------------- Spark versions


def check_digit_col(body: Column) -> Column:
    """Spark expression: check digit of a 9-digit string column (null when invalid / not digits)."""
    total = sum(F.substring(body, i + 1, 1).try_cast("int") * w for i, w in enumerate(WEIGHTS))
    c = F.lit(11) - total % 11
    return F.when(body.rlike(r"^[0-9]{9}$"), F.when(c == 11, 0).when(c == 10, None).otherwise(c))


def is_valid_col(nhs: Column) -> Column:
    """Spark expression: true if a 10-character string is a valid NHS number."""
    ok = nhs.rlike(r"^[0-9]{10}$") & (
        check_digit_col(F.substring(nhs, 1, 9)) == F.substring(nhs, 10, 1).cast("int")
    )
    return F.coalesce(ok, F.lit(False))


def synthetic_col(index: Column) -> Column:
    """Spark expression: test-range NHS number for an integer index (null if the body is invalid)."""
    body = F.concat(F.lit(TEST_PREFIX), F.lpad(index.cast("string"), 6, "0"))
    return F.concat(body, check_digit_col(body).cast("string"))


def pseudonymise_col(nhs: Column, key: str) -> Column:
    """HMAC-SHA256 of the NHS number with a secret key: stable per person, not reversible
    without the key, and unlinkable to other datasets that use a different key."""
    k = key.encode()

    @pandas_udf(StringType())
    def _hmac(values: pd.Series) -> pd.Series:
        return values.map(
            lambda v: None if v is None else hmac.new(k, v.encode(), hashlib.sha256).hexdigest()
        )

    return _hmac(nhs)
