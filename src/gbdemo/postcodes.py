"""Postcode standardisation and validation.

Job ad: "Standardise postcodes and enrich records using NHS and ONS postcode products".
Cleans free-text postcodes (case, spacing, stray characters), validates the UK format,
and prepares a join key for the NHS Postcode Directory (NHSPD), including terminated
postcodes. Unfixable values are routed to a quarantine table with a reason code.

A UK postcode is an outward code (2-4 characters, e.g. EX1, EC1A) and an inward code that is
always exactly 3 characters: digit + 2 letters (e.g. 1HS). So after removing everything that
isn't a letter or digit, the standard form is "<all but last 3> <last 3>".
Only deterministic fixes are made (case, spacing, punctuation): guessing at typos such as
O for 0 would silently attach records to the wrong place.
"""
from pyspark.sql import Column
from pyspark.sql import functions as F

# Format check: outward = 1-2 letters, a digit, optional letter/digit; inward = digit + 2 letters.
# NHSPD membership is the authoritative test; this only rejects values that cannot be postcodes.
# Two real exceptions found in NHSPD itself: GIR 0AA (Girobank, live) and the historic NPT
# (Newport) area, whose outward code is 3 letters with no digit (2,417 postcodes, all terminated).
POSTCODE_RE = r"^(GIR 0AA|NPT [0-9][A-Z]{2}|[A-Z]{1,2}[0-9][A-Z0-9]? [0-9][A-Z]{2})$"


def normalise_postcode(col: Column) -> Column:
    """Upper-case, keep letters and digits only, re-insert the single space before the inward code.

    Returns null for empty input and for strings too short or long to be a postcode (5-7 chars
    without the space), so they fail validation and are quarantined with a reason.
    """
    compact = F.regexp_replace(F.upper(col), r"[^A-Z0-9]", "")
    n = F.length(compact)
    outward = compact.substr(F.lit(1), n - 3)
    inward = F.substring(compact, -3, 3)
    return F.when(n.between(5, 7), F.concat(outward, F.lit(" "), inward))


def is_valid_postcode(col: Column) -> Column:
    """True if a normalised postcode has the UK format (null input -> False)."""
    return F.coalesce(col.rlike(POSTCODE_RE), F.lit(False))
