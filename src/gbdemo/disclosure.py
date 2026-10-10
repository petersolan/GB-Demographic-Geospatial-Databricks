"""Statistical disclosure control for published outputs.

Job ad: "Apply ... disclosure controls to sensitive data".
Small counts are suppressed and the rest rounded to the nearest 5 before anything
leaves the platform (gold tables, CSV exports, the public results site).

Rules (common NHS / ONS practice):
- counts 1..(threshold-1) are suppressed (null) and flagged; 0 is safe to show
- every other count is rounded to the nearest `base` (default 5), so exact small values can't be
  recovered by subtracting published totals
- a ratio is computed from exact counts but suppressed if either of its counts is suppressed
Not handled here: secondary suppression (hiding extra cells so a suppressed cell can't be derived
from row/column totals). That needs a cell-pattern algorithm and is documented as out of scope.
"""

from pyspark.sql import Column
from pyspark.sql import functions as F

THRESHOLD = 10
BASE = 5


def is_suppressed(count: Column, threshold: int = THRESHOLD) -> Column:
    return (count > 0) & (count < threshold)


def protect(count: Column, threshold: int = THRESHOLD, base: int = BASE) -> Column:
    """Suppress small counts (null), round the rest to the nearest `base` (half away from zero)."""
    rounded = (F.round(count / base) * base).cast("long")
    return F.when(count.isNull(), None).when(is_suppressed(count, threshold), None).otherwise(rounded)


def protect_ratio(
    numerator: Column, denominator: Column, digits: int = 3, threshold: int = THRESHOLD
) -> Column:
    """Ratio from exact counts, suppressed when either count is suppressed or the denominator is 0."""
    bad = is_suppressed(numerator, threshold) | is_suppressed(denominator, threshold) | (denominator == 0)
    return F.when(bad, None).otherwise(F.round(numerator / denominator, digits))
