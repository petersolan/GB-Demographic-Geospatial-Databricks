"""Reconciliation of counts before and after joins.

Job ad: "Automate data quality checks and reconcile counts before and after joins".
Every step records named metrics (rows in, rows out, matched, flagged...) into one
long-format table, and balance checks stop a run when totals don't add up.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

_SCHEMA = "run_id string, step string, version string, metric string, value long"


class ReconciliationError(AssertionError):
    """Raised when counts that must balance do not."""


def assert_equal(name: str, expected: int, actual: int) -> None:
    if expected != actual:
        diff = actual - expected
        raise ReconciliationError(f"{name}: expected {expected:,}, got {actual:,} (diff {diff:+,})")


def record(spark: SparkSession, table: str, run_id: str, step: str, version: str, metrics: dict) -> None:
    """Append one row per metric, e.g. {'rows_in': 10, 'rows_out': 10, 'flag_no_grid': 3}."""
    rows = [(run_id, step, version, k, int(v)) for k, v in metrics.items()]
    (
        spark.createDataFrame(rows, _SCHEMA)
        .withColumn("recorded_at", F.current_timestamp())
        .write.format("delta").mode("append").saveAsTable(table)
    )
