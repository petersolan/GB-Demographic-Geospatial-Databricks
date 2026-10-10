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


class Recorder:
    """Record a step's metrics to the reconciliation table and print them for the job log."""

    def __init__(self, spark: SparkSession, table: str, run_id: str):
        self.spark, self.table, self.run_id = spark, table, run_id

    def __call__(self, step: str, version: str, metrics: dict) -> None:
        record(self.spark, self.table, self.run_id, step, version, metrics)
        shown = ", ".join(f"{k}={v:,}" for k, v in sorted(metrics.items()))
        print(f"  {step} {version}: {shown}", flush=True)


def value_counts(df, column: str, prefix: str) -> dict:
    """{'<prefix><value>': rows} for each distinct value of a column (array columns are exploded)."""
    from pyspark.sql import functions as F

    col = F.explode(column) if dict(df.dtypes)[column].startswith("array") else F.col(column)
    rows = df.select(col.alias("v")).groupBy("v").count().collect()
    return {f"{prefix}{r.v}": r["count"] for r in rows}
