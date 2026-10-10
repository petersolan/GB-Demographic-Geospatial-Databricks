"""Reconciliation: balance checks and the long-format metrics table."""
import pytest

from gbdemo.reconcile import ReconciliationError, assert_equal, record


def test_assert_equal():
    assert_equal("rows", 10, 10)
    with pytest.raises(ReconciliationError, match=r"expected 10, got 9 \(diff -1\)"):
        assert_equal("rows", 10, 9)


def test_record_appends_long_format(spark):
    spark.sql("DROP TABLE IF EXISTS default.recon_test")
    record(spark, "default.recon_test", "r1", "silver_postcodes", "2026-08", {"rows_in": 5, "rows_out": 5})
    rows = {r.metric: r.value for r in spark.table("default.recon_test").collect()}
    assert rows == {"rows_in": 5, "rows_out": 5}
