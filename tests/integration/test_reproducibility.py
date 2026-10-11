"""Gold outputs can be rebuilt from exactly what they read (runs on Databricks after the pipeline).

pipelines/gold_aggregate.py pins every input to a Delta version and records the input and output
versions with the run's metrics. Here those versions are read back with time travel: the inputs
must give the counts the run reported, and the outputs must still hold what the run wrote, even
after later runs have overwritten the tables.
"""
import os

import pytest
from pyspark.sql import functions as F

SILVER, GOLD = os.environ.get("GBDEMO_SILVER"), os.environ.get("GBDEMO_GOLD")
pytestmark = pytest.mark.skipif(not (SILVER and GOLD), reason="needs the pipeline's schemas")


@pytest.fixture(scope="module")
def run_metrics(spark):
    """Metrics of the latest gold_aggregate run: {metric: value}."""
    recon = spark.table(f"{SILVER}.reconciliation").where(F.col("step") == "gold_aggregate")
    latest = recon.orderBy(F.desc("recorded_at")).first().run_id
    return {r.metric: r.value for r in recon.where(F.col("run_id") == latest).collect()}


def count_as_of(spark, table: str, version: int, where: str = "true") -> int:
    return spark.sql(f"SELECT count(*) FROM {table} VERSION AS OF {version} WHERE {where}").first()[0]


def test_all_versions_recorded(run_metrics):
    inputs = {k for k in run_metrics if k.startswith("input_version.")}
    outputs = {k for k in run_metrics if k.startswith("output_version.")}
    assert {"input_version.registrations", "input_version.gp_registered_lsoa"} <= inputs and len(inputs) >= 7
    assert outputs == {f"output_version.{t}" for t in (
        "lsoa_registered_vs_resident", "msoa_registered_vs_resident", "practice_profile", "icb_summary",
        "lsoa_calibration", "practice_calibration")}


def test_input_snapshot_reproduces_the_run(spark, run_metrics):
    v = run_metrics["input_version.registrations"]
    assert count_as_of(spark, f"{SILVER}.registrations", v) == run_metrics["registrations"]
    assert count_as_of(spark, f"{SILVER}.registrations", v, "lsoa21_code IS NULL") == \
        run_metrics["registrations_without_lsoa"]


def test_output_snapshot_reproduces_the_run(spark, run_metrics):
    for table, metric in (("lsoa_registered_vs_resident", "lsoa_rows"),
                          ("msoa_registered_vs_resident", "msoa_rows"),
                          ("icb_summary", "icb_rows")):
        v = run_metrics[f"output_version.{table}"]
        assert count_as_of(spark, f"{GOLD}.{table}", v) == run_metrics[metric], table


def test_gold_columns_documented_in_catalog(spark):
    from gbdemo import catalog_docs

    for table, (_, columns) in catalog_docs.GOLD.items():
        described = {r.col_name: r.comment for r in spark.sql(f"DESCRIBE TABLE {GOLD}.{table}").collect()}
        missing = [c for c, text in columns.items() if described.get(c) != text]
        assert not missing, f"{table}: {missing}"
