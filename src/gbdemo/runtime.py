"""Helpers shared by the job entry points in pipelines/."""
import argparse

from gbdemo.config import Layout


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Job parameters: Unity Catalog locations plus the Databricks run id (for lineage)."""
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--bronze-schema", required=True)
    p.add_argument("--silver-schema", required=True)
    p.add_argument("--gold-schema", required=True)
    p.add_argument("--run-id", default="local")
    a, _ = p.parse_known_args(argv)
    return a


def parse_layout(argv: list[str] | None = None) -> Layout:
    """Read the Unity Catalog locations passed in by the job (see resources/*.yml)."""
    a = parse_args(argv)
    return Layout(a.catalog, a.bronze_schema, a.silver_schema, a.gold_schema)


def get_spark():
    """The active Spark session (Databricks provides one; locally, tests create it)."""
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


def check_schema(spark, catalog: str, schema: str) -> None:
    """Fail fast with a clear message if a schema is missing or not visible."""
    found = spark.sql(f"SHOW SCHEMAS IN {catalog} LIKE '{schema}'").count()
    if not found:
        raise RuntimeError(f"Schema {catalog}.{schema} not found: was the bundle deployed?")
