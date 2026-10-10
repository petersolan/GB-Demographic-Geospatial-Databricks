"""Physical table layout: Delta liquid clustering.

Job ad: "Optimise Spark and spatial processing as data volumes grow".
Clustering co-locates rows with similar key values in the same files, so a filter on the keys
(one NHSPD release, one local authority) skips most files using Delta's per-file min/max stats.
Unlike Hive-style partitions, the keys can change later and small key values don't produce
thousands of tiny files.

It only pays once a table spans many files: today's silver tables are one or two files per
release, so nothing can be skipped yet. The effect at a larger scale is measured by
pipelines/benchmark_layout.py and scripts/measure_layout.py.
"""

from pyspark.sql import SparkSession


def cluster(spark: SparkSession, table: str, columns: list[str], optimize: bool = True) -> None:
    """Set the table's clustering keys (idempotent) and rewrite its files in cluster order."""
    spark.sql(f"ALTER TABLE {table} CLUSTER BY ({', '.join(columns)})")
    if optimize:
        spark.sql(f"OPTIMIZE {table}")


def clustering_columns(spark: SparkSession, table: str) -> list[str]:
    return list(spark.sql(f"DESCRIBE DETAIL {table}").first()["clusteringColumns"])
