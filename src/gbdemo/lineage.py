"""Reproducible outputs with Delta time travel.

Job ad: "Version lookups and boundaries so historical outputs stay reproducible".
A step reads each input pinned to the table's current Delta version (VERSION AS OF), so every
action in the step sees the same snapshot even if another job writes meanwhile, and records the
versions it read and wrote. Any past output can then be rebuilt from exactly its inputs:

    SELECT * FROM <catalog>.<schema>.registrations VERSION AS OF <input_version.registrations>

Versions stay readable while the table keeps their files (Delta's retention settings, 7 days by
default); longer-lived reproducibility needs longer retention or a deep clone per release.
"""

from pyspark.sql import DataFrame, SparkSession


def current_version(spark: SparkSession, table: str) -> int:
    """Latest committed Delta version of a table."""
    return int(spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"])


class PinnedReader:
    """Read tables at their current version and remember which versions were read and written.

    versions() gives {'input_version.<name>': v, 'output_version.<name>': v} for the step's
    reconciliation metrics.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self._versions: dict[str, int] = {}

    def read(self, table: str, name: str) -> DataFrame:
        version = current_version(self.spark, table)
        self._versions[f"input_version.{name}"] = version
        return self.spark.sql(f"SELECT * FROM {table} VERSION AS OF {version}")

    def wrote(self, table: str, name: str) -> int:
        version = current_version(self.spark, table)
        self._versions[f"output_version.{name}"] = version
        return version

    def versions(self) -> dict[str, int]:
        return dict(self._versions)
