"""On Databricks the job already has a Spark session; tests just use it."""
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.getOrCreate()
