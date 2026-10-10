"""Bronze layer: land source files as Delta tables, as received, with lineage.

Principles:
- every value lands as a string; types are applied (and failures counted) in silver
- headerless files get explicit names from gbdemo.schemas
- malformed CSV lines are kept in _corrupt_record, not silently dropped
- each source version is written with replaceWhere, so re-runs replace instead of duplicating
"""
import re

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

CORRUPT = "_corrupt_record"
LINEAGE = ["_dataset", "_version", "_source_file", "_ingested_at", "_run_id"]

# ONS puts the vintage inside column names (ICB23CD, ICB26CD, LSOA21NM...). Strip it so
# editions stack in one table; the edition itself is kept in _version.
_VINTAGE = re.compile(r"^([A-Za-z]+?)\d{2}(CDH|CD|NMW|NM)$")


def string_schema(fields: list[str], corrupt_col: str | None = CORRUPT) -> StructType:
    cols = [StructField(f, StringType(), True) for f in fields]
    if corrupt_col:
        cols.append(StructField(corrupt_col, StringType(), True))
    return StructType(cols)


def read_headerless_csv(spark: SparkSession, path: str, fields: list[str]) -> DataFrame:
    """Read a CSV with no header row using explicit field names, all as strings.

    PERMISSIVE mode keeps lines that don't parse (e.g. wrong field count) in _corrupt_record.
    """
    return (
        spark.read.schema(string_schema(fields))
        .option("header", False)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT)
        .csv(path)
        .select("*", "_metadata.file_path")
    )


def clean_column_name(name: str) -> str:
    """Strip a UTF-8 byte-order mark and whitespace; drop an ONS vintage from GSS-style names."""
    name = name.replace("﻿", "").strip()
    m = _VINTAGE.match(name)
    return f"{m.group(1)}_{m.group(2)}".lower() if m else name.lower()


def read_headed_csv(spark: SparkSession, path: str) -> DataFrame:
    """Read a CSV that has a header row, all as strings, with cleaned column names."""
    df = spark.read.option("header", True).option("inferSchema", False).csv(path)
    df = df.toDF(*[clean_column_name(c) for c in df.columns])
    return df.select("*", "_metadata.file_path")


def read_geoparquet(spark: SparkSession, path: str) -> DataFrame:
    """Read a GeoParquet boundary file: attributes + WKB geometry (still binary in bronze)."""
    df = spark.read.parquet(path)
    renamed = {c: ("geom_wkb" if c == "geometry" else clean_column_name(c)) for c in df.columns}
    df = df.select([F.col(c).alias(n) for c, n in renamed.items()])
    return df.select("*", "_metadata.file_path")


def with_lineage(df: DataFrame, dataset: str, version: str, run_id: str) -> DataFrame:
    """Add lineage columns. Expects a file_path column from the readers above."""
    return (
        df.withColumnRenamed("file_path", "_source_file")
        .withColumn("_dataset", F.lit(dataset))
        .withColumn("_version", F.lit(version))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_run_id", F.lit(run_id))
    )


def write_version(df: DataFrame, table: str, version: str, column: str = "_version") -> None:
    """Overwrite only this version's rows (Delta replaceWhere): idempotent re-runs."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"{column} = '{version}'")
        .saveAsTable(table)
    )


def ingest_stats(spark: SparkSession, table: str, version: str) -> dict:
    """Rows and malformed lines for one version, counted from the Delta table after the write.

    Counting what actually landed (not the in-flight DataFrame) also sidesteps Spark's rule
    against queries on raw CSV that reference only the _corrupt_record column.
    """
    df = spark.table(table).where(F.col("_version") == version)
    agg = [F.count(F.lit(1)).alias("rows")]
    if CORRUPT in df.columns:
        agg.append(F.count(F.col(CORRUPT)).alias("corrupt_rows"))
    row = df.agg(*agg).first()
    return {"rows": row["rows"], "corrupt_rows": row["corrupt_rows"] if CORRUPT in df.columns else 0}


def log_ingest(spark: SparkSession, log_table: str, entries: list[dict]) -> None:
    """Append one audit row per dataset version loaded (feeds reconciliation later)."""
    schema = "run_id string, dataset string, version string, target_table string, " \
             "rows long, corrupt_rows long, source_sha256 string"
    (
        spark.createDataFrame(entries, schema)
        .withColumn("logged_at", F.current_timestamp())
        .write.format("delta").mode("append").saveAsTable(log_table)
    )
