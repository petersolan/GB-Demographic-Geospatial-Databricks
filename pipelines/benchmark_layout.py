"""Benchmark job: what liquid clustering does once a table spans many files (run on demand).

The pipeline's silver tables are still one or two files per release, so clustering can't skip
anything there yet. This job builds the scale where it matters: --copies monthly snapshots of every
postcode in the latest NHSPD release (40 x 2.7M = 109M rows), written twice:

  layout_bench_plain      in arrival order (snapshot, then postcode), compacted
  layout_bench_clustered  the same rows, CLUSTER BY (snapshot, lad_code), optimised

Both get the same small target file size, so they have similar file counts and only the order of
rows across files differs. scripts/measure_layout.py then runs the same queries against both on
the SQL warehouse and reads files read / skipped from the query history.
"""

import argparse

from pyspark.sql import functions as F

from gbdemo import clustering
from gbdemo.runtime import check_schema, get_spark, parse_args, parse_layout

COLUMNS = ["postcode", "lad_code", "lsoa21_code", "msoa21_code", "icb_gss", "easting", "northing"]


def main() -> None:
    args, layout = parse_args(), parse_layout()
    p = argparse.ArgumentParser()
    p.add_argument("--copies", type=int, default=40)
    p.add_argument("--target-file-size", default="16mb")
    bench, _ = p.parse_known_args()
    spark = get_spark()
    check_schema(spark, layout.catalog, layout.silver_schema)

    postcodes = spark.table(layout.silver("postcode"))
    release = postcodes.agg(F.max("nhspd_release")).first()[0]
    rows = (
        postcodes.where(F.col("nhspd_release") == release)
        .select(*COLUMNS)
        .crossJoin(spark.range(bench.copies).select(F.col("id").cast("int").alias("snapshot")))
        .orderBy("snapshot", "postcode")  # how monthly appends would arrive
    )

    for name, keys in (("layout_bench_plain", None), ("layout_bench_clustered", ["snapshot", "lad_code"])):
        table = layout.silver(name)
        rows.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
        size = bench.target_file_size
        spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('delta.targetFileSize' = '{size}')")
        if keys:
            clustering.cluster(spark, table, keys)
        else:
            spark.sql(f"OPTIMIZE {table}")  # compaction only: same file size, arrival order kept
        d = spark.sql(f"DESCRIBE DETAIL {table}").first()
        print(f"  {name}: {spark.table(table).count():,} rows, {d['numFiles']} files, "
              f"{d['sizeInBytes'] / 1e6:,.0f} MB, clustering {list(d['clusteringColumns'])}", flush=True)
    print(f"[benchmark_layout] NHSPD {release} x {bench.copies} snapshots; run {args.run_id}")


if __name__ == "__main__":
    main()
