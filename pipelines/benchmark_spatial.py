"""Benchmark: point-in-polygon join strategies as data volume grows.

Variants (same result, different query):
  A  ST_Covers only           - leave the optimisation to Databricks' spatial join
  B  bounding box + ST_Covers - "filter, then refine" with plain numeric comparisons first
Points: located postcodes in one NHS England region (default Y58, South West) at increasing sizes,
then every located postcode in England and Wales.
Polygons: all LSOAs in England and Wales (35,672), so the engine must find the relevant few.

Quota safety (Databricks Free Edition): each query is cancelled after TIMEOUT_S with
spark.interruptAll(); a variant that times out is not tried at larger sizes; the job itself has
a timeout in resources/benchmark_job.yml.
"""

import contextlib
import io
import threading
import time

from pyspark.sql import functions as F

from gbdemo import spatial
from gbdemo.runtime import check_schema, get_spark, parse_args, parse_layout

REGION = "Y58"  # NHS England South West
SIZES = [10_000, 50_000, None, "national"]  # None = whole region; "national" = all of E&W
TIMEOUT_S = 300


def join_plan(df) -> str:
    """The join operator Spark chose (from the physical plan), e.g. a spatial or nested-loop join."""
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain()
    lines = [ln.strip(" +-*:") for ln in buf.getvalue().splitlines() if "Join" in ln]
    return lines[0].split("(")[0].split(" [")[0][:80] if lines else "unknown"


def timed_count(spark, df) -> tuple[int | None, float, str]:
    timer = threading.Timer(TIMEOUT_S, spark.interruptAll)
    timer.start()
    t0 = time.time()
    try:
        return df.count(), round(time.time() - t0, 1), "ok"
    except Exception as e:  # noqa: BLE001 - an interrupted query surfaces as an exception
        status = "timeout" if time.time() - t0 >= TIMEOUT_S - 1 else f"failed: {type(e).__name__}"
        return None, round(time.time() - t0, 1), status
    finally:
        timer.cancel()


def main() -> None:
    args, layout = parse_args(), parse_layout()
    spark = get_spark()
    check_schema(spark, layout.catalog, layout.silver_schema)

    postcodes = spark.table(layout.silver("postcode"))
    release = postcodes.agg(F.max("nhspd_release")).first()[0]
    region = postcodes.where((F.col("nhspd_release") == release) & (F.col("nhs_region_code") == REGION))
    points_all = spatial.postcode_points(region).select("postcode", "easting", "northing", "geom")
    ew = postcodes.where(
        (F.col("nhspd_release") == release) & F.col("country_code").isin("E92000001", "W92000004")
    )
    points_national = spatial.postcode_points(ew).select("postcode", "easting", "northing", "geom")
    n_national = points_national.count()
    polys = spark.table(layout.silver("lsoa21_geo")).select("code", "xmin", "ymin", "xmax", "ymax", "geom")
    n_polys, n_all = polys.count(), points_all.count()  # warm-up reads, not timed
    print(f"  region {REGION}: {n_all:,} located postcodes; {n_polys:,} LSOA polygons", flush=True)

    p, g = "p", "g"
    variants = {
        "A_st_covers_only": lambda pts: pts.alias(p).join(
            polys.alias(g), F.expr(f"ST_Covers({g}.geom, {p}.geom)")
        ),
        "B_bbox_then_st_covers": lambda pts: pts.alias(p).join(
            polys.alias(g),
            spatial.within_box(
                F.col(f"{p}.easting"),
                F.col(f"{p}.northing"),
                F.col(f"{g}.xmin"),
                F.col(f"{g}.ymin"),
                F.col(f"{g}.xmax"),
                F.col(f"{g}.ymax"),
            )
            & F.expr(f"ST_Covers({g}.geom, {p}.geom)"),
        ),
    }
    results, gave_up = [], set()
    for size in SIZES:
        # deterministic sample: the same postcodes every run, whatever the partitioning
        if size == "national":
            pts, n_pts = points_national, n_national
        else:
            pts = points_all if size is None else points_all.orderBy(F.xxhash64("postcode")).limit(size)
            n_pts = n_all if size is None else min(size, n_all)
        for name, build in variants.items():
            if name in gave_up:
                results.append(
                    (name, n_pts, n_polys, None, None, "skipped (timed out at a smaller size)", "")
                )
                continue
            df = build(pts)
            plan = join_plan(df)
            matches, secs, status = timed_count(spark, df)
            if status != "ok":
                gave_up.add(name)
            results.append((name, n_pts, n_polys, matches, secs, status, plan))
            print(
                f"  {name:<24} points={n_pts:>8,}  {secs!s:>7}s  matches={matches}  {status}  [{plan}]",
                flush=True,
            )

    (
        spark.createDataFrame(
            results,
            "variant string, points long, polygons long, matches long, "
            "seconds double, status string, join_operator string",
        )
        .withColumn("region", F.lit(REGION))
        .withColumn("nhspd_release", F.lit(release))
        .withColumn("_run_id", F.lit(args.run_id))
        .withColumn("run_at", F.current_timestamp())
        .write.format("delta")
        .mode("append")
        .saveAsTable(layout.silver("benchmark_spatial"))
    )
    print(f"[benchmark_spatial] {len(results)} measurements, run {args.run_id}")


if __name__ == "__main__":
    main()
