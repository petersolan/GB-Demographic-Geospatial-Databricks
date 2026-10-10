"""Job step: point-in-polygon joins in BNG with edge-case handling and reconciliation (silver).

1. lsoa21_geo / icb_geo: boundaries as native GEOMETRY (BNG), validated, repaired if needed;
   ICB editions carry SCD2 validity so each NHSPD release meets the edition in force
2. postcode_geo: each located postcode assigned to an LSOA and an ICB with ST_Covers, compared
   with NHSPD's own assignment (two methods reconciled), edge cases resolved and counted
Scope: --spatial-scope = one ICB (bounding box + 2 km, so border postcodes are tested) or "all".
"""
from pyspark.sql import functions as F

from gbdemo import reconcile, spatial
from gbdemo.bronze import write_version
from gbdemo.runtime import check_schema, get_spark, parse_args, parse_layout
from gbdemo.versioning import scd2_from_editions

BUFFER_M = 2000


def overlaps_box(df, box):
    """Polygons whose bounding box overlaps the scope box."""
    x0, y0, x1, y1 = box
    return df.where((F.col("xmax") >= x0) & (F.col("xmin") <= x1)
                    & (F.col("ymax") >= y0) & (F.col("ymin") <= y1))


def main() -> None:
    args, layout = parse_args(), parse_layout()
    spark = get_spark()
    check_schema(spark, layout.catalog, layout.silver_schema)
    log = reconcile.Recorder(spark, layout.silver("reconciliation"), args.run_id)

    # 1. boundaries -> native GEOMETRY (rebuilt each run; small tables)
    def save(df, name):
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
            .saveAsTable(layout.silver(name))
        return spark.table(layout.silver(name))

    lsoa = save(spatial.boundaries(spark.table(layout.bronze("lsoa21_boundary")), "lsoa_cd", "lsoa_nm"),
                "lsoa21_geo")
    icb_raw = spatial.boundaries(spark.table(layout.bronze("icb_boundary")), "icb_cd", "icb_nm")
    icb = save(scd2_from_editions(icb_raw, "edition"), "icb_geo")
    for name, df in (("lsoa21_geo", lsoa), ("icb_geo", icb)):
        log(name, "all", {"rows_out": df.count(), "repaired": df.where("NOT was_valid").count(),
                          "still_invalid": df.where("NOT is_valid").count()})

    # 2. scope: one ICB's bounding box (+ buffer), or everything
    box = None
    if args.spatial_scope != "all":
        # select plain columns only: the Spark Connect client can't receive GEOMETRY values,
        # so geometry must stay on the server (convert with ST_AsText/ST_AsBinary if ever needed)
        r = (icb.where((F.col("code") == args.spatial_scope) & F.col("is_current"))
             .select("code", "name", "xmin", "ymin", "xmax", "ymax").first())
        if r is None:
            raise ValueError(f"spatial scope {args.spatial_scope} is not a current ICB code")
        box = (r.xmin - BUFFER_M, r.ymin - BUFFER_M, r.xmax + BUFFER_M, r.ymax + BUFFER_M)
        print(f"  scope {args.spatial_scope} ({r.name}): box {tuple(round(v) for v in box)}")

    # 3. per NHSPD release: LSOA + ICB (edition in force) for every located postcode
    postcodes = spark.table(layout.silver("postcode"))
    out_table = layout.silver("postcode_geo")
    releases = postcodes.select("nhspd_release", "release_date").distinct().collect()
    for (release, release_date) in sorted(releases):
        pts = spatial.postcode_points(postcodes.where(F.col("nhspd_release") == release))
        lsoa_polys, icb_polys = lsoa, icb.where(
            (F.col("valid_from") <= F.lit(release_date))
            & (F.coalesce(F.col("valid_to"), F.lit("9999-12-31").cast("date")) >= F.lit(release_date)))
        if box:
            pts = pts.where(spatial.within_box(F.col("easting"), F.col("northing"), *map(F.lit, box)))
            lsoa_polys, icb_polys = overlaps_box(lsoa_polys, box), overlaps_box(icb_polys, box)
        edition = icb_polys.select("edition").first()[0]

        by_lsoa = spatial.assign(pts, lsoa_polys, ["postcode"], "lsoa21_code", "lsoa")
        by_icb = spatial.assign(pts, icb_polys, ["postcode"], "icb_gss", "icb")
        out = (pts.select("postcode", "nhspd_release", "easting", "northing", "grid_quality", "is_live")
               .join(by_lsoa, "postcode").join(by_icb, "postcode")
               .withColumn("icb_edition", F.lit(edition))
               .withColumn("spatial_scope", F.lit(args.spatial_scope))
               .withColumn("_run_id", F.lit(args.run_id)))
        write_version(out, out_table, release, column="nhspd_release")

        written = spark.table(out_table).where(F.col("nhspd_release") == release)
        points_in, rows_out = pts.count(), written.count()
        reconcile.assert_equal(f"postcode_geo {release} rows (one row per point)", points_in, rows_out)
        log("postcode_geo", release, {
            "points_in": points_in, "rows_out": rows_out,
            "lsoa_agrees": written.where("lsoa_agrees").count(),
            "icb_agrees": written.where("icb_agrees").count(),
            **reconcile.value_counts(written, "lsoa_resolution", "lsoa_"),
            **reconcile.value_counts(written, "icb_resolution", "icb_"),
        })
    print(f"[silver_spatial] done, scope {args.spatial_scope}, run {args.run_id}")


if __name__ == "__main__":
    main()
