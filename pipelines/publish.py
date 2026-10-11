"""Job step: export protected outputs as static files for the results site.

The single exit point from the platform. All values come from gold (disclosure-controlled);
boundary shapes come from silver but are ONS open data, not patient data. Files land in the gold
exports volume (never beside raw data) and are fetched by scripts/fetch_site_data.py.

Geometry: simplified with a 10 m tolerance in BNG (metres), then re-projected to WGS84 for web
maps, and serialised to GeoJSON on the server (the Spark Connect client can't receive GEOMETRY).
"""

import datetime as dt
import json
from pathlib import Path

from pyspark.sql import functions as F

from gbdemo import disclosure, spatial
from gbdemo.runtime import check_schema, get_spark, parse_args, parse_layout

SIMPLIFY_M = 10.0
POINT_BNG = "ST_Point(CAST(easting AS DOUBLE), CAST(northing AS DOUBLE), 27700)"


def to_web_geojson(df, wkb_col: str):
    """BNG WKB -> simplified -> WGS84 GeoJSON string (all on the server)."""
    simplified = spatial.simplify_wkb(SIMPLIFY_M)(F.col(wkb_col))
    return (
        df.withColumn("_s", simplified)
        .withColumn("geojson", F.expr("ST_AsGeoJSON(ST_Transform(ST_GeomFromWKB(_s, 27700), 4326))"))
        .drop("_s")
    )


def write_json(path: Path, obj) -> None:
    path.write_text(json.dumps(obj, separators=(",", ":"), default=str))
    print(f"  wrote {path.name} ({path.stat().st_size / 1e3:,.0f} kB)")


def main() -> None:
    args, layout = parse_args(), parse_layout()
    spark = get_spark()
    check_schema(spark, layout.catalog, layout.gold_schema)
    out = Path(layout.exports_path)
    out.mkdir(parents=True, exist_ok=True)

    # 1. LSOAs in scope: protected values + simplified boundaries
    calibrated = spark.table(layout.gold("lsoa_calibration")).select(
        "lsoa21_code", "registered_nhs", "nhs_per_resident", "postcodes_since_census"
    )
    lsoa = (
        spark.table(layout.gold("lsoa_registered_vs_resident"))
        .where("lad_code IS NOT NULL")
        .join(calibrated, "lsoa21_code", "left")
    )
    shapes = spark.table(layout.silver("lsoa21_geo")).select(
        F.col("code").alias("lsoa21_code"), F.expr("ST_AsBinary(geom)").alias("wkb")
    )
    rows = to_web_geojson(lsoa.join(shapes, "lsoa21_code"), "wkb").drop("wkb").collect()
    props = [
        "lsoa21_code",
        "lsoa21_name",
        "lad_code",
        "residents",
        "registered",
        "registered_per_resident",
        "any_suppressed",
        "registered_nhs",  # real, NHS England (gold.lsoa_calibration)
        "nhs_per_resident",
        "postcodes_since_census",
    ]
    write_json(
        out / "lsoa.geojson",
        {
            "type": "FeatureCollection",
            "features": [
                {"type": "Feature", "geometry": json.loads(r.geojson), "properties": {k: r[k] for k in props}}
                for r in rows
            ],
        },
    )

    # 2. practices with registrations: protected list size, public ODS location
    sizes = (
        spark.table(layout.gold("practice_profile"))
        .select("practice_code", "practice_name", "list_size")
        .distinct()
    )
    snapshot = spark.table(layout.silver("gp_practice")).agg(F.max("ods_snapshot")).first()[0]
    loc = (
        spark.table(layout.silver("gp_practice"))
        .where(F.col("ods_snapshot") == snapshot)
        .where("easting IS NOT NULL")
        .select(
            "practice_code",
            F.expr(f"ST_AsGeoJSON(ST_Transform({POINT_BNG}, 4326))").alias("geojson"),
        )
    )
    real = spark.table(layout.gold("practice_calibration")).select("practice_code", "registered_nhs")
    prows = sizes.join(loc, "practice_code").join(real, "practice_code", "left").collect()
    write_json(
        out / "practices.geojson",
        {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": json.loads(r.geojson),
                    "properties": {
                        "practice_code": r.practice_code,
                        "practice_name": r.practice_name,
                        "list_size": r.list_size,
                        "registered_nhs_study_area": r.registered_nhs,  # real patients from the study LSOAs
                    },
                }
                for r in prows
            ],
        },
    )

    # 3. ICB summary + pipeline quality metrics (latest run of each step) + metadata
    icb = [r.asDict() for r in spark.table(layout.gold("icb_summary")).collect()]
    recon = spark.table(layout.silver("reconciliation"))
    latest = recon.groupBy("step").agg(F.max_by("run_id", "recorded_at").alias("run_id"))
    metrics = {}
    for r in recon.join(latest, ["step", "run_id"]).collect():
        metrics.setdefault(r.step, {}).setdefault(r.version, {})[r.metric] = r.value
    release = spark.table(layout.gold("icb_summary")).select("nhspd_release").first()[0]
    write_json(
        out / "summary.json",
        {
            "generated_at": dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
            "run_id": args.run_id,
            "nhspd_release": release,
            "ods_snapshot": snapshot,
            "scope_lads": args.synthetic_lads.split(","),
            "disclosure": {"suppress_below": disclosure.THRESHOLD, "round_to": disclosure.BASE},
            "icb_summary": icb,
            "pipeline_metrics": metrics,
        },
    )
    print(f"[publish] {len(rows)} LSOAs, {len(prows)} practices -> {out}")


if __name__ == "__main__":
    main()
