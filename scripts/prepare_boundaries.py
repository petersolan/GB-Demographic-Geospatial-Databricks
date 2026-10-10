"""Convert downloaded boundary GeoPackages to GeoParquet for Databricks (which has no GeoPackage reader).

Geometry stays exactly as published (British National Grid, EPSG:27700): invalid polygons are
counted and reported here but NOT repaired; repair is a visible, counted step in the silver layer.
Output: data/prepared/<dataset>/<version>/<dataset>.parquet + data/prepared/prepare_report.json

Usage: python scripts/prepare_boundaries.py
"""
import json
from pathlib import Path

import pyogrio
import shapely

ROOT = Path(__file__).resolve().parents[1]
LANDING = ROOT / "data" / "landing"
PREPARED = ROOT / "data" / "prepared"
BOUNDARIES = ("lsoa21_bfc", "msoa21_bfc", "icb_bfc")


def main() -> None:
    manifest = json.loads((LANDING / "manifest.json").read_text())
    report = {}
    for key, meta in manifest.items():
        if meta["dataset"] not in BOUNDARIES:
            continue
        src = LANDING / key
        gdf = pyogrio.read_dataframe(src, use_arrow=True)
        epsg = gdf.crs.to_epsg() if gdf.crs else None
        if epsg != 27700:
            raise ValueError(f"{key}: expected EPSG:27700, got {gdf.crs}")

        geoms = gdf.geometry.values
        invalid = ~shapely.is_valid(geoms)
        reasons = {}
        for reason in shapely.is_valid_reason(geoms[invalid]):
            kind = reason.split("[")[0].strip()
            reasons[kind] = reasons.get(kind, 0) + 1

        out = PREPARED / meta["dataset"] / meta["version"] / f"{meta['dataset']}.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)
        gdf.to_parquet(out, compression="zstd", geometry_encoding="WKB",
                       schema_version="1.1.0", write_covering_bbox=True)

        report[key] = {
            "features": len(gdf), "epsg": epsg, "columns": list(gdf.columns),
            "invalid_geometries": int(invalid.sum()), "invalid_reasons": reasons,
            "vertices": int(shapely.get_num_coordinates(geoms).sum()),
            "gpkg_mb": round(src.stat().st_size / 1e6, 1), "parquet_mb": round(out.stat().st_size / 1e6, 1),
            "output": str(out.relative_to(ROOT)).replace("\\", "/"),
        }
        r = report[key]
        print(f"{key:<38} {r['features']:>6} features  {r['vertices']:>11,} vertices  "
              f"invalid {r['invalid_geometries']:>3} {reasons or ''}  "
              f"{r['gpkg_mb']} MB -> {r['parquet_mb']} MB", flush=True)
    (PREPARED / "prepare_report.json").write_text(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
