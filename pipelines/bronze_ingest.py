"""Job step: land raw reference data as bronze Delta tables (as received, with lineage).

Reads the manifests uploaded with the data, so the job loads exactly the versions that were
downloaded (see scripts/download_reference_data.py). Re-running replaces each version in place.
"""
import json
import re
import shutil
import zipfile
from pathlib import Path

from gbdemo import bronze
from gbdemo.runtime import check_schema, get_spark, parse_args, parse_layout
from gbdemo.schemas import EPRACCUR_FIELDS, NHSPD_FIELDS

NHSPD_MEMBER = re.compile(r"^Data/nhg\d{2}[a-z]{3}\.csv$")  # national file, e.g. Data/nhg26aug.csv
BOUNDARY_TABLES = {
    "lsoa21_bfc": "lsoa21_boundary",
    "msoa21_bfc": "msoa21_boundary",
    "icb_bfc": "icb_boundary",
}


def extract_nhspd(zip_path: Path) -> Path:
    """Unzip the national NHSPD CSV next to the zip, once. Zips aren't splittable; CSVs are."""
    out = zip_path.parent / "extracted" / "nhspd.csv"
    with zipfile.ZipFile(zip_path) as z:
        member = next(m for m in z.infolist() if NHSPD_MEMBER.match(m.filename))
        if out.exists() and out.stat().st_size == member.file_size:
            print(f"  already extracted: {out}")
            return out
        out.parent.mkdir(parents=True, exist_ok=True)
        with z.open(member) as src, open(out, "wb") as dst:
            shutil.copyfileobj(src, dst, length=16 << 20)
    print(f"  extracted {member.filename} -> {out} ({out.stat().st_size / 1e6:,.0f} MB)")
    return out


def main() -> None:
    args, layout = parse_args(), parse_layout()
    spark = get_spark()
    check_schema(spark, layout.catalog, layout.bronze_schema)
    landing = Path(layout.landing_path)
    manifest = json.loads((landing / "manifest.json").read_text())
    prepared = json.loads((landing / "prepare_report.json").read_text())
    log = []

    def load(df, dataset: str, version: str, table: str, sha: str) -> None:
        target = layout.bronze(table)
        bronze.write_version(bronze.with_lineage(df, dataset, version, args.run_id), target, version)
        stats = bronze.ingest_stats(spark, target, version)
        log.append({"run_id": args.run_id, "dataset": dataset, "version": version,
                    "target_table": target, "source_sha256": sha, **stats})
        print(f"  {dataset} {version} -> {target}: {stats['rows']:,} rows, "
              f"{stats['corrupt_rows']} malformed", flush=True)

    for key, meta in manifest.items():
        ds, ver = meta["dataset"], meta["version"]
        path = landing / key
        if ds == "nhspd":
            csv = extract_nhspd(path)
            load(bronze.read_headerless_csv(spark, str(csv), NHSPD_FIELDS), ds, ver, "nhspd", meta["sha256"])
        elif ds == "ods_epraccur":
            df = bronze.read_headerless_csv(spark, str(path), EPRACCUR_FIELDS)
            load(df, ds, ver, "ods_epraccur", meta["sha256"])
        elif ds == "icb_codes":
            load(bronze.read_headed_csv(spark, str(path)), ds, ver, "icb_codes", meta["sha256"])
        elif ds in BOUNDARY_TABLES:
            # boundaries arrive as GeoParquet made from the downloaded GeoPackage (same geometry)
            parquet = landing / Path(prepared[key]["output"]).relative_to("data/prepared")
            load(bronze.read_geoparquet(spark, str(parquet)), ds, ver, BOUNDARY_TABLES[ds], meta["sha256"])

    bronze.log_ingest(spark, layout.bronze("ingest_log"), log)
    print(f"[bronze_ingest] {len(log)} dataset versions loaded, run {args.run_id}")


if __name__ == "__main__":
    main()
