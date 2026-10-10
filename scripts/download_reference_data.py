"""Download the open reference data the pipeline is built from, with provenance.

Files land in data/landing/<dataset>/<version>/ (versions side by side, never overwritten)
and data/landing/manifest.json records source, SHA-256, size and retrieval time for each.
Databricks Free Edition has no general outbound internet, so data is fetched here and
uploaded to the Unity Catalog volume afterwards (scripts/upload_to_volume.ps1).

Usage: python scripts/download_reference_data.py [dataset ...]
"""
import datetime as dt
import hashlib
import json
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LANDING = ROOT / "data" / "landing"
UA = {"User-Agent": "GB-Demographic-Geospatial-Databricks (portfolio project)"}

ARCGIS_ITEM = "https://www.arcgis.com/sharing/rest/content/items/{id}/data"
HUB_EXPORT = "https://hub.arcgis.com/api/download/v1/items/{id}/geoPackage?redirect=false&layers=0"
ODS_EPRACCUR = "https://www.odsdatasearchandexport.nhs.uk/api/getReport?report=epraccur"

# dataset, version, how to fetch, source id/url, file name, description
SOURCES = [
    ("nhspd", "2025-08", "arcgis_item", "402152391b1f459598a8ce5bed1b6cf8", "NHSPD_AUG_2025_UK_FULL.zip",
     "NHS Postcode Directory UK Full, August 2025 (incl. terminated postcodes)"),
    ("nhspd", "2026-08", "arcgis_item", "55d0b9a92ed940f887dfd9a9402be0c8", "NHSPD_AUG_2026_UK_FULL.zip",
     "NHS Postcode Directory UK Full, August 2026 (incl. terminated postcodes)"),
    ("lsoa21_bfc", "v10", "hub_gpkg", "2bbaef5230694f3abae4f9145a3a9800", "lsoa21_bfc.gpkg",
     "LSOA (December 2021) Boundaries EW BFC V10, full resolution clipped, BNG"),
    ("msoa21_bfc", "v7", "hub_gpkg", "12baf1e6a44441208ffe5ba5ed063a68", "msoa21_bfc.gpkg",
     "MSOA (December 2021) Boundaries EW BFC V7, full resolution clipped, BNG"),
    ("icb_bfc", "2023-04", "hub_gpkg", "da81300d4b624a0b81376416c8b5d90e", "icb_2023_04_bfc.gpkg",
     "Integrated Care Boards (April 2023) Boundaries EN BFC, BNG"),
    ("icb_bfc", "2026-04", "hub_gpkg", "56ea2366d7274cefb33dd2d83e9f6a10", "icb_2026_04_bfc.gpkg",
     "Integrated Care Boards (April 2026) Boundaries EN BFC, BNG"),
    # ODS publishes only the current list: our dated snapshots ARE the version history
    ("ods_epraccur", dt.date.today().isoformat(), "url", ODS_EPRACCUR, "epraccur.csv",
     "ODS GP practices (epraccur), current snapshot on download date; no header row"),
]


def fetch(url: str, dest: Path) -> None:
    tmp = dest.with_suffix(dest.suffix + ".part")
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=600) as r, open(tmp, "wb") as f:
        while chunk := r.read(1 << 20):
            f.write(chunk)
    tmp.replace(dest)  # only a complete download gets the real name


def hub_export_url(item_id: str, attempts: int = 60) -> str:
    """ArcGIS Hub builds export files on request: poll until it reports Completed."""
    for _ in range(attempts):
        req = urllib.request.Request(HUB_EXPORT.format(id=item_id), headers=UA)
        with urllib.request.urlopen(req, timeout=120) as r:
            body = json.load(r)
        if body.get("status") == "Completed" and body.get("resultUrl"):
            return body["resultUrl"]
        print(f"    export {body.get('status', '?')}, waiting...", flush=True)
        time.sleep(10)
    raise TimeoutError(f"Hub export for {item_id} not ready")


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(1 << 20):
            h.update(chunk)
    return h.hexdigest()


def main(only: list[str]) -> None:
    manifest_path = LANDING / "manifest.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    for dataset, version, kind, src, name, desc in SOURCES:
        if only and dataset not in only:
            continue
        dest = LANDING / dataset / version / name
        key = f"{dataset}/{version}/{name}"
        if dest.exists() and key in manifest:
            print(f"skip  {key} (already downloaded)")
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        print(f"get   {key}", flush=True)
        url = (ARCGIS_ITEM.format(id=src) if kind == "arcgis_item"
               else hub_export_url(src) if kind == "hub_gpkg" else src)
        fetch(url, dest)
        manifest[key] = {
            "dataset": dataset, "version": version, "description": desc,
            "source": src if kind == "url" else f"arcgis item {src}",
            "bytes": dest.stat().st_size, "sha256": sha256(dest),
            "retrieved_at": dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
        }
        manifest_path.write_text(json.dumps(manifest, indent=2))
        print(f"      {dest.stat().st_size / 1e6:,.1f} MB", flush=True)


if __name__ == "__main__":
    main(sys.argv[1:])
