"""Upload landed + prepared files to the Unity Catalog landing volume, keeping <dataset>/<version>/ folders.

Raw sources go up as received (NHSPD zips, ODS CSV); boundaries go up as the GeoParquet made by
prepare_boundaries.py. manifest.json and prepare_report.json travel with the data (provenance).

Usage: python scripts/upload_to_volume.py --volume /Volumes/workspace/dev_<user>_gbdemo_bronze/landing
Needs the Databricks CLI on PATH (or DATABRICKS_CLI=<path to databricks.exe>).
"""
import argparse
import json
import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LANDING = ROOT / "data" / "landing"
PREPARED = ROOT / "data" / "prepared"
RAW_AS_RECEIVED = ("nhspd", "ods_epraccur")
CLI = os.environ.get("DATABRICKS_CLI", "databricks")


def cp(local: Path, remote: str) -> None:
    # volumes need the folder to exist first; fs mkdir creates missing parents like mkdir -p
    subprocess.run([CLI, "fs", "mkdir", f"dbfs:{remote.rsplit('/', 1)[0]}"], check=True,
                   stdout=subprocess.DEVNULL)
    subprocess.run([CLI, "fs", "cp", str(local), f"dbfs:{remote}", "--overwrite"], check=True,
                   stdout=subprocess.DEVNULL)
    print(f"  {local.stat().st_size / 1e6:8.1f} MB  {remote}", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--volume", required=True, help="e.g. /Volumes/workspace/dev_x_gbdemo_bronze/landing")
    vol = ap.parse_args().volume.rstrip("/")
    if not vol.startswith("/Volumes/"):
        # Git Bash rewrites /Volumes/... into C:/Program Files/Git/Volumes/... (MSYS path
        # conversion); run with MSYS_NO_PATHCONV=1 or from PowerShell
        raise SystemExit(f"--volume must start with /Volumes/, got {vol!r}")

    manifest = json.loads((LANDING / "manifest.json").read_text())
    for key, meta in manifest.items():
        if meta["dataset"] in RAW_AS_RECEIVED:
            cp(LANDING / key, f"{vol}/{key}")
    report = json.loads((PREPARED / "prepare_report.json").read_text())
    for meta in report.values():
        local = ROOT / meta["output"]
        cp(local, f"{vol}/{local.relative_to(PREPARED).as_posix()}")
    cp(LANDING / "manifest.json", f"{vol}/manifest.json")
    cp(PREPARED / "prepare_report.json", f"{vol}/prepare_report.json")


if __name__ == "__main__":
    main()
