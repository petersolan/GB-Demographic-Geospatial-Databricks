"""Copy the published exports (protected aggregates only) from Databricks into site/data/.

Usage: python scripts/fetch_site_data.py --volume /Volumes/workspace/dev_<user>_gbdemo_gold/exports
Needs the Databricks CLI on PATH (or DATABRICKS_CLI=<path to databricks.exe>).
In Git Bash, prefix with MSYS_NO_PATHCONV=1 so /Volumes/... is not rewritten as a Windows path.
"""

import argparse
import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FILES = ("lsoa.geojson", "practices.geojson", "summary.json")
CLI = os.environ.get("DATABRICKS_CLI", "databricks")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--volume", required=True)
    vol = ap.parse_args().volume.rstrip("/")
    if not vol.startswith("/Volumes/"):
        raise SystemExit(f"--volume must start with /Volumes/, got {vol!r}")
    out = ROOT / "site" / "data"
    out.mkdir(parents=True, exist_ok=True)
    for name in FILES:
        subprocess.run([CLI, "fs", "cp", f"dbfs:{vol}/{name}", str(out / name), "--overwrite"],
                       check=True, stdout=subprocess.DEVNULL)
        print(f"  {name}: {(out / name).stat().st_size / 1e3:,.0f} kB")


if __name__ == "__main__":
    main()
