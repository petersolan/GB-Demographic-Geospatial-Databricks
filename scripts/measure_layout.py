"""Measure what liquid clustering skips: run the same queries on the plain and clustered benchmark
tables (pipelines/benchmark_layout.py) on the SQL warehouse, then read files read / skipped,
bytes read and time from the query history.

Usage:
  python scripts/measure_layout.py --warehouse <id> --schema workspace.dev_<user>_gbdemo_silver

Each query text carries a unique comment, so the warehouse's result cache can't answer it.
Requires the Databricks CLI, authenticated (as for `databricks bundle deploy`).
"""

import argparse
import json
import shutil
import subprocess
import time
import uuid

CLI = shutil.which("databricks") or "databricks"
AREA, SNAPSHOT = "E07000041", 39  # Exeter; the latest of 40 snapshots
QUERIES = {
    "one area, all snapshots": f"lad_code = '{AREA}'",
    "one area, one snapshot": f"lad_code = '{AREA}' AND snapshot = {SNAPSHOT}",
    "one snapshot, all areas": f"snapshot = {SNAPSHOT}",
}
TABLES = ("layout_bench_plain", "layout_bench_clustered")
METRICS = ("read_files_count", "pruned_files_count", "read_bytes", "total_time_ms")


def api(method: str, path: str, body: dict | None = None) -> dict:
    args = [CLI, "api", method, path] + (["--json", json.dumps(body)] if body else [])
    return json.loads(subprocess.run(args, capture_output=True, text=True, check=True).stdout)


def run(warehouse: str, sql: str) -> str:
    body = {"warehouse_id": warehouse, "statement": sql, "wait_timeout": "50s"}
    r = api("post", "/api/2.0/sql/statements", body)
    if r["status"]["state"] != "SUCCEEDED":
        raise RuntimeError(f"{r['status']}: {sql}")
    return r["statement_id"]


def metrics(statement_id: str) -> dict:
    """Query-history metrics for one statement (they appear a few seconds after it finishes)."""
    for _ in range(30):
        res = api("get", "/api/2.0/sql/history/queries?max_results=50&include_metrics=true").get("res", [])
        hit = next((q for q in res if q["query_id"] == statement_id and q.get("metrics")), None)
        if hit and hit["metrics"].get("read_files_count") is not None:
            return {k: hit["metrics"].get(k) for k in METRICS}
        time.sleep(2)
    raise RuntimeError(f"no metrics for {statement_id}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--warehouse", required=True)
    ap.add_argument("--schema", required=True, help="catalog.schema of the silver layer")
    a = ap.parse_args()

    print("| Query | Table | Files read | Files skipped | MB read | Time (s) |")
    print("|---|---|---|---|---|---|")
    for label, where in QUERIES.items():
        for table in TABLES:
            sql = (f"/* measure_layout {uuid.uuid4()} */ SELECT count(*), sum(easting) "
                   f"FROM {a.schema}.{table} WHERE {where}")
            m = metrics(run(a.warehouse, sql))
            name = table.removeprefix("layout_bench_")
            mb, secs = m["read_bytes"] / 1e6, m["total_time_ms"] / 1e3
            print(f"| {label} | {name} | {m['read_files_count']} | {m['pruned_files_count']} | "
                  f"{mb:,.1f} | {secs:.1f} |", flush=True)


if __name__ == "__main__":
    main()
