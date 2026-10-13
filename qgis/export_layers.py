r"""Export the pipeline's inputs, checks and outputs to one GeoPackage for exploring in QGIS.

Writes data/qgis/gbdemo.gpkg (British National Grid, EPSG:27700); then build the styled project
with qgis/build_project.py. Run from the repo root with a Python that has geopandas:

    python qgis/export_layers.py              # dev schemas (dev_<user>_gbdemo_*)
    python qgis/export_layers.py --prod       # prod schemas (gbdemo_*)

Sources: boundaries from data/prepared (scripts/prepare_boundaries.py); everything else from the
Databricks tables via the SQL Statement Execution API (Databricks CLI, default profile).
Only gold (disclosure-controlled) outputs and public reference data (postcodes, practices) are
exported: no record-level patient data, synthetic or not.
"""

import argparse
import json
import os
import subprocess
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pyarrow.parquet as pq
from shapely import wkb

ROOT = Path(__file__).resolve().parents[1]
PREPARED = ROOT / "data" / "prepared"
OUT = ROOT / "data" / "qgis" / "gbdemo.gpkg"
CLI = os.environ.get("DATABRICKS_CLI", "databricks")
BNG = "EPSG:27700"
STUDY_LADS = {
    "E07000040": "East Devon",
    "E07000041": "Exeter",
    "E07000042": "Mid Devon",
    "E07000045": "Teignbridge",
}


# ---------------------------------------------------------------- Databricks SQL


def cli(*args: str) -> dict:
    out = subprocess.run([CLI, *args, "-o", "json"], capture_output=True, text=True, check=True).stdout
    return json.loads(out)


def warehouse_id() -> str:
    whs = cli("warehouses", "list")
    return (whs if isinstance(whs, list) else whs.get("warehouses", []))[0]["id"]


def sql(statement: str, wid: str) -> pd.DataFrame:
    """Run a statement and return all rows (follows result chunks), typed by the result schema."""
    body = {
        "warehouse_id": wid,
        "statement": statement,
        "wait_timeout": "50s",
        "disposition": "INLINE",
        "format": "JSON_ARRAY",
    }
    d = json.loads(
        subprocess.run(
            [CLI, "api", "post", "/api/2.0/sql/statements", "--json", json.dumps(body)],
            capture_output=True,
            text=True,
            check=True,
        ).stdout
    )
    while d["status"]["state"] in ("PENDING", "RUNNING"):
        d = json.loads(
            subprocess.run(
                [CLI, "api", "get", f"/api/2.0/sql/statements/{d['statement_id']}"],
                capture_output=True,
                text=True,
                check=True,
            ).stdout
        )
    if d["status"]["state"] != "SUCCEEDED":
        raise RuntimeError(d["status"])
    schema = d["manifest"]["schema"]["columns"]
    rows = list(d.get("result", {}).get("data_array", []))
    for i in range(1, d["manifest"].get("total_chunk_count", 1)):
        chunk = json.loads(
            subprocess.run(
                [CLI, "api", "get", f"/api/2.0/sql/statements/{d['statement_id']}/result/chunks/{i}"],
                capture_output=True,
                text=True,
                check=True,
            ).stdout
        )
        rows += chunk.get("data_array", [])
    df = pd.DataFrame(rows, columns=[c["name"] for c in schema])
    for c in schema:  # JSON_ARRAY returns strings: restore numbers and booleans
        t = c["type_name"]
        if t in ("INT", "LONG", "SHORT", "BYTE"):
            df[c["name"]] = pd.to_numeric(df[c["name"]]).astype("Int64")
        elif t in ("DOUBLE", "FLOAT", "DECIMAL"):
            df[c["name"]] = pd.to_numeric(df[c["name"]])
        elif t == "BOOLEAN":
            df[c["name"]] = df[c["name"]].map({"true": True, "false": False})
    return df


def points(df: pd.DataFrame, x: str = "easting", y: str = "northing") -> gpd.GeoDataFrame:
    df = df.dropna(subset=[x, y])
    return gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df[x], df[y]), crs=BNG)


# ---------------------------------------------------------------- local boundaries


def boundaries(dataset: str, version: str, code: str, keep: set[str] | None) -> gpd.GeoDataFrame:
    path = next((PREPARED / dataset / version).glob("*.parquet"))
    table = pq.read_table(path, filters=[(code, "in", sorted(keep))] if keep else None)
    df = table.to_pandas()
    return gpd.GeoDataFrame(
        df.drop(columns="geometry"), geometry=df["geometry"].map(lambda b: wkb.loads(bytes(b))), crs=BNG
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--prod", action="store_true", help="read the prod schemas instead of dev")
    a = ap.parse_args()
    user = cli("current-user", "me")["userName"].split("@")[0]
    prefix = "" if a.prod else f"dev_{user}_"
    bronze, silver, gold = (f"workspace.{prefix}gbdemo_{x}" for x in ("bronze", "silver", "gold"))
    wid = warehouse_id()
    lads = ", ".join(f"'{c}'" for c in STUDY_LADS)
    layers: dict[str, gpd.GeoDataFrame] = {}
    print(f"reading {prefix or 'prod '}schemas via warehouse {wid}")

    # ------------------------------------------------ inputs (reference data)
    pc = sql(
        f"""
        SELECT p.postcode, p.lad_code, p.lsoa21_code, p.msoa21_code, p.icb_name, p.is_live,
               p.grid_quality, CAST(p.date_introduced AS STRING) AS date_introduced,
               CAST(p.date_terminated AS STRING) AS date_terminated,
               coalesce(c.residents, 0) AS census_residents, p.easting, p.northing
        FROM {silver}.postcode p
        LEFT JOIN (SELECT upper(regexp_replace(postcode, '\\\\s+', '')) AS k,
                          sum(CAST(total AS INT)) AS residents
                   FROM {bronze}.census_postcode_population GROUP BY 1) c
          ON c.k = replace(p.postcode, ' ', '')
        WHERE p.nhspd_release = (SELECT max(nhspd_release) FROM {silver}.postcode)
          AND p.lad_code IN ({lads})""",
        wid,
    )
    pc["district"] = pc["lad_code"].map(STUDY_LADS)
    layers["in_postcodes"] = points(pc)
    study_lsoas = set(pc["lsoa21_code"].dropna())
    study_msoas = set(pc["msoa21_code"].dropna())

    gp = sql(
        f"""
        SELECT practice_code, practice_name, postcode, status, prescribing_setting,
               prescribing_setting = 'RO76' AS is_gp_practice, lsoa21_code, easting, northing
        FROM {silver}.gp_practice
        WHERE ods_snapshot = (SELECT max_by(ods_snapshot, snapshot_date) FROM {silver}.gp_practice)
          AND status = 'ACTIVE' AND postcode_icb_gss = 'E54000037'""",
        wid,
    )
    layers["in_ods_organisations"] = points(gp)

    lsoa = boundaries("lsoa21_bfc", "v10", "LSOA21CD", study_lsoas).rename(
        columns={"LSOA21CD": "lsoa21_code", "LSOA21NM": "lsoa21_name"}
    )[["lsoa21_code", "lsoa21_name", "geometry"]]
    lsoa["lad_code"] = lsoa["lsoa21_code"].map(
        pc.groupby("lsoa21_code")["lad_code"].agg(lambda s: s.mode()[0])
    )
    lsoa["district"] = lsoa["lad_code"].map(STUDY_LADS)
    layers["in_lsoa21"] = lsoa
    msoa = boundaries("msoa21_bfc", "v7", "MSOA21CD", study_msoas).rename(
        columns={"MSOA21CD": "msoa21_code", "MSOA21NM": "msoa21_name"}
    )[["msoa21_code", "msoa21_name", "geometry"]]
    layers["in_msoa21"] = msoa
    for ver, code, name in (("2023-04", "ICB23CD", "ICB23NM"), ("2026-04", "ICB26CD", "ICB26NM")):
        icb = boundaries("icb_bfc", ver, code, None).rename(columns={code: "icb_gss", name: "icb_name"})
        icb["geometry"] = icb.geometry.simplify(25)  # whole of England: 25 m is plenty for viewing
        layers[f"in_icb_{ver[:4]}"] = icb[["icb_gss", "icb_name", "geometry"]]

    # ------------------------------------------------ checks (silver spatial QA on public postcodes)
    qa = sql(
        f"""
        SELECT postcode, nhspd_release, lsoa_reference, lsoa_spatial, lsoa_resolution, lsoa_agrees,
               grid_quality, is_live, easting, northing
        FROM {silver}.postcode_geo WHERE lsoa_resolution = 'no_match' OR NOT lsoa_agrees""",
        wid,
    )
    layers["check_no_match"] = points(
        qa[(qa.lsoa_resolution == "no_match") & (qa.nhspd_release == qa.nhspd_release.max())]
    )
    layers["check_disagreements_2025"] = points(
        qa[(qa.lsoa_resolution != "no_match") & (qa.nhspd_release == "2025-08")]
    )

    # ------------------------------------------------ outputs (gold: disclosure-controlled)
    reg = sql(
        f"SELECT * EXCEPT (_run_id) FROM {gold}.lsoa_registered_vs_resident WHERE lad_code IS NOT NULL", wid
    )
    cal = sql(
        f"""SELECT lsoa21_code, registered_synthetic, registered_nhs, synthetic_per_resident,
                  nhs_per_resident, postcodes_since_census, nhs_extract
           FROM {gold}.lsoa_calibration""",
        wid,
    )
    out_lsoa = (
        lsoa[["lsoa21_code", "lsoa21_name", "district", "geometry"]]
        .merge(reg.drop(columns=["lsoa21_name", "lad_code"]), on="lsoa21_code", how="left")
        .merge(cal, on="lsoa21_code", how="left")
    )
    layers["out_lsoa"] = out_lsoa
    mreg = sql(
        f"SELECT * EXCEPT (_run_id) FROM {gold}.msoa_registered_vs_resident WHERE lad_code IS NOT NULL", wid
    )
    layers["out_msoa"] = msoa.merge(mreg.drop(columns=["msoa21_name"]), on="msoa21_code", how="inner")

    pcal = sql(
        f"""SELECT practice_code, registered_synthetic, registered_nhs,
                  registered_synthetic - registered_nhs AS synthetic_minus_real
           FROM {gold}.practice_calibration""",
        wid,
    )
    layers["out_practices"] = points(gp[gp.is_gp_practice].merge(pcal, on="practice_code", how="inner"))

    icb_sum = sql(
        f"SELECT icb_gss, residents, registered, registered_per_resident FROM {gold}.icb_summary", wid
    )
    layers["out_icb"] = layers["in_icb_2026"].merge(icb_sum, on="icb_gss", how="inner")

    monthly = sql(
        f"SELECT CAST(month_end AS STRING) AS month_end, lad_code, registered FROM {gold}.registered_monthly",
        wid,
    )
    wide = monthly[monthly.lad_code.isin(list(STUDY_LADS))].pivot(
        index="lad_code", columns="month_end", values="registered"
    )
    wide.columns = [f"registered_{c.replace('-', '_')}" for c in wide.columns]
    if len(wide.columns) > 1:
        wide["change_over_period"] = wide.iloc[:, -1] - wide.iloc[:, 0]
    districts = lsoa.dissolve(by="lad_code", as_index=False)[["lad_code", "geometry"]]
    districts["district"] = districts["lad_code"].map(STUDY_LADS)
    layers["out_districts_monthly"] = districts.merge(wide.reset_index(), on="lad_code", how="left")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.unlink(missing_ok=True)
    for name, gdf in layers.items():
        gdf.to_file(OUT, layer=name, driver="GPKG")
        print(f"  {name:<28} {len(gdf):>7,} features")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
