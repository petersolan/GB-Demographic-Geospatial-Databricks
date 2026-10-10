"""Bronze layer behaviour: names, malformed lines, GeoParquet, lineage, idempotent writes."""
import pandas as pd
import pytest
import shapely

from gbdemo import bronze


@pytest.mark.parametrize(("raw", "clean"), [
    ("\ufeffICB23CD", "icb_cd"),      # byte-order mark + vintage
    ("ICB26CD", "icb_cd"),            # different vintage, same name
    ("ICB26CDH", "icb_cdh"),          # ODS ("health") code
    ("LSOA21NMW", "lsoa_nmw"),        # Welsh name
    ("BNG_E", "bng_e"),               # ordinary name: just lower-cased
    (" ObjectId ", "objectid"),
])
def test_clean_column_name(raw, clean):
    assert bronze.clean_column_name(raw) == clean


def test_headerless_csv_names_strings_and_corrupt_lines(spark, tmp_path):
    f = tmp_path / "pc.csv"
    f.write_text('"EX1 1HS","198001","3"\n"PL1 1AA","199606","4"\n"BROKEN","1","2","extra"\n')
    df = bronze.read_headerless_csv(spark, str(f), ["pcds", "dointr", "osgrdind"])

    assert df.columns[:3] == ["pcds", "dointr", "osgrdind"]
    assert all(t == "string" for _, t in df.dtypes if _ != "file_path")
    rows = {r.pcds: r for r in df.collect()}
    assert rows["EX1 1HS"].dointr == "198001"        # leading zeros / formats kept as text
    assert rows["BROKEN"][bronze.CORRUPT] is not None  # malformed line captured, not dropped
    assert rows["EX1 1HS"][bronze.CORRUPT] is None


def test_read_geoparquet_renames(spark, tmp_path):
    f = tmp_path / "icb.parquet"
    pd.DataFrame({
        "ICB26CD": ["E54000037"], "ICB26NM": ["NHS Devon Integrated Care Board"],
        "geometry": [shapely.to_wkb(shapely.box(0, 0, 1, 1))],
    }).to_parquet(f)
    df = bronze.read_geoparquet(spark, str(f))
    assert {"icb_cd", "icb_nm", "geom_wkb", "file_path"} <= set(df.columns)
    assert shapely.from_wkb(df.first().geom_wkb).area == 1.0


def test_write_version_is_idempotent_and_versions_stack(spark, tmp_path):
    f = tmp_path / "pc.csv"
    f.write_text('"EX1 1HS"\n"PL1 1AA"\n')
    table = "default.bronze_idempotency_test"
    spark.sql(f"DROP TABLE IF EXISTS {table}")

    def load(version):
        df = bronze.read_headerless_csv(spark, str(f), ["pcds"])
        bronze.write_version(bronze.with_lineage(df, "nhspd", version, "run-1"), table, version)

    load("2025-08")
    load("2025-08")  # re-run: must replace, not duplicate
    load("2026-08")

    per_version = spark.sql(f"SELECT _version, count(*) n FROM {table} GROUP BY 1").collect()
    counts = {r._version: r.n for r in per_version}
    assert counts == {"2025-08": 2, "2026-08": 2}
    assert bronze.ingest_stats(spark, table, "2025-08") == {"rows": 2, "corrupt_rows": 0}
    src = spark.table(table).select("_source_file").first()[0]
    assert src.endswith("pc.csv")
    assert set(bronze.LINEAGE) <= set(spark.table(table).columns)
