"""Layout builds every Unity Catalog name from one place."""
from gbdemo.config import Layout


def test_layout_names():
    lay = Layout("workspace", "dev_x_gbdemo_bronze", "dev_x_gbdemo_silver", "dev_x_gbdemo_gold")
    assert lay.bronze("nhspd") == "workspace.dev_x_gbdemo_bronze.nhspd"
    assert lay.gold("registered_vs_resident") == "workspace.dev_x_gbdemo_gold.registered_vs_resident"
    assert lay.landing_path == "/Volumes/workspace/dev_x_gbdemo_bronze/landing"


def test_runtime_parses_job_parameters():
    from gbdemo.runtime import parse_layout

    lay = parse_layout(["--catalog", "workspace", "--bronze-schema", "b", "--silver-schema", "s",
                        "--gold-schema", "g", "--unknown", "ignored"])
    got = (lay.catalog, lay.bronze_schema, lay.silver_schema, lay.gold_schema)
    assert got == ("workspace", "b", "s", "g")
