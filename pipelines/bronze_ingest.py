"""Job step: Land raw reference data and synthetic registrations as Delta tables (bronze)."""
from gbdemo import __version__
from gbdemo.runtime import check_schema, get_spark, parse_layout


def main() -> None:
    layout = parse_layout()
    spark = get_spark()
    check_schema(spark, layout.catalog, layout.bronze_schema)
    target = f"{layout.catalog}.{layout.bronze_schema}"
    print(f"[bronze_ingest] gbdemo {__version__} -> {target}: not implemented yet")


if __name__ == "__main__":
    main()
