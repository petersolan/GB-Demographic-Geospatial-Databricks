"""Job step: Standardise and validate postcodes, join to NHSPD, quarantine failures (silver)."""
from gbdemo import __version__
from gbdemo.runtime import check_schema, get_spark, parse_layout


def main() -> None:
    layout = parse_layout()
    spark = get_spark()
    check_schema(spark, layout.catalog, layout.silver_schema)
    target = f"{layout.catalog}.{layout.silver_schema}"
    print(f"[silver_postcodes] gbdemo {__version__} -> {target}: not implemented yet")


if __name__ == "__main__":
    main()
