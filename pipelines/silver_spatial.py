"""Job step: Point-in-polygon joins in BNG with edge-case handling and reconciliation (silver)."""
from gbdemo import __version__
from gbdemo.runtime import check_schema, get_spark, parse_layout


def main() -> None:
    layout = parse_layout()
    spark = get_spark()
    check_schema(spark, layout.catalog, layout.silver_schema)
    target = f"{layout.catalog}.{layout.silver_schema}"
    print(f"[silver_spatial] gbdemo {__version__} -> {target}: not implemented yet")


if __name__ == "__main__":
    main()
