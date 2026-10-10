"""Job step: Disclosure-controlled aggregates: registered vs resident by LSOA, practice, ICB (gold)."""
from gbdemo import __version__
from gbdemo.runtime import check_schema, get_spark, parse_layout


def main() -> None:
    layout = parse_layout()
    spark = get_spark()
    check_schema(spark, layout.catalog, layout.gold_schema)
    target = f"{layout.catalog}.{layout.gold_schema}"
    print(f"[gold_aggregate] gbdemo {__version__} -> {target}: not implemented yet")


if __name__ == "__main__":
    main()
