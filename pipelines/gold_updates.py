"""Job step: protected monthly outputs from the incremental updates (gold).

Writes gold.registered_monthly (registered patients per district at each month end, read from
the effective-dated history) and gold.update_summary (per monthly file: events received, applied,
and quarantined by reason). Only protected values, like every gold table.
"""

from pyspark.sql import functions as F

from gbdemo import catalog_docs, incremental, reconcile
from gbdemo.runtime import check_schema, get_spark, parse_args, parse_layout


def main() -> None:
    args, layout = parse_args(), parse_layout()
    spark = get_spark()
    check_schema(spark, layout.catalog, layout.gold_schema)
    history = spark.table(layout.silver("patient_history"))
    updates = spark.table(layout.bronze("registration_updates"))
    quarantine = spark.table(layout.silver("registration_update_quarantine"))

    months = sorted(
        r.extract_month for r in updates.select("extract_month").distinct().collect() if r.extract_month
    )
    month_ends = [spark.sql(f"SELECT last_day(to_date('{m}-01')) AS d").first().d.isoformat() for m in months]
    outputs = {
        "registered_monthly": incremental.registered_by_month(history, month_ends),
        "update_summary": incremental.update_summary(updates, history, quarantine),
    }
    for name, df in outputs.items():
        (
            df.withColumn("_run_id", F.lit(args.run_id))
            .write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(layout.gold(name))
        )
        catalog_docs.apply(spark, layout.gold(name), catalog_docs.GOLD[name])

    reconcile.Recorder(spark, layout.silver("reconciliation"), args.run_id)(
        "gold_updates",
        args.month or "all",
        {f"{name}_rows": spark.table(layout.gold(name)).count() for name in outputs},
    )
    print(f"[gold_updates] months {months}")


if __name__ == "__main__":
    main()
