"""Gold documentation: complete, applied to the catalog, and the markdown copy is up to date."""

from pathlib import Path

from gbdemo import catalog_docs, gold

ADDED_ON_SAVE = ["nhspd_release", "_run_id"]  # pipelines/gold_aggregate.py adds these


def gold_outputs(spark):
    """Each gold builder run on minimal input: {table: output columns}."""
    census = spark.createDataFrame([("EX1 1HS", 20)], "postcode string, residents int")
    pcs = spark.createDataFrame(
        [("EX1 1HS", "A", "M", "E07000041")],
        "postcode string, lsoa21_code string, msoa21_code string, lad_code string",
    )
    regs = spark.createDataFrame(
        [("A", "M", "P1", "00-04", "I")],
        "lsoa21_code string, msoa21_code string, practice_code string, age_band string, icb_gss string",
    )
    names = lambda code, name: spark.createDataFrame([("A", "x")], f"{code} string, {name} string")  # noqa: E731
    practices = spark.createDataFrame(
        [("P1", "Practice", "EX1 1HS", "A")],
        "practice_code string, practice_name string, postcode string, lsoa21_code string",
    )
    res_icb = spark.createDataFrame([("I", 20)], "icb_gss string, residents int")
    icb_names = spark.createDataFrame([("I", "NHS X")], "icb_gss string, icb_name string")
    return {
        "lsoa_registered_vs_resident": gold.lsoa_registered_vs_resident(
            gold.residents_by_lsoa(census, pcs, ["E07000041"]), gold.registered_by_lsoa(regs),
            names("lsoa21_code", "lsoa21_name")),
        "msoa_registered_vs_resident": gold.msoa_registered_vs_resident(
            gold.residents_by_area(census, pcs, ["E07000041"], "msoa21_code"),
            gold.registered_by_area(regs, "msoa21_code"), names("msoa21_code", "msoa21_name")),
        "practice_profile": gold.practice_profile(regs, practices),
        "icb_summary": gold.icb_summary(regs, res_icb, icb_names),
    }


def test_every_gold_column_is_documented(spark):
    outputs = gold_outputs(spark)
    assert set(outputs) == set(catalog_docs.GOLD)
    for table, df in outputs.items():
        documented = list(catalog_docs.GOLD[table][1])
        assert documented == df.columns + ADDED_ON_SAVE, table


def test_comments_written_to_catalog(spark):
    t = "docs_icb"
    gold_outputs(spark)["icb_summary"].selectExpr("*", "'2026-08' AS nhspd_release", "'1' AS _run_id") \
        .write.format("delta").mode("overwrite").saveAsTable(t)
    catalog_docs.apply(spark, t, catalog_docs.GOLD["icb_summary"])

    columns = {r.col_name: r.comment for r in spark.sql(f"DESCRIBE TABLE {t}").collect()}
    assert columns["registered_per_resident"] == catalog_docs.RATIO
    table_comment = spark.sql(f"DESCRIBE TABLE EXTENDED {t}").where("col_name = 'Comment'").first()
    assert table_comment.data_type == catalog_docs.GOLD["icb_summary"][0]


def test_markdown_dictionary_up_to_date():
    path = Path(__file__).resolve().parents[2] / "docs" / "data_dictionary.md"
    assert path.read_text(encoding="utf-8") == catalog_docs.markdown(), (
        "run: python scripts/make_data_dictionary.py"
    )
