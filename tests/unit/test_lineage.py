"""Pinned reads: a step sees one snapshot per input and records the versions it used."""

from gbdemo.lineage import PinnedReader, current_version


def test_pinned_read_ignores_later_writes_and_reproduces(spark):
    t = "lineage_regs"
    spark.createDataFrame([(1,), (2,)], "id int").write.format("delta").mode("overwrite").saveAsTable(t)

    reader = PinnedReader(spark)
    pinned = reader.read(t, "regs")
    v = reader.versions()["input_version.regs"]
    assert v == current_version(spark, t)

    # another job appends after the step started: the pinned read still sees 2 rows
    spark.createDataFrame([(3,)], "id int").write.format("delta").mode("append").saveAsTable(t)
    assert pinned.count() == 2
    assert spark.table(t).count() == 3

    # and the recorded version rebuilds exactly what the step read
    assert spark.sql(f"SELECT * FROM {t} VERSION AS OF {v}").count() == 2


def test_output_versions_recorded(spark):
    t = "lineage_out"
    spark.createDataFrame([(1,)], "id int").write.format("delta").mode("overwrite").saveAsTable(t)
    reader = PinnedReader(spark)
    v = reader.wrote(t, "out")
    assert reader.versions() == {"output_version.out": v}
