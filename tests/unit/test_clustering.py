"""Liquid clustering: keys are set idempotently and data is unchanged by OPTIMIZE."""

from gbdemo.clustering import cluster, clustering_columns


def test_cluster_sets_keys_and_keeps_rows(spark):
    t = "layout_postcode"
    rows = [("2026-08", f"E0700004{i % 3}", f"EX{i} 1AA") for i in range(30)]
    df = spark.createDataFrame(rows, "nhspd_release string, lad_code string, postcode string")
    df.write.format("delta").mode("overwrite").saveAsTable(t)

    cluster(spark, t, ["nhspd_release", "lad_code"])
    cluster(spark, t, ["nhspd_release", "lad_code"])  # re-runs are harmless

    assert clustering_columns(spark, t) == ["nhspd_release", "lad_code"]
    assert spark.table(t).count() == 30
    assert spark.table(t).subtract(df).count() == 0
