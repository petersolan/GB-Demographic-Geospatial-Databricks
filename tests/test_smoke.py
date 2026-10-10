"""Smoke test: the local Spark + Delta setup works (write, append, time travel)."""


def test_delta_write_append_time_travel(spark, tmp_path):
    path = str(tmp_path / "people")
    spark.createDataFrame([("EX1 1HS", 12), ("PL1 1AA", 7)], ["postcode", "people"]) \
        .write.format("delta").save(path)
    spark.createDataFrame([("TQ1 1AA", 3)], ["postcode", "people"]) \
        .write.format("delta").mode("append").save(path)

    version_0 = spark.read.format("delta").option("versionAsOf", 0).load(path)
    latest = spark.read.format("delta").load(path)

    assert version_0.count() == 2
    assert latest.count() == 3
    assert latest.agg({"people": "sum"}).first()[0] == 22
