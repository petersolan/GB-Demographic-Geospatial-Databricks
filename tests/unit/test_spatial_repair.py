"""The Shapely repair UDF runs anywhere (no ST_* functions), so it is unit-tested locally."""
import shapely

from gbdemo.spatial import make_valid_wkb


def test_make_valid_wkb_repairs_bowtie(spark):
    bowtie = shapely.from_wkt("POLYGON ((0 0, 10 10, 10 0, 0 10, 0 0))")
    square = shapely.box(0, 0, 1, 1)
    df = spark.createDataFrame([(shapely.to_wkb(bowtie),), (shapely.to_wkb(square),)], "wkb binary")
    fixed = [shapely.from_wkb(r.fixed) for r in df.select(make_valid_wkb("wkb").alias("fixed")).collect()]

    assert not bowtie.is_valid
    assert all(g.is_valid for g in fixed)
    assert fixed[0].geom_type == "MultiPolygon" and abs(fixed[0].area - 50.0) < 1e-9  # two triangles
    assert fixed[1].equals(square)                                                    # valid input unchanged
