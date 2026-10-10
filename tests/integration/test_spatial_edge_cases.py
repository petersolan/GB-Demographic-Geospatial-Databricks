"""Native Databricks spatial behaviour on hand-made geometry with known answers (runs on Databricks)."""
import shapely
from pyspark.sql import functions as F

from gbdemo import spatial

POLY_SCHEMA = ("code string, name string, _version string, geom_wkb binary, "
               "bbox struct<xmin:double, ymin:double, xmax:double, ymax:double>")
SQUARE_A = "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"
SQUARE_B = "POLYGON ((10 0, 20 0, 20 10, 10 10, 10 0))"   # shares the x=10 edge with A
OVERLAP_C = "POLYGON ((5 5, 15 5, 15 15, 5 15, 5 5))"     # overlaps A and B
BOWTIE = "POLYGON ((0 0, 10 10, 10 0, 0 10, 0 0))"        # self-intersecting (invalid)


def polys(spark, rows):
    data = []
    for code, wkt in rows:
        g = shapely.from_wkt(wkt)
        xmin, ymin, xmax, ymax = g.bounds
        data.append((code, f"name {code}", "test", shapely.to_wkb(g),
                     {"xmin": xmin, "ymin": ymin, "xmax": xmax, "ymax": ymax}))
    return spatial.boundaries(spark.createDataFrame(data, POLY_SCHEMA), "code", "name")


def points(spark, rows):
    schema = "postcode string, easting int, northing int, grid_quality int, ref string"
    df = spark.createDataFrame(rows, schema)
    return spatial.postcode_points(df)


def test_boundaries_repair_and_srid(spark):
    out = {r.code: r for r in polys(spark, [("A", SQUARE_A), ("BOW", BOWTIE)])
           .select("code", "was_valid", "is_valid", F.expr("ST_SRID(geom)").alias("srid")).collect()}
    assert out["A"].was_valid and out["A"].is_valid
    assert not out["BOW"].was_valid and out["BOW"].is_valid      # repaired by the Shapely UDF
    assert {r.srid for r in out.values()} == {27700}


def test_assign_edge_cases(spark):
    g = polys(spark, [("A", SQUARE_A), ("B", SQUARE_B)])
    p = points(spark, [
        ("INSIDE", 5, 5, 1, "A"),
        ("EDGE", 10, 5, 1, "B"),        # on the shared edge: covered by A and B
        ("CORNER", 10, 10, 1, "X"),     # shared corner, reference not a candidate
        ("OUTSIDE", 25, 5, 1, "A"),
        ("DISAGREE", 15, 5, 1, "A"),    # spatially in B, reference says A
        ("NOGRID", 5, 5, 9, "A"),       # grid quality 9: no usable location, excluded
    ])
    out = {r.postcode: r for r in spatial.assign(p, g, ["postcode"], "ref", "x").collect()}

    assert set(out) == {"INSIDE", "EDGE", "CORNER", "OUTSIDE", "DISAGREE"}
    inside = out["INSIDE"]
    assert (inside.x_spatial, inside.x_resolution, inside.x_agrees) == ("A", "single", True)
    assert (out["EDGE"].x_candidates, out["EDGE"].x_spatial, out["EDGE"].x_resolution) == \
        (2, "B", "multi_prefer_reference")
    assert (out["CORNER"].x_spatial, out["CORNER"].x_resolution) == ("A", "multi_lowest_code")
    assert out["CORNER"].x_agrees is False
    assert (out["OUTSIDE"].x_candidates, out["OUTSIDE"].x_spatial, out["OUTSIDE"].x_resolution) == \
        (0, None, "no_match")
    assert (out["DISAGREE"].x_spatial, out["DISAGREE"].x_agrees) == ("B", False)


def test_overlapping_polygons(spark):
    g = polys(spark, [("A", SQUARE_A), ("C", OVERLAP_C)])
    out = spatial.assign(points(spark, [("OVERLAP", 7, 7, 1, None)]), g, ["postcode"], "ref", "x").first()
    assert (out.x_candidates, out.x_spatial, out.x_resolution) == (2, "A", "multi_lowest_code")
