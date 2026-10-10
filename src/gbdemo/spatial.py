"""The thin spatial layer: everything that needs Databricks native ST_* functions.

Job ad: "Implement spatial joins and point-in-polygon matching, handling edge cases and
multiple matches"; "CRS (BNG, WGS84), geometry validation".
Joins run in British National Grid (EPSG:27700): Databricks ST_Transform matches PROJ
only to ~1.5-2 m, so points are not re-projected before point-in-polygon tests.
ST_Covers (not ST_Contains) is used so points exactly on a boundary still match.
Invalid boundaries are repaired with Shapely (Databricks has no ST_MakeValid).

The ST_* expressions only run on Databricks (integration tests: tests/integration). The repair
UDF is plain Shapely and is unit-tested locally.
"""
import pandas as pd
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import BinaryType

BNG = 27700


# BinaryType() rather than "binary": a type string is parsed at import time, which needs a
# running Spark session; a type object does not
@pandas_udf(BinaryType())
def make_valid_wkb(wkb: pd.Series) -> pd.Series:
    """Repair invalid polygons (self-intersections etc.) with Shapely's make_valid."""
    import shapely

    fixed = shapely.make_valid(shapely.from_wkb(wkb.to_numpy()))
    return pd.Series(list(shapely.to_wkb(fixed)), index=wkb.index)



def simplify_wkb(tolerance_m: float):
    """pandas UDF factory: simplify polygons (in BNG metres) for web display, keeping them valid.

    Simplify in a projected CRS (metres) before re-projecting to WGS84 for the web: a tolerance in
    degrees would distort shapes unevenly. Databricks has no ST_Simplify, so Shapely does it.
    """

    @pandas_udf(BinaryType())
    def _simplify(wkb: pd.Series) -> pd.Series:
        import shapely

        geoms = shapely.simplify(shapely.from_wkb(wkb.to_numpy()), tolerance_m, preserve_topology=True)
        return pd.Series(list(shapely.to_wkb(geoms)), index=wkb.index)

    return _simplify

def boundaries(df: DataFrame, code_col: str, name_col: str) -> DataFrame:
    """Bronze boundary rows -> code, name, edition, bbox, native GEOMETRY in BNG, validity.

    Invalid geometries are split off first and only those go through the Shapely UDF:
    Spark does not promise to skip a UDF inside CASE WHEN, and repairing every polygon
    (31.9M vertices for LSOAs) for nothing would be slow.
    """
    base = df.select(
        F.col(code_col).alias("code"), F.col(name_col).alias("name"), F.col("_version").alias("edition"),
        "geom_wkb",
        F.col("bbox.xmin").cast("double").alias("xmin"), F.col("bbox.ymin").cast("double").alias("ymin"),
        F.col("bbox.xmax").cast("double").alias("xmax"), F.col("bbox.ymax").cast("double").alias("ymax"),
    ).withColumn("was_valid", F.expr(f"ST_IsValid(ST_GeomFromWKB(geom_wkb, {BNG}))"))

    valid = base.where("was_valid")
    repaired = base.where("NOT was_valid").withColumn("geom_wkb", make_valid_wkb("geom_wkb"))
    return (
        valid.unionByName(repaired)
        .withColumn("geom", F.expr(f"ST_GeomFromWKB(geom_wkb, {BNG})"))
        .withColumn("is_valid", F.expr("ST_IsValid(geom)"))
        .drop("geom_wkb")
    )


def postcode_points(postcodes: DataFrame) -> DataFrame:
    """Postcodes with a usable grid reference -> BNG point geometry (no re-projection)."""
    located = postcodes.where(
        "easting IS NOT NULL AND northing IS NOT NULL AND coalesce(grid_quality, 0) <> 9")
    point = f"ST_Point(CAST(easting AS DOUBLE), CAST(northing AS DOUBLE), {BNG})"
    return located.withColumn("geom", F.expr(point))


def within_box(x: Column, y: Column, xmin: Column, ymin: Column, xmax: Column, ymax: Column) -> Column:
    """Cheap pre-filter: point inside a bounding box (inclusive, so edges still pass)."""
    return x.between(xmin, xmax) & y.between(ymin, ymax)


def assign(points: DataFrame, polys: DataFrame, key: list[str], reference_col: str, prefix: str) -> DataFrame:
    """Point-in-polygon with edge-case handling; one output row per input point.

    Filter then refine: a cheap bounding-box test first, ST_Covers only on candidates.
    ST_Covers is true on the boundary, so a point on a shared edge matches both neighbours:
      0 candidates -> no_match (kept, counted)
      1 candidate  -> single
      2+           -> prefer the reference code (e.g. NHSPD's own assignment) if it is a
                      candidate, else the lowest code (deterministic, so re-runs agree)
    """
    p, g = points.alias("p"), polys.alias("g")
    hits = p.join(
        g,
        within_box(F.col("p.easting"), F.col("p.northing"), F.col("g.xmin"), F.col("g.ymin"),
                   F.col("g.xmax"), F.col("g.ymax"))
        & F.expr("ST_Covers(g.geom, p.geom)"),
        "inner",
    ).select(*[F.col(f"p.{k}") for k in key], F.col("g.code").alias("hit"))
    grouped = hits.groupBy(*key).agg(F.array_sort(F.collect_set("hit")).alias("hits"))

    out = points.select(*key, reference_col).join(grouped, key, "left")
    n = F.when(F.col("hits").isNull(), F.lit(0)).otherwise(F.size("hits"))
    ref_is_candidate = F.coalesce(F.array_contains("hits", F.col(reference_col)), F.lit(False))
    chosen = (F.when(n == 0, F.lit(None).cast("string"))
              .when(ref_is_candidate, F.col(reference_col))
              .otherwise(F.element_at("hits", 1)))
    resolution = (F.when(n == 0, "no_match").when(n == 1, "single")
                  .when(ref_is_candidate, "multi_prefer_reference").otherwise("multi_lowest_code"))
    return out.select(
        *key,
        F.col(reference_col).alias(f"{prefix}_reference"),
        chosen.alias(f"{prefix}_spatial"),
        n.alias(f"{prefix}_candidates"),
        resolution.alias(f"{prefix}_resolution"),
        F.coalesce(chosen == F.col(reference_col), F.lit(False)).alias(f"{prefix}_agrees"),
    )
