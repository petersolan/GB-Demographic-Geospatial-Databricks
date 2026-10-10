"""The thin spatial layer: everything that needs Databricks native ST_* functions.

Job ad: "Implement spatial joins and point-in-polygon matching, handling edge cases and
multiple matches"; "CRS (BNG, WGS84), geometry validation".
Joins run in British National Grid (EPSG:27700): Databricks ST_Transform matches PROJ
only to ~1.5-2 m, so points are not re-projected before point-in-polygon tests.
ST_Covers (not ST_Contains) is used so points exactly on a boundary still match.
Invalid boundaries are repaired with Shapely (Databricks has no ST_MakeValid).
This module only runs on Databricks; it is covered by integration tests there.
"""
