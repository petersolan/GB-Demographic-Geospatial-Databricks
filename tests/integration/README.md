# Integration tests (run on Databricks)

Native Databricks spatial functions (`ST_*`) exist only on Databricks, so spatial tests run
there as a bundle job against small hand-made fixtures: points on a boundary, points in the
sea, points matching two overlapping polygons, and invalid polygons.

Unit tests for everything else live in `tests/unit/` and run locally (Docker) and in CI.
