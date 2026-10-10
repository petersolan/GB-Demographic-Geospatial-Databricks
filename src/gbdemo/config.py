"""Table and volume naming in one place, so no job hard-codes a name."""
from dataclasses import dataclass


@dataclass(frozen=True)
class Layout:
    """Unity Catalog locations for one deployment (dev or prod).

    Bronze, silver and gold are separate schemas so access can be granted per layer,
    e.g. analysts read gold only and nobody but the pipeline touches bronze.
    """

    catalog: str
    bronze_schema: str
    silver_schema: str
    gold_schema: str
    landing_volume: str = "landing"
    exports_volume: str = "exports"

    def bronze(self, table: str) -> str:
        return f"{self.catalog}.{self.bronze_schema}.{table}"

    def silver(self, table: str) -> str:
        return f"{self.catalog}.{self.silver_schema}.{table}"

    def gold(self, table: str) -> str:
        return f"{self.catalog}.{self.gold_schema}.{table}"

    @property
    def landing_path(self) -> str:
        return f"/Volumes/{self.catalog}/{self.bronze_schema}/{self.landing_volume}"

    @property
    def exports_path(self) -> str:
        """Files meant to leave the platform: gold schema only, never beside raw data."""
        return f"/Volumes/{self.catalog}/{self.gold_schema}/{self.exports_volume}"
