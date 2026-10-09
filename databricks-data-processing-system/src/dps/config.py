"""Pipeline configuration.

Paths and output format are driven by environment variables so the same code
runs locally (parquet files under ./output) and on Databricks (Delta tables
in DBFS / Unity Catalog volumes).
"""
import os
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def on_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


@dataclass(frozen=True)
class Config:
    raw_path: str
    bronze_path: str
    silver_path: str
    gold_path: str
    output_format: str  # "delta" on Databricks, "parquet" locally

    @classmethod
    def from_env(cls) -> "Config":
        if on_databricks():
            base = os.getenv("DPS_BASE_PATH", "/Volumes/main/dps/data")
            return cls(
                raw_path=os.getenv("DPS_RAW_PATH", f"{base}/raw"),
                bronze_path=f"{base}/bronze",
                silver_path=f"{base}/silver",
                gold_path=f"{base}/gold",
                output_format="delta",
            )
        out = Path(os.getenv("DPS_OUTPUT_DIR", PROJECT_ROOT / "output"))
        return cls(
            raw_path=os.getenv("DPS_RAW_PATH", str(PROJECT_ROOT / "data" / "raw")),
            bronze_path=str(out / "bronze"),
            silver_path=str(out / "silver"),
            gold_path=str(out / "gold"),
            output_format="parquet",
        )
