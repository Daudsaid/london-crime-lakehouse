"""Central configuration: every path and setting lives here.

Paths hang off DATA_DIR so that on Databricks it can point at a Unity Catalog
Volume (e.g. DATA_DIR=/Volumes/main/crime/data) without touching any other code.
"""

import os
from pathlib import Path

PROJECT_ROOT: Path = Path(__file__).resolve().parents[2]

DATA_DIR: Path = Path(os.environ.get("UK_CRIME_DATA_DIR", PROJECT_ROOT / "data"))

LANDING_DIR: Path = DATA_DIR / "landing"
LAKEHOUSE_DIR: Path = DATA_DIR / "lakehouse"
BRONZE_DIR: Path = LAKEHOUSE_DIR / "bronze"
SILVER_DIR: Path = LAKEHOUSE_DIR / "silver"
GOLD_DIR: Path = LAKEHOUSE_DIR / "gold"

SPARK_APP_NAME: str = os.environ.get("UK_CRIME_SPARK_APP_NAME", "uk-crime-lakehouse")
SPARK_MASTER: str = os.environ.get("UK_CRIME_SPARK_MASTER", "local[*]")
SPARK_SHUFFLE_PARTITIONS: int = int(os.environ.get("UK_CRIME_SHUFFLE_PARTITIONS", "8"))
SPARK_DRIVER_MEMORY: str = os.environ.get("UK_CRIME_SPARK_DRIVER_MEMORY", "4g")
SPARK_TIMEZONE: str = "UTC"
