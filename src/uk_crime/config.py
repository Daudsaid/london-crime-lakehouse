"""Central configuration: every path and setting lives here.

Paths hang off DATA_DIR so that on Databricks it can point at a Unity Catalog
Volume (e.g. DATA_DIR=/Volumes/main/crime/data) without touching any other code.
"""

import os
from pathlib import Path

PROJECT_ROOT: Path = Path(__file__).resolve().parents[2]

DATA_DIR: Path = Path(os.environ.get("UK_CRIME_DATA_DIR", PROJECT_ROOT / "data"))

LANDING_DIR: Path = DATA_DIR / "landing"
DOWNLOADS_DIR: Path = DATA_DIR / "_downloads"
TMP_DIR: Path = DATA_DIR / "_tmp"
LAKEHOUSE_DIR: Path = DATA_DIR / "lakehouse"
BRONZE_DIR: Path = LAKEHOUSE_DIR / "bronze"
SILVER_DIR: Path = LAKEHOUSE_DIR / "silver"
GOLD_DIR: Path = LAKEHOUSE_DIR / "gold"

ARCHIVE_BASE_URL: str = "https://data.police.uk/data/archive"
KEEP_ARCHIVES: bool = os.environ.get("UK_CRIME_KEEP_ARCHIVES", "true").lower() == "true"
FORCES: tuple[str, ...] = tuple(
    f.strip()
    for f in os.environ.get("UK_CRIME_FORCES", "metropolitan,city-of-london").split(",")
    if f.strip()
)
HTTP_TIMEOUT: tuple[float, float] = (10.0, 60.0)  # (connect, read) seconds
HTTP_MAX_ATTEMPTS: int = 5

SPARK_APP_NAME: str = os.environ.get("UK_CRIME_SPARK_APP_NAME", "uk-crime-lakehouse")
SPARK_MASTER: str = os.environ.get("UK_CRIME_SPARK_MASTER", "local[*]")
SPARK_SHUFFLE_PARTITIONS: int = int(os.environ.get("UK_CRIME_SHUFFLE_PARTITIONS", "8"))
SPARK_DRIVER_MEMORY: str = os.environ.get("UK_CRIME_SPARK_DRIVER_MEMORY", "4g")
SPARK_TIMEZONE: str = "UTC"
