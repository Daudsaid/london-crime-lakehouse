"""Central configuration: every path and setting lives here.

Paths hang off DATA_DIR so that on Databricks it can point at a Unity Catalog
Volume (e.g. DATA_DIR=/Volumes/main/crime/data) without touching any other code.
"""

import os
from pathlib import Path
from typing import NamedTuple

PROJECT_ROOT: Path = Path(__file__).resolve().parents[2]

DATA_DIR: Path = Path(os.environ.get("UK_CRIME_DATA_DIR", PROJECT_ROOT / "data"))

LANDING_DIR: Path = DATA_DIR / "landing"
DOWNLOADS_DIR: Path = DATA_DIR / "_downloads"
TMP_DIR: Path = DATA_DIR / "_tmp"
LAKEHOUSE_DIR: Path = DATA_DIR / "lakehouse"
BRONZE_DIR: Path = LAKEHOUSE_DIR / "bronze"
SILVER_DIR: Path = LAKEHOUSE_DIR / "silver"
GOLD_DIR: Path = LAKEHOUSE_DIR / "gold"
BRONZE_STREET_TABLE: Path = BRONZE_DIR / "crimes_street"
SILVER_STREET_TABLE: Path = SILVER_DIR / "crimes_street"
SILVER_QUARANTINE_TABLE: Path = SILVER_DIR / "crimes_street_quarantine"
SILVER_RUN_SUMMARY: Path = SILVER_DIR / "run_summary.json"

# Small Parquet snapshot of gold that the Streamlit dashboard reads (committed to git).
DASHBOARD_DATA_DIR: Path = Path(
    os.environ.get("UK_CRIME_DASHBOARD_DATA_DIR", PROJECT_ROOT / "dashboard" / "data")
)

ARCHIVE_BASE_URL: str = "https://data.police.uk/data/archive"
KEEP_ARCHIVES: bool = os.environ.get("UK_CRIME_KEEP_ARCHIVES", "true").lower() == "true"
FORCES: tuple[str, ...] = tuple(
    f.strip()
    for f in os.environ.get("UK_CRIME_FORCES", "metropolitan,city-of-london").split(",")
    if f.strip()
)
# Gold marts. All windows are relative to the latest month in silver, not today's date.
#
# mart_outcome_rates leaves out the newest OUTCOME_MATURITY_MONTHS months, whose outcomes
# have not matured, and the months in OUTCOME_EXCLUDED_MONTHS (known outliers, YYYY-MM).
# Rule for the maturity window: exclude the smallest number of most-recent months so that
# every remaining month newer than the baseline window has a placeholder-outcome share
# within 2pp of the baseline (the median of months older than 18 months). Set from archive
# 2026-08 (analyses/profile_outcome_maturity.py); re-check when a new archive lands.
# 2024-10 is excluded from this mart only: its placeholder share is 25.6% against a 2.4%
# baseline. Every other table keeps it.
OUTCOME_MATURITY_MONTHS: int = int(
    os.environ.get("UK_CRIME_OUTCOME_MATURITY_MONTHS", "16")
)
OUTCOME_EXCLUDED_MONTHS: tuple[str, ...] = tuple(
    m.strip()
    for m in os.environ.get("UK_CRIME_OUTCOME_EXCLUDED_MONTHS", "2024-10").split(",")
    if m.strip()
)
# mart_lsoa_ranking covers the newest LSOA_RANKING_MONTHS months.
LSOA_RANKING_MONTHS: int = int(os.environ.get("UK_CRIME_LSOA_RANKING_MONTHS", "12"))
# dq_volume_anomalies flags a (force, crime_type, month) whose crime_count differs from the
# median of the previous 3 months by more than VOLUME_ANOMALY_THRESHOLD_PCT percent, when
# that baseline median is at least VOLUME_ANOMALY_MIN_BASELINE rows (small counts are noisy).
VOLUME_ANOMALY_THRESHOLD_PCT: float = float(
    os.environ.get("UK_CRIME_VOLUME_ANOMALY_THRESHOLD_PCT", "50")
)
VOLUME_ANOMALY_MIN_BASELINE: int = int(
    os.environ.get("UK_CRIME_VOLUME_ANOMALY_MIN_BASELINE", "500")
)


class AcknowledgedAnomaly(NamedTuple):
    """A known source change: flags for this force and crime type from from_month (YYYY-MM)
    onwards are marked is_acknowledged and carry the note. Still flagged, never hidden."""

    force: str
    crime_type: str
    from_month: str
    note: str


ACKNOWLEDGED_VOLUME_ANOMALIES: tuple[AcknowledgedAnomaly, ...] = (
    AcknowledgedAnomaly(
        force="metropolitan",
        crime_type="Criminal damage and arson",
        from_month="2026-05",
        note=(
            "Source change: Met CD&A fell ~94% while other types rose; consistent with "
            "reclassification (unverified). Coincides with the Met placeholder-text change "
            "(2026-04) and the out-of-London drop (2026-05)."
        ),
    ),
)
HTTP_TIMEOUT: tuple[float, float] = (10.0, 60.0)  # (connect, read) seconds
HTTP_MAX_ATTEMPTS: int = 5

SPARK_APP_NAME: str = os.environ.get("UK_CRIME_SPARK_APP_NAME", "uk-crime-lakehouse")
SPARK_MASTER: str = os.environ.get("UK_CRIME_SPARK_MASTER", "local[*]")
SPARK_SHUFFLE_PARTITIONS: int = int(os.environ.get("UK_CRIME_SHUFFLE_PARTITIONS", "8"))
SPARK_DRIVER_MEMORY: str = os.environ.get("UK_CRIME_SPARK_DRIVER_MEMORY", "4g")
SPARK_TIMEZONE: str = "UTC"
