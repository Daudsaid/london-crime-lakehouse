"""Shared pytest fixtures."""

from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from uk_crime.spark import get_spark

# Tests use tiny DataFrames, so a small session is faster than the production defaults
# (local[*] with 8 shuffle partitions): fewer tasks and no web UI.
TEST_SPARK_SETTINGS = {
    "spark.master": "local[2]",
    "spark.sql.shuffle.partitions": 1,
    "spark.default.parallelism": 2,
    "spark.ui.enabled": False,
    "spark.driver.memory": "2g",  # tiny data; leaves headroom on small machines and CI
    # Delta reads its transaction log with 50 tasks by default; tiny tables need 2.
    "spark.databricks.delta.snapshotPartitions": 2,
    # Adaptive execution starts new thread pools for every query. Hundreds of small test
    # queries pushed the JVM past macOS's 2048-threads-per-process limit ("unable to create
    # native thread"), so tests run without it (about 500 threads at most).
    "spark.sql.adaptive.enabled": False,
}


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """One small local Delta-enabled SparkSession shared by the whole test run."""
    session = get_spark(TEST_SPARK_SETTINGS)
    yield session
    session.stop()


@pytest.fixture
def export_env(tmp_path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point silver, gold, the run summary and the dashboard folder at a temp folder.

    Also uses a 6-month outcome window with no excluded months. Returns the dashboard
    data folder.
    """
    from uk_crime import config

    monkeypatch.setattr(
        config, "SILVER_STREET_TABLE", tmp_path / "silver" / "crimes_street"
    )
    monkeypatch.setattr(
        config,
        "SILVER_QUARANTINE_TABLE",
        tmp_path / "silver" / "crimes_street_quarantine",
    )
    monkeypatch.setattr(
        config, "SILVER_RUN_SUMMARY", tmp_path / "silver" / "run_summary.json"
    )
    monkeypatch.setattr(config, "GOLD_DIR", tmp_path / "gold")
    monkeypatch.setattr(config, "TMP_DIR", tmp_path / "_tmp")
    monkeypatch.setattr(config, "DASHBOARD_DATA_DIR", tmp_path / "dashboard_data")
    monkeypatch.setattr(config, "OUTCOME_MATURITY_MONTHS", 6)
    monkeypatch.setattr(config, "OUTCOME_EXCLUDED_MONTHS", ())
    return tmp_path / "dashboard_data"
