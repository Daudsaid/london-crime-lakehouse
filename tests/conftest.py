"""Shared pytest fixtures."""

from collections.abc import Iterator

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
