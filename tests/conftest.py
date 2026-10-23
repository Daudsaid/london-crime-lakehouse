"""Shared pytest fixtures."""

from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession

from uk_crime.spark import get_spark


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """One local Delta-enabled SparkSession shared by the whole test run."""
    session = get_spark()
    yield session
    session.stop()
