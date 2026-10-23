"""Smoke test: the local session can write and read a Delta table."""

from pathlib import Path

from pyspark.sql import SparkSession


def test_delta_write_and_read_roundtrip(spark: SparkSession, tmp_path: Path) -> None:
    path = str(tmp_path / "smoke_delta")
    df = spark.createDataFrame([(1, "a"), (2, "b")], "id int, label string")

    df.write.format("delta").save(path)
    result = spark.read.format("delta").load(path)

    assert sorted(tuple(r) for r in result.collect()) == [(1, "a"), (2, "b")]
    assert (tmp_path / "smoke_delta" / "_delta_log").is_dir()


def test_session_timezone_is_utc(spark: SparkSession) -> None:
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
