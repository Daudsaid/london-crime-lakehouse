"""Bronze tests: tiny landed CSVs in tmp_path, local Spark, no network, no data/."""

from datetime import date
from pathlib import Path

import pytest
from py4j.protocol import Py4JJavaError
from pyspark.errors import PySparkException
from pyspark.sql import SparkSession

from uk_crime import bronze, config

HEADER = (
    "Crime ID,Month,Reported by,Falls within,Longitude,Latitude,Location,"
    "LSOA code,LSOA name,Crime type,Last outcome category,Context"
)
ROW_A = "id-a,2026-07,Met,Met,-0.1,51.5,On or near X,E01,Camden 001A,Burglary,Under investigation,"
ROW_B = "id-b,2026-07,Met,Met,-0.2,51.6,On or near Y,E02,Camden 002B,Robbery,,"
ASB = ",2026-07,Met,Met,-0.3,51.7,On or near Z,E03,Camden 003C,Anti-social behaviour,,"


def land(root: Path, archive: str, data_month: str, force: str, *lines: str) -> Path:
    folder = root / f"archive={archive}" / f"month={data_month}"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{data_month}-{force}-street.csv"
    path.write_text("\n".join([HEADER, *lines]) + "\n")
    return path


@pytest.fixture(autouse=True)
def isolated_dirs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "LANDING_DIR", tmp_path / "landing")
    monkeypatch.setattr(
        config, "BRONZE_STREET_TABLE", tmp_path / "bronze" / "crimes_street"
    )


def table(spark: SparkSession):
    return spark.read.format("delta").load(str(config.BRONZE_STREET_TABLE))


def test_metadata_parsed_from_path_including_hyphenated_force(
    spark: SparkSession,
) -> None:
    land(config.LANDING_DIR, "2026-08", "2026-07", "avon-and-somerset", ROW_A, ASB)
    land(config.LANDING_DIR, "2026-08", "2026-08", "city-of-london", ROW_B)

    assert bronze.load_archive(spark, "2026-08") == 3

    rows = table(spark).collect()
    meta = {(r["_force"], r["_month"], r["_archive_month"]) for r in rows}
    assert meta == {
        ("avon-and-somerset", date(2026, 7, 1), date(2026, 8, 1)),
        ("city-of-london", date(2026, 8, 1), date(2026, 8, 1)),
    }
    assert {r["_source_file"] for r in rows} == {
        "archive=2026-08/month=2026-07/2026-07-avon-and-somerset-street.csv",
        "archive=2026-08/month=2026-08/2026-08-city-of-london-street.csv",
    }
    assert len({r["_ingested_at"] for r in rows}) == 1  # one value per run


def test_source_values_untouched_and_blank_cells_are_null(spark: SparkSession) -> None:
    land(config.LANDING_DIR, "2026-08", "2026-07", "metropolitan", ROW_A, ASB)
    bronze.load_archive(spark, "2026-08")

    by_type = {r["crime_type"]: r for r in table(spark).collect()}
    burglary = by_type["Burglary"]
    assert (burglary["crime_id"], burglary["longitude"], burglary["month"]) == (
        "id-a",
        "-0.1",
        "2026-07",
    )
    assert by_type["Anti-social behaviour"]["crime_id"] is None
    assert burglary["context"] is None
    assert burglary["_corrupt_record"] is None


def test_no_dedupe_in_bronze(spark: SparkSession) -> None:
    land(
        config.LANDING_DIR, "2026-08", "2026-07", "metropolitan", ROW_A, ROW_A, ASB, ASB
    )
    assert bronze.load_archive(spark, "2026-08") == 4


def test_corrupt_row_is_kept_in_corrupt_record(spark: SparkSession) -> None:
    too_many = ROW_A + ",unexpected,extra"
    land(config.LANDING_DIR, "2026-08", "2026-07", "metropolitan", ROW_A, too_many)

    assert bronze.load_archive(spark, "2026-08") == 2

    corrupt = table(spark).where("_corrupt_record IS NOT NULL").collect()
    assert len(corrupt) == 1
    assert "unexpected,extra" in corrupt[0]["_corrupt_record"]


def test_rerun_same_archive_gives_identical_counts(spark: SparkSession) -> None:
    land(config.LANDING_DIR, "2026-08", "2026-07", "metropolitan", ROW_A, ROW_B)
    land(config.LANDING_DIR, "2026-07", "2026-07", "metropolitan", ROW_A)

    bronze.load_archive(spark, "2026-07")
    first = bronze.load_archive(spark, "2026-08")
    second = bronze.load_archive(spark, "2026-08")

    assert first == second == 2
    assert table(spark).count() == 3  # archive 2026-07 partition left alone
    assert table(spark).where("_archive_month = DATE'2026-07-01'").count() == 1


def test_header_drift_fails_loudly(spark: SparkSession) -> None:
    folder = config.LANDING_DIR / "archive=2026-08" / "month=2026-07"
    folder.mkdir(parents=True)
    (folder / "2026-07-metropolitan-street.csv").write_text(
        HEADER.replace("Crime ID", "Crime Identifier") + "\n" + ROW_A + "\n"
    )
    with pytest.raises(
        (PySparkException, Py4JJavaError),
        match="Expected: Crime ID but found: Crime Identifier",
    ):
        bronze.load_archive(spark, "2026-08")
    assert not config.BRONZE_STREET_TABLE.exists()


def test_missing_archive_folder_raises(spark: SparkSession) -> None:
    with pytest.raises(bronze.BronzeError, match="run extract first"):
        bronze.load_archive(spark, "2026-08")


def test_unparseable_path_raises(spark: SparkSession) -> None:
    folder = config.LANDING_DIR / "archive=2026-08" / "month=2026-07"
    folder.mkdir(parents=True)
    (folder / "weird-street.csv").write_text(HEADER + "\n" + ROW_A + "\n")
    with pytest.raises(bronze.BronzeError, match="force/month"):
        bronze.load_archive(spark, "2026-08")


def test_main_rejects_bad_archive() -> None:
    with pytest.raises(SystemExit) as exc:
        bronze.main(["--archive", "2026-13"])
    assert exc.value.code == 2
