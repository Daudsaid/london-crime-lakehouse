"""Export tests: the dashboard snapshot built from small in-memory or temp-folder gold."""

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from tests.test_gold import asb, crime, silver_df
from uk_crime import config, export, gold, silver

RUN_ID = "silver-run-1"


def sample_rows() -> list[dict[str, Any]]:
    """Ten months of Camden/Hounslow crimes, some ASB and a row with no location."""
    rows = [
        crime(
            month=date(2026, m, 1), latitude=51.5 + m / 1000, longitude=-0.1 - m / 1000
        )
        for m in range(1, 9)
    ]
    rows += [
        crime(month=date(2025, 11, 1), lsoa_code="E01H", lsoa_name="Hounslow 021D")
    ]
    rows += [asb(month=date(2026, 8, 1)), asb(month=date(2026, 8, 1))]
    rows += [crime(has_location=False, latitude=None, longitude=None)]
    return rows


def summary_for(rows: list[dict[str, Any]], run_id: str = RUN_ID) -> dict[str, object]:
    return silver.build_run_summary(
        run_id,
        datetime(2026, 10, 28, 12, 0, tzinfo=UTC),
        ["2026-08"],
        bronze_rows=len(rows) + 3,
        classes={
            "silver": len(rows),
            "quarantined": 1,
            "deduped": 1,
            "superseded": 1,
            "archive_replaced": 0,
        },
        quarantine_reasons={"conflicting_duplicate_crime_id": 1},
        asb_rows=sum(r["is_asb"] for r in rows),
        no_location_rows=sum(not r["has_location"] for r in rows),
        attribute_conflict_rows=0,
    )


def gold_tables(spark: SparkSession, rows: list[dict[str, Any]], run_id: str = RUN_ID):
    built = gold.build_gold(silver_df(spark, [{**r, "_run_id": run_id} for r in rows]))
    return gold.with_lineage(built, run_id)


def prepare_gold(spark: SparkSession, rows: list[dict[str, Any]]) -> dict[str, object]:
    """Write silver tables and the run summary to the temp lakehouse and run gold."""
    rows = [{**r, "_run_id": RUN_ID} for r in rows]
    silver.overwrite_table(silver_df(spark, rows), config.SILVER_STREET_TABLE, RUN_ID)
    empty = spark.createDataFrame([], silver.QUARANTINE_SCHEMA)
    silver.overwrite_table(empty, config.SILVER_QUARANTINE_TABLE, RUN_ID)
    summary = summary_for(rows)
    silver.write_run_summary(summary, config.SILVER_RUN_SUMMARY)
    gold.run_gold(spark)
    return summary


# ---------------------------------------------------------------- transformations


def test_exported_tables_have_the_documented_columns(spark: SparkSession) -> None:
    rows = sample_rows()
    exports = export.export_tables(gold_tables(spark, rows), summary_for(rows))

    assert set(exports) == set(export.EXPORT_COLUMNS)
    for name, df in exports.items():
        assert df.columns == export.EXPORT_COLUMNS[name], name
    types = dict(exports["mart_lsoa_ranking"].dtypes)
    assert types["latitude"] == types["longitude"] == "double"
    assert types["crime_count"] == types["asb_count"] == "bigint"
    assert dict(exports["mart_monthly_crimes"].dtypes)["month"] == "date"


def test_lsoa_centroids_are_means_over_the_ranking_window(spark: SparkSession) -> None:
    rows = [
        crime(lsoa_code="E01A", latitude=51.0, longitude=-0.2, month=date(2026, 8, 1)),
        crime(lsoa_code="E01A", latitude=53.0, longitude=-0.4, month=date(2026, 7, 1)),
        crime(
            lsoa_code="E01A", latitude=99.0, longitude=9.0, month=date(2024, 1, 1)
        ),  # old
        crime(lsoa_code="E01A", latitude=None, longitude=None, has_location=False),
        crime(lsoa_code=None, lsoa_name=None, latitude=40.0, longitude=1.0),  # UNKNOWN
    ]
    built = gold.build_gold(silver_df(spark, rows))
    centroids = export.lsoa_centroids(
        built["fct_crimes"], built["dim_date"], 12
    ).collect()

    assert [(r["lsoa_code"], r["latitude"], r["longitude"]) for r in centroids] == [
        ("E01A", pytest.approx(52.0), pytest.approx(-0.3))
    ]


def test_ranking_keeps_lsoas_without_coordinates(spark: SparkSession) -> None:
    rows = [crime(has_location=False, latitude=None, longitude=None)]
    exports = export.export_tables(gold_tables(spark, rows), summary_for(rows))
    row = exports["mart_lsoa_ranking"].first()
    assert row["crime_count"] == 1 and row["latitude"] is None


def test_reconciliation_table_adds_up_to_the_bronze_rows(spark: SparkSession) -> None:
    rows = sample_rows()
    exports = export.export_tables(gold_tables(spark, rows), summary_for(rows))
    table = {(r["group"], r["item"]): r for r in exports["reconciliation"].collect()}

    bronze = table[("reconciliation", "bronze_rows")]["rows"]
    classes = [table[("reconciliation", c)]["rows"] for c in export.CLASS_ORDER]
    assert sum(classes) == bronze == len(rows) + 3
    assert table[("gold", "fct_crimes_rows")]["rows"] == len(rows)
    assert table[("gold", "orphan_fact_keys")]["rows"] == 0
    assert table[("quarantine", "conflicting_duplicate_crime_id")]["rows"] == 1
    assert table[("silver_flags", "asb_rows")]["rows"] == 2
    assert table[("volume_anomalies", "flagged")]["rows"] == 0
    assert table[("volume_anomalies", "unacknowledged")]["rows"] == 0


def test_flagged_anomalies_are_exported_and_counted_in_the_reconciliation(
    spark: SparkSession,
) -> None:
    rows = sample_rows()
    tables = gold_tables(spark, rows)
    flagged = spark.createDataFrame(
        [
            (
                "metropolitan",
                "Robbery",
                date(2026, 8, 1),
                400,
                1000.0,
                -60.0,
                False,
                None,
            ),
            (
                "metropolitan",
                "Burglary",
                date(2026, 7, 1),
                100,
                1000.0,
                -90.0,
                True,
                "n",
            ),
        ],
        "force string, crime_type string, month date, crime_count long, "
        "baseline_median double, pct_change double, is_acknowledged boolean, note string",
    ).withColumn("_silver_run_id", F.lit(RUN_ID))
    tables = {**tables, "dq_volume_anomalies": flagged}
    exports = export.export_tables(tables, summary_for(rows))

    assert (
        exports["dq_volume_anomalies"].columns
        == (export.EXPORT_COLUMNS["dq_volume_anomalies"])
    )
    assert [r["month"] for r in exports["dq_volume_anomalies"].collect()] == [
        date(2026, 7, 1),
        date(2026, 8, 1),
    ]
    table = {(r["group"], r["item"]): r for r in exports["reconciliation"].collect()}
    assert table[("volume_anomalies", "flagged")]["rows"] == 2
    assert table[("volume_anomalies", "unacknowledged")]["rows"] == 1


def test_export_refuses_a_summary_from_another_silver_run(spark: SparkSession) -> None:
    rows = sample_rows()
    with pytest.raises(export.ExportError, match="other-run"):
        export.export_tables(
            gold_tables(spark, rows), summary_for(rows, run_id="other-run")
        )


def test_export_refuses_when_the_fact_does_not_match_the_summary(
    spark: SparkSession,
) -> None:
    rows = sample_rows()
    summary = summary_for(rows)
    summary["classes"]["silver"] += 1  # type: ignore[index]
    with pytest.raises(export.ExportError, match="fct_crimes has"):
        export.export_tables(gold_tables(spark, rows), summary)


def test_snapshot_records_provenance_and_windows(spark: SparkSession) -> None:
    rows = sample_rows()
    exports = export.export_tables(gold_tables(spark, rows), summary_for(rows))
    snapshot = export.build_snapshot(
        exports,
        summary_for(rows),
        datetime(2026, 10, 28, 13, 0, tzinfo=UTC),
        {"a.parquet": 10},
    )
    assert snapshot["archive_month"] == "2026-08"
    assert snapshot["silver_run_id"] == RUN_ID
    assert snapshot["exported_at"] == "2026-10-28T13:00:00+00:00"
    assert snapshot["months"] == {"first": "2025-11", "last": "2026-08"}
    assert snapshot["outcome"]["maturity_months"] == config.OUTCOME_MATURITY_MONTHS  # type: ignore[index]
    assert snapshot["lsoa_window"] == {"first": "2025-09", "last": "2026-08"}
    assert snapshot["file_bytes"] == {"a.parquet": 10}


def test_missing_run_summary_is_a_clear_error(tmp_path: Path) -> None:
    with pytest.raises(export.ExportError, match="run silver"):
        export.read_run_summary(tmp_path / "nope.json")


def test_main_returns_one_when_the_export_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeSpark:
        stopped = False

        def stop(self) -> None:
            self.stopped = True

    fake = FakeSpark()

    def fail(_spark: object) -> None:
        raise export.ExportError("no gold")

    # a stand-in session: the real main() stops its session, which would end the shared one
    monkeypatch.setattr(export, "get_spark", lambda: fake)
    monkeypatch.setattr(export, "run_export", fail)
    assert export.main([]) == 1
    assert fake.stopped


def test_main_returns_zero_on_success(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeSpark:
        def stop(self) -> None:
            pass

    monkeypatch.setattr(export, "get_spark", lambda: FakeSpark())
    monkeypatch.setattr(export, "run_export", lambda _spark: {})
    assert export.main([]) == 0


# ----------------------------------------------------------------- end to end


@pytest.mark.slow
def test_run_export_writes_the_expected_files_and_schemas(
    spark: SparkSession, export_env: Path
) -> None:
    prepare_gold(spark, sample_rows())
    snapshot = export.run_export(spark)

    expected = {f"{name}.parquet" for name in export.EXPORT_COLUMNS} | {
        export.SNAPSHOT_FILE
    }
    assert {p.name for p in export_env.iterdir()} == expected
    for name, columns in export.EXPORT_COLUMNS.items():
        written = spark.read.parquet(str(export_env / f"{name}.parquet"))
        assert written.columns == columns, name
    assert (
        spark.read.parquet(str(export_env / "mart_lsoa_ranking.parquet")).count() == 2
    )

    on_disk = json.loads((export_env / export.SNAPSHOT_FILE).read_text())
    assert on_disk == snapshot
    assert on_disk["silver_run_id"] == RUN_ID and on_disk["archive_month"] == "2026-08"
    assert set(on_disk["file_bytes"]) == {f"{n}.parquet" for n in export.EXPORT_COLUMNS}
    assert sum(p.stat().st_size for p in export_env.iterdir()) < export.MAX_TOTAL_BYTES
    assert not list(export_env.glob("*.tmp"))


@pytest.mark.slow
def test_run_export_refuses_inconsistent_gold(
    spark: SparkSession, export_env: Path
) -> None:
    prepare_gold(spark, sample_rows())
    other = spark.read.format("delta").load(str(gold.table_path("dim_force")))
    silver.overwrite_table(
        other.withColumn("_silver_run_id", gold.F.lit("run-B")),
        gold.table_path("dim_force"),
        "run-B",
    )
    with pytest.raises(gold.GoldConsistencyError):
        export.run_export(spark)
    assert not export_env.exists()


@pytest.mark.slow
def test_run_export_refuses_a_stale_run_summary(
    spark: SparkSession, export_env: Path
) -> None:
    prepare_gold(spark, sample_rows())
    stale = summary_for(sample_rows(), run_id="older-run")
    silver.write_run_summary(stale, config.SILVER_RUN_SUMMARY)
    with pytest.raises(export.ExportError, match="older-run"):
        export.run_export(spark)
