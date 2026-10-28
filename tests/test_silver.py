"""Silver tests: small in-memory bronze DataFrames, one case per test."""

import logging
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from uk_crime import config, silver

BRONZE_SCHEMA = StructType(
    [StructField(c, StringType(), True) for c in silver.SOURCE]
    + [
        StructField("_corrupt_record", StringType(), True),
        StructField("_archive_month", DateType(), True),
        StructField("_month", DateType(), True),
        StructField("_force", StringType(), True),
        StructField("_source_file", StringType(), True),
        StructField("_ingested_at", TimestampType(), True),
    ]
)
RUN_TS = datetime(2026, 10, 27, 12, 0, tzinfo=UTC)
RUN_ID = "run-1"


def row(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "crime_id": "c1",
        "month": "2026-07",
        "reported_by": "Met",
        "falls_within": "Met",
        "longitude": "-0.1",
        "latitude": "51.5",
        "location": "On or near X",
        "lsoa_code": "E01",
        "lsoa_name": "Camden 001A",
        "crime_type": "Burglary",
        "last_outcome_category": "Under investigation",
        "context": None,
        "_corrupt_record": None,
        "_archive_month": date(2026, 8, 1),
        "_month": date(2026, 7, 1),
        "_force": "metropolitan",
        "_source_file": "archive=2026-08/month=2026-07/2026-07-metropolitan-street.csv",
        "_ingested_at": RUN_TS,
    }
    return {**base, **overrides}


def asb(**overrides: Any) -> dict[str, Any]:
    return row(crime_id=None, crime_type=silver.ASB_CRIME_TYPE, **overrides)


def bronze(spark: SparkSession, rows: list[dict[str, Any]]) -> DataFrame:
    names = BRONZE_SCHEMA.fieldNames()
    return spark.createDataFrame(
        [tuple(r[n] for n in names) for r in rows], BRONZE_SCHEMA
    )


def build(spark: SparkSession, rows: list[dict[str, Any]]):
    """Return (silver rows, quarantine rows, class counts, reconciled classes)."""
    df = bronze(spark, rows)
    result = silver.build_silver(df, RUN_TS, RUN_ID)
    counts = silver.count_classes(result.classified)
    selected = silver.select_newest_archive(df).count()
    classes = silver.reconcile(len(rows), selected, counts)
    return result.silver.collect(), result.quarantine.collect(), counts, classes


def test_newest_archive_wins_at_file_grain(spark: SparkSession) -> None:
    old = date(2026, 7, 1)
    rows = [
        # (metropolitan, 2026-06) exists in both archives: only the 2026-08 copy survives
        row(crime_id="a", last_outcome_category="Local resolution", _month=date(2026, 6, 1), month="2026-06"),
        row(crime_id="a", last_outcome_category="Under investigation", _month=date(2026, 6, 1), month="2026-06", _archive_month=old),
        row(crime_id="gone", _month=date(2026, 6, 1), month="2026-06", _archive_month=old),
        # (metropolitan, 2026-05) exists only in the older archive: kept
        row(crime_id="b", _month=date(2026, 5, 1), month="2026-05", _archive_month=old),
    ]  # fmt: skip
    kept, _q, _counts, classes = build(spark, rows)
    assert {(r["crime_id"], r["last_outcome_category"]) for r in kept} == {
        ("a", "Local resolution"),
        ("b", "Under investigation"),
    }
    assert classes["archive_replaced"] == 2
    assert {r["_archive_month"] for r in kept} == {date(2026, 8, 1), date(2026, 7, 1)}


def test_identical_crime_rows_collapse(spark: SparkSession) -> None:
    kept, quarantined, counts, _ = build(spark, [row(), row()])
    assert len(kept) == 1 and not quarantined
    assert counts["deduped"] == 1
    assert kept[0]["record_key"] == "c1"


def test_progressed_outcome_beats_placeholder(spark: SparkSession) -> None:
    rows = [
        row(last_outcome_category="Status update unavailable"),
        row(last_outcome_category="Local resolution"),
        row(last_outcome_category=None),
    ]
    kept, quarantined, counts, _ = build(spark, rows)
    assert [r["last_outcome_category"] for r in kept] == ["Local resolution"]
    assert counts["superseded"] == 2 and not quarantined
    assert kept[0]["dq_attribute_conflict"] is False  # outcome was the only difference


def test_identical_copies_of_the_progressed_variant_are_deduped_not_superseded(
    spark: SparkSession,
) -> None:
    rows = [
        row(last_outcome_category="Local resolution"),
        row(last_outcome_category="Local resolution"),
        row(last_outcome_category="Under investigation"),
    ]
    kept, _q, counts, _ = build(spark, rows)
    assert len(kept) == 1
    assert (counts["deduped"], counts["superseded"]) == (1, 1)


def test_two_different_progressed_outcomes_quarantine_all_copies(
    spark: SparkSession,
) -> None:
    rows = [
        row(last_outcome_category="Local resolution"),
        row(last_outcome_category="Offender given a caution"),
        row(last_outcome_category="Under investigation"),
    ]
    kept, quarantined, counts, _ = build(spark, rows)
    assert not kept
    assert {r["_dq_reason"] for r in quarantined} == {"conflicting_duplicate_crime_id"}
    assert counts["quarantined"] == 3


def test_only_placeholders_that_differ_are_quarantined(spark: SparkSession) -> None:
    rows = [
        row(last_outcome_category=None),
        row(last_outcome_category="Under investigation"),
    ]
    kept, quarantined, _c, _ = build(spark, rows)
    assert not kept and len(quarantined) == 2


def test_r5_keep_with_crime_type_difference_sets_attribute_conflict(
    spark: SparkSession,
) -> None:
    rows = [
        row(
            crime_type="Public order", last_outcome_category="Status update unavailable"
        ),
        row(
            crime_type="Violence and sexual offences",
            last_outcome_category="Local resolution",
        ),
    ]
    kept, _q, _c, _ = build(spark, rows)
    assert len(kept) == 1
    assert kept[0]["crime_type"] == "Violence and sexual offences"
    assert kept[0]["dq_attribute_conflict"] is True


def test_r5_keep_with_location_difference_sets_attribute_conflict(
    spark: SparkSession,
) -> None:
    rows = [
        row(latitude="51.6", last_outcome_category="Status update unavailable"),
        row(latitude="51.5", last_outcome_category="Local resolution"),
    ]
    kept, _q, _c, _ = build(spark, rows)
    assert kept[0]["dq_attribute_conflict"] is True and kept[0]["latitude"] == 51.5


def test_identical_asb_rows_are_all_kept_with_distinct_stable_keys(
    spark: SparkSession,
) -> None:
    rows = [asb(), asb(), asb()]
    kept, quarantined, counts, _ = build(spark, rows)
    keys = [r["record_key"] for r in kept]
    assert len(kept) == 3 and len(set(keys)) == 3 and not quarantined
    assert counts["deduped"] == 0
    rebuilt, *_ = build(spark, list(reversed(rows)))
    assert sorted(keys) == sorted(r["record_key"] for r in rebuilt)


def test_asb_rows_with_nulls_in_different_fields_do_not_collide(
    spark: SparkSession,
) -> None:
    # concat_ws would skip NULLs and make both rows hash "X"
    rows = [
        asb(location=None, lsoa_code="X"),
        asb(location="X", lsoa_code=None),
    ]
    kept, _q, _c, _ = build(spark, rows)
    assert len({r["record_key"] for r in kept}) == 2


def test_crime_record_key_is_the_crime_id(spark: SparkSession) -> None:
    kept, *_ = build(spark, [row(crime_id="abc123")])
    assert kept[0]["record_key"] == "abc123" and kept[0]["is_asb"] is False


def test_strings_are_trimmed_and_blanks_become_null(spark: SparkSession) -> None:
    kept, *_ = build(
        spark, [row(location="  On or near X  ", lsoa_name="   ", context="")]
    )
    assert kept[0]["location"] == "On or near X"
    assert kept[0]["lsoa_name"] is None and kept[0]["context"] is None
    assert kept[0]["month"] == date(2026, 7, 1)


def test_corrupt_row_is_quarantined_without_vocabulary_error(
    spark: SparkSession,
) -> None:
    corrupt = row(
        crime_id=None, crime_type=None, last_outcome_category=None, month=None,
        _corrupt_record="id,2026-07,too,many,fields",
    )  # fmt: skip
    kept, quarantined, _c, _ = build(spark, [row(crime_id="ok"), corrupt])
    assert len(kept) == 1
    assert [r["_dq_reason"] for r in quarantined] == ["corrupt_record"]
    assert quarantined[0]["_corrupt_record"] == "id,2026-07,too,many,fields"
    assert quarantined[0]["_quarantined_at"] is not None


@pytest.mark.parametrize("month", ["2026-13", "July", "2026-7", None])
def test_unparseable_month_is_quarantined(
    spark: SparkSession, month: str | None
) -> None:
    kept, quarantined, _c, _ = build(spark, [row(month=month)])
    assert not kept and quarantined[0]["_dq_reason"] == "invalid_month"


def test_month_that_differs_from_the_path_month_is_quarantined(
    spark: SparkSession,
) -> None:
    _k, quarantined, _c, _ = build(spark, [row(month="2026-06")])
    assert quarantined[0]["_dq_reason"] == "invalid_month"


def test_asb_with_a_crime_id_and_crime_without_one_are_quarantined(
    spark: SparkSession,
) -> None:
    rows = [row(crime_id=None), row(crime_id="x", crime_type=silver.ASB_CRIME_TYPE)]
    kept, quarantined, _c, _ = build(spark, rows)
    assert not kept
    assert {r["_dq_reason"] for r in quarantined} == {"unexpected_crime_id_presence"}


def test_null_coordinates_are_kept_with_has_location_false(spark: SparkSession) -> None:
    kept, quarantined, _c, _ = build(
        spark, [row(longitude=None, latitude=None), row(crime_id="d")]
    )
    by_id = {r["crime_id"]: r for r in kept}
    assert not quarantined
    assert by_id["c1"]["has_location"] is False and by_id["c1"]["longitude"] is None
    assert by_id["d"]["has_location"] is True


@pytest.mark.parametrize(
    ("lon", "lat"),
    [
        ("100.0", "51.5"),
        ("-0.1", "10.0"),
        ("abc", "51.5"),
        ("-0.1", None),
        ("NaN", "51.5"),
    ],
)
def test_bad_coordinates_are_quarantined(
    spark: SparkSession, lon: str, lat: str | None
) -> None:
    kept, quarantined, _c, _ = build(spark, [row(longitude=lon, latitude=lat)])
    assert not kept and quarantined[0]["_dq_reason"] == "invalid_coordinates"


def test_only_the_first_failing_rule_is_recorded(spark: SparkSession) -> None:
    both = row(month="2026-13", longitude="100", crime_id=None)
    _k, quarantined, _c, _ = build(spark, [both])
    assert [r["_dq_reason"] for r in quarantined] == ["invalid_month"]


def test_quarantine_keeps_the_raw_bronze_row(spark: SparkSession) -> None:
    _k, quarantined, _c, _ = build(spark, [row(longitude="100.0", location="  raw  ")])
    q = quarantined[0]
    assert q["longitude"] == "100.0" and q["location"] == "  raw  "  # untouched strings
    assert q["_force"] == "metropolitan" and q["_quarantined_at"] is not None


def test_unknown_outcome_raises_with_the_value(spark: SparkSession) -> None:
    with pytest.raises(silver.VocabularyError, match="Brand new outcome"):
        build(spark, [row(last_outcome_category="Brand new outcome")])


def test_unknown_crime_type_raises_with_the_value(spark: SparkSession) -> None:
    with pytest.raises(silver.VocabularyError, match="Cyber crime"):
        build(spark, [row(crime_type="Cyber crime")])


def test_reconciliation_holds_for_a_mixed_bronze(spark: SparkSession) -> None:
    old = date(2026, 7, 1)
    rows = [
        row(crime_id="ok"),
        row(crime_id="dup"), row(crime_id="dup"),
        row(crime_id="sup", last_outcome_category="Local resolution"),
        row(crime_id="sup", last_outcome_category="Under investigation"),
        row(crime_id="bad", month="2026-13"),
        row(crime_id="conflict", last_outcome_category="Local resolution"),
        row(crime_id="conflict", last_outcome_category="Offender given a caution"),
        asb(), asb(),
        row(crime_id="old", _month=date(2026, 6, 1), month="2026-06", _archive_month=old),
        row(crime_id="new", _month=date(2026, 6, 1), month="2026-06"),
    ]  # fmt: skip
    kept, quarantined, _counts, classes = build(spark, rows)
    assert classes == {
        "silver": len(kept),
        "quarantined": len(quarantined),
        "deduped": 1,
        "superseded": 1,
        "archive_replaced": 1,
    }
    assert sum(classes.values()) == len(rows)


def test_reconcile_raises_when_classes_do_not_add_up() -> None:
    counts = {"silver": 5, "quarantined": 1, "deduped": 0, "superseded": 0}
    with pytest.raises(silver.ReconciliationError):
        silver.reconcile(10, 10, counts)


def test_guardrail_raises_with_counts_when_a_reason_is_too_common() -> None:
    with pytest.raises(silver.GuardrailError, match="invalid_month': 10"):
        silver.check_quarantine_guardrail(
            {"invalid_month": 10, "corrupt_record": 1}, 1000
        )
    silver.check_quarantine_guardrail(
        {"invalid_month": 5}, 1000
    )  # exactly 0.5%: allowed


# ----------------------------------------------------------- end-to-end on Delta


@pytest.fixture
def tables(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "BRONZE_STREET_TABLE", tmp_path / "bronze")
    monkeypatch.setattr(config, "SILVER_STREET_TABLE", tmp_path / "silver")
    monkeypatch.setattr(config, "SILVER_QUARANTINE_TABLE", tmp_path / "quarantine")
    monkeypatch.setattr(config, "SILVER_RUN_SUMMARY", tmp_path / "run_summary.json")


def mixed_rows() -> list[dict[str, Any]]:
    return [
        row(crime_id="ok"),
        row(crime_id="dup"), row(crime_id="dup"),
        row(crime_id="sup", last_outcome_category="Local resolution"),
        row(crime_id="sup", last_outcome_category="Under investigation"),
        row(crime_id="bad", month="2026-13"),
        asb(), asb(), asb(location=None),
    ]  # fmt: skip


@pytest.mark.slow
def test_rerun_gives_identical_output(spark: SparkSession, tables: None) -> None:
    bronze(spark, mixed_rows()).write.format("delta").save(
        str(config.BRONZE_STREET_TABLE)
    )

    first = silver.run_silver(spark, max_quarantine_fraction=1.0)
    second = silver.run_silver(spark, max_quarantine_fraction=1.0)

    for key in ("rows", "distinct_keys", "key_checksum", "row_checksum", "classes"):
        assert first[key] == second[key]
    assert first["rows"] == 6  # ok, dup, sup and the three ASB rows
    assert first["classes"] == {
        "silver": 6, "quarantined": 1, "deduped": 1, "superseded": 1, "archive_replaced": 0,
    }  # fmt: skip
    table = spark.read.format("delta").load(str(config.SILVER_STREET_TABLE))
    assert table.count() == 6 and table.select("record_key").distinct().count() == 6
    assert (
        spark.read.format("delta").load(str(config.SILVER_QUARANTINE_TABLE)).count()
        == 1
    )


@pytest.mark.slow
def test_guardrail_stops_before_anything_is_written(
    spark: SparkSession, tables: None
) -> None:
    bronze(spark, mixed_rows()).write.format("delta").save(
        str(config.BRONZE_STREET_TABLE)
    )
    with pytest.raises(silver.GuardrailError):
        silver.run_silver(spark)  # 1 of 9 rows is 11%: far above 0.5%
    assert not config.SILVER_STREET_TABLE.exists()
    assert not config.SILVER_QUARANTINE_TABLE.exists()


def test_main_requires_all() -> None:
    with pytest.raises(SystemExit) as exc:
        silver.main([])
    assert exc.value.code == 2


# ------------------------------------------------------------------ _run_id / consistency


@pytest.mark.slow
def test_both_tables_carry_the_same_run_id(spark: SparkSession, tables: None) -> None:
    bronze(spark, mixed_rows()).write.format("delta").save(
        str(config.BRONZE_STREET_TABLE)
    )
    stats = silver.run_silver(spark, max_quarantine_fraction=1.0)

    for path in (config.SILVER_STREET_TABLE, config.SILVER_QUARANTINE_TABLE):
        ids = (
            spark.read.format("delta")
            .load(str(path))
            .select("_run_id")
            .distinct()
            .collect()
        )
        assert [r[0] for r in ids] == [stats["run_id"]]
    assert silver.assert_silver_consistent(spark) == stats["run_id"]


@pytest.mark.slow
def test_each_run_gets_a_new_run_id(spark: SparkSession, tables: None) -> None:
    bronze(spark, mixed_rows()).write.format("delta").save(
        str(config.BRONZE_STREET_TABLE)
    )
    first = silver.run_silver(spark, max_quarantine_fraction=1.0)
    second = silver.run_silver(spark, max_quarantine_fraction=1.0)
    assert first["run_id"] != second["run_id"]
    assert silver.assert_silver_consistent(spark) == second["run_id"]


@pytest.mark.slow
def test_consistency_fails_when_only_quarantine_was_rewritten(
    spark: SparkSession, tables: None
) -> None:
    source = bronze(spark, mixed_rows())
    source.write.format("delta").save(str(config.BRONZE_STREET_TABLE))
    silver.run_silver(spark, max_quarantine_fraction=1.0)

    other = silver.build_silver(source, RUN_TS, "other-run")  # a later run that crashed
    silver.overwrite_table(
        other.quarantine, config.SILVER_QUARANTINE_TABLE, "other-run"
    )

    with pytest.raises(silver.ConsistencyError, match="other-run"):
        silver.assert_silver_consistent(spark)


@pytest.mark.slow
def test_consistency_fails_when_only_silver_was_rewritten(
    spark: SparkSession, tables: None
) -> None:
    source = bronze(spark, mixed_rows())
    source.write.format("delta").save(str(config.BRONZE_STREET_TABLE))
    silver.run_silver(spark, max_quarantine_fraction=1.0)

    other = silver.build_silver(source, RUN_TS, "other-run")
    silver.overwrite_table(other.silver, config.SILVER_STREET_TABLE, "other-run")

    with pytest.raises(silver.ConsistencyError):
        silver.assert_silver_consistent(spark)


@pytest.mark.slow
def test_consistency_works_when_quarantine_is_empty(
    spark: SparkSession, tables: None
) -> None:
    clean = [row(crime_id="a"), row(crime_id="b")]
    bronze(spark, clean).write.format("delta").save(str(config.BRONZE_STREET_TABLE))
    stats = silver.run_silver(spark, max_quarantine_fraction=1.0)

    quarantine = spark.read.format("delta").load(str(config.SILVER_QUARANTINE_TABLE))
    assert quarantine.count() == 0  # the run id then lives on the Delta commit only
    assert silver.assert_silver_consistent(spark) == stats["run_id"]


@pytest.mark.slow
def test_consistency_fails_when_a_table_holds_two_run_ids(
    spark: SparkSession, tables: None
) -> None:
    source = bronze(spark, mixed_rows())
    source.write.format("delta").save(str(config.BRONZE_STREET_TABLE))
    silver.run_silver(spark, max_quarantine_fraction=1.0)

    extra = silver.build_silver(source, RUN_TS, "other-run").silver.limit(1)
    extra.write.format("delta").mode("append").save(str(config.SILVER_STREET_TABLE))

    with pytest.raises(silver.ConsistencyError):
        silver.assert_silver_consistent(spark)


@pytest.mark.slow
def test_consistency_fails_when_a_table_is_missing(
    spark: SparkSession, tables: None
) -> None:
    with pytest.raises(silver.ConsistencyError, match="missing or not a Delta table"):
        silver.assert_silver_consistent(spark)


# ----------------------------------------------- non-blank unparseable coordinates


@pytest.mark.parametrize(
    ("lon", "lat"),
    [("abc", "def"), ("abc", None), (None, "def"), ("-0.1", "5x1.5"), ("1,5", "51.5")],
)
def test_unparseable_non_blank_coordinates_are_quarantined_not_kept_without_location(
    spark: SparkSession, lon: str | None, lat: str | None
) -> None:
    kept, quarantined, _c, _ = build(spark, [row(longitude=lon, latitude=lat)])
    assert kept == []  # NOT kept with has_location = false
    assert [r["_dq_reason"] for r in quarantined] == ["invalid_coordinates"]
    assert (quarantined[0]["longitude"], quarantined[0]["latitude"]) == (
        lon,
        lat,
    )  # raw text kept


# ------------------------------------------------------------- schema change path


def test_built_tables_match_the_schemas_defined_in_code(spark: SparkSession) -> None:
    result = silver.build_silver(bronze(spark, mixed_rows()), RUN_TS, RUN_ID)
    assert silver.diff_schemas(result.silver.schema, silver.SILVER_SCHEMA).is_empty()
    assert silver.diff_schemas(
        result.quarantine.schema, silver.QUARANTINE_SCHEMA
    ).is_empty()


def test_diff_schemas_names_added_removed_and_changed_columns() -> None:
    existing = StructType(
        [
            StructField("record_key", StringType()),
            StructField("longitude", StringType()),  # expected: double
            StructField("legacy", StringType()),  # not expected
        ]
    )
    expected = StructType(
        [
            StructField("record_key", StringType()),
            StructField("longitude", DoubleType()),
            StructField("_run_id", StringType()),  # new
        ]
    )
    diff = silver.diff_schemas(existing, expected)
    assert diff.added == {"_run_id": "string"}
    assert diff.removed == {"legacy": "string"}
    assert diff.changed == {"longitude": ("string", "double")}
    assert silver.diff_schemas(expected, expected).is_empty()


def old_style_tables(spark: SparkSession) -> None:
    """Write silver/quarantine tables as they were before _run_id existed."""
    source = bronze(spark, mixed_rows())
    source.write.format("delta").save(str(config.BRONZE_STREET_TABLE))
    result = silver.build_silver(source, RUN_TS, RUN_ID)
    result.silver.drop("_run_id").write.format("delta").save(
        str(config.SILVER_STREET_TABLE)
    )
    result.quarantine.drop("_run_id").write.format("delta").save(
        str(config.SILVER_QUARANTINE_TABLE)
    )


@pytest.mark.slow
def test_schema_mismatch_without_the_flag_raises_and_writes_nothing(
    spark: SparkSession, tables: None
) -> None:
    old_style_tables(spark)
    before = spark.read.format("delta").load(str(config.SILVER_STREET_TABLE)).columns

    with pytest.raises(silver.SchemaChangeError) as exc:
        silver.run_silver(spark, max_quarantine_fraction=1.0)

    assert "--allow-schema-change" in str(exc.value)
    assert "added _run_id (string)" in str(exc.value)
    for path in (config.SILVER_STREET_TABLE, config.SILVER_QUARANTINE_TABLE):
        assert (
            DeltaTable.forPath(spark, str(path)).history().count() == 1
        )  # only the old write
    assert (
        spark.read.format("delta").load(str(config.SILVER_STREET_TABLE)).columns
        == before
    )


@pytest.mark.slow
def test_schema_mismatch_with_the_flag_rewrites_and_logs_the_diff(
    spark: SparkSession, tables: None, caplog: pytest.LogCaptureFixture
) -> None:
    old_style_tables(spark)

    with caplog.at_level(logging.WARNING, logger=silver.logger.name):
        stats = silver.run_silver(
            spark, max_quarantine_fraction=1.0, allow_schema_change=True
        )

    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any("crimes_street" in m and "added _run_id (string)" in m for m in warnings)
    assert any("crimes_street_quarantine" in m for m in warnings)
    for path, expected in (
        (config.SILVER_STREET_TABLE, silver.SILVER_SCHEMA),
        (config.SILVER_QUARANTINE_TABLE, silver.QUARANTINE_SCHEMA),
    ):
        written = spark.read.format("delta").load(str(path)).schema
        assert silver.diff_schemas(written, expected).is_empty()
    assert silver.assert_silver_consistent(spark) == stats["run_id"]


@pytest.mark.slow
def test_matching_schema_needs_no_flag(spark: SparkSession, tables: None) -> None:
    bronze(spark, mixed_rows()).write.format("delta").save(
        str(config.BRONZE_STREET_TABLE)
    )
    assert silver.existing_schema_diffs(spark) == {}  # no tables yet: no flag needed
    silver.run_silver(spark, max_quarantine_fraction=1.0)
    assert silver.existing_schema_diffs(spark) == {}
    silver.run_silver(
        spark, max_quarantine_fraction=1.0
    )  # second run, no flag, no error


def test_main_still_requires_all_when_flag_is_given() -> None:
    with pytest.raises(SystemExit) as exc:
        silver.main(["--allow-schema-change"])
    assert exc.value.code == 2


# ------------------------------------------------------------------- run summary


def test_run_summary_has_the_reconciliation_and_is_written_atomically(
    tmp_path: Path,
) -> None:
    summary = silver.build_run_summary(
        "run-9",
        datetime(2026, 10, 28, 12, 30, tzinfo=UTC),
        ["2026-08", "2026-07"],
        bronze_rows=10,
        classes={"silver": 7, "quarantined": 1, "deduped": 1, "superseded": 1},
        quarantine_reasons={"invalid_month": 1},
        asb_rows=2,
        no_location_rows=1,
        attribute_conflict_rows=0,
    )
    path = tmp_path / "out" / "run_summary.json"
    silver.write_run_summary(summary, path)

    import json

    assert json.loads(path.read_text()) == summary
    assert summary["archive_months"] == ["2026-07", "2026-08"]
    assert summary["created_at"] == "2026-10-28T12:30:00+00:00"
    assert not list(path.parent.glob("*.tmp"))


@pytest.mark.slow
def test_run_silver_writes_a_summary_that_matches_the_run(
    spark: SparkSession, tables: None
) -> None:
    import json

    bronze(spark, mixed_rows()).write.format("delta").save(
        str(config.BRONZE_STREET_TABLE)
    )
    stats = silver.run_silver(spark, max_quarantine_fraction=1.0)

    summary = json.loads(config.SILVER_RUN_SUMMARY.read_text())
    assert summary["silver_run_id"] == stats["run_id"]
    assert summary["bronze_rows"] == stats["bronze_rows"] == 9
    assert summary["classes"] == stats["classes"]
    assert summary["quarantine_reasons"] == {"invalid_month": 1}
    assert summary["archive_months"] == ["2026-08"]
    assert summary["asb_rows"] == 3
