"""Silver (T): typed, cleaned, deduplicated, quality-checked street crimes.

Silver is a full rebuild from bronze on every run (every archive restates all 36
months), written as one atomic Delta overwrite per table. Every bronze row ends up in
exactly one class: silver, quarantined, deduped, superseded or archive_replaced.

Pipeline order (each step is a plain DataFrame function):
 1. select_newest_archive  per (_force, _month) keep only the newest archive's rows
 2. clean_and_cast         trim, blank -> NULL, month -> DATE, coordinates -> DOUBLE
 3. check_vocabulary       unknown outcome / crime type RAISES (never quarantines)
 4. apply_row_dq           first failing rule -> _dq_reason
 5. resolve_duplicates     the R5 rule from CLAUDE.md section 5
 6. add_record_key         crime_id for crimes, deterministic sha2 for ASB
"""

import argparse
import logging
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from uk_crime import config
from uk_crime.bronze import SOURCE_BRONZE_NAMES
from uk_crime.spark import get_spark

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------- vocabulary

ASB_CRIME_TYPE = "Anti-social behaviour"
OUTCOME = "last_outcome_category"

# Outcomes that say nothing has progressed (NULL is also a placeholder).
PLACEHOLDER_OUTCOMES: tuple[str | None, ...] = (
    None,
    "Status update unavailable",
    "Under investigation",
)
# Every other value observed in archive 2026-08. "Awaiting court outcome" is progressed:
# it records a real action (the suspect was charged), not a missing status.
PROGRESSED_OUTCOMES: tuple[str, ...] = (
    "Investigation complete; no suspect identified",
    "Unable to prosecute suspect",
    "Court result unavailable",
    "Local resolution",
    "Awaiting court outcome",
    "Action to be taken by another organisation",
    "Offender given a caution",
    "Offender given penalty notice",
    "Formal action is not in the public interest",
    "Further investigation is not in the public interest",
    "Further action is not in the public interest",
    "Offender given a drugs possession warning",
    "Suspect charged as part of another case",
)
# Every crime type observed in archive 2026-08.
CRIME_TYPES: tuple[str, ...] = (
    ASB_CRIME_TYPE,
    "Bicycle theft",
    "Burglary",
    "Criminal damage and arson",
    "Drugs",
    "Other crime",
    "Other theft",
    "Possession of weapons",
    "Public order",
    "Robbery",
    "Shoplifting",
    "Theft from the person",
    "Vehicle crime",
    "Violence and sexual offences",
)

# ------------------------------------------------------------------ settings

# Coarse sanity box around the UK (Scilly to Shetland, Northern Ireland to Lowestoft).
MIN_LATITUDE, MAX_LATITUDE = 49.8, 61.0
MIN_LONGITUDE, MAX_LONGITUDE = -8.7, 1.9
MAX_QUARANTINE_FRACTION = 0.005

MONTH_PATTERN = r"^\d{4}-(0[1-9]|1[0-2])$"
NULL_SENTINEL = "\x1eNULL"  # control characters cannot occur in the CSV text
FIELD_SEPARATOR = "\x1f"

SOURCE = SOURCE_BRONZE_NAMES  # the 12 source columns, in order
METADATA = ["_corrupt_record", "_archive_month", "_month", "_force", "_source_file"]
SILVER_COLUMNS = [
    "record_key", "crime_id", "month", "force", "reported_by", "falls_within",
    "longitude", "latitude", "location", "lsoa_code", "lsoa_name", "crime_type",
    OUTCOME, "context", "is_asb", "has_location", "dq_attribute_conflict",
    "_archive_month", "_source_file", "_run_id",
]  # fmt: skip


def _schema(*columns: tuple[str, DataType]) -> StructType:
    return StructType([StructField(name, kind, True) for name, kind in columns])


# The expected schema of each output table, defined in code. A run whose existing table
# differs fails unless --allow-schema-change is given (see run_silver).
SILVER_SCHEMA = _schema(
    ("record_key", StringType()), ("crime_id", StringType()), ("month", DateType()),
    ("force", StringType()), ("reported_by", StringType()), ("falls_within", StringType()),
    ("longitude", DoubleType()), ("latitude", DoubleType()), ("location", StringType()),
    ("lsoa_code", StringType()), ("lsoa_name", StringType()), ("crime_type", StringType()),
    (OUTCOME, StringType()), ("context", StringType()), ("is_asb", BooleanType()),
    ("has_location", BooleanType()), ("dq_attribute_conflict", BooleanType()),
    ("_archive_month", DateType()), ("_source_file", StringType()), ("_run_id", StringType()),
)  # fmt: skip
QUARANTINE_SCHEMA = _schema(
    *[(c, StringType()) for c in SOURCE],
    ("_corrupt_record", StringType()), ("_archive_month", DateType()), ("_month", DateType()),
    ("_force", StringType()), ("_source_file", StringType()), ("_ingested_at", TimestampType()),
    ("_dq_reason", StringType()), ("_quarantined_at", TimestampType()), ("_run_id", StringType()),
)  # fmt: skip

DISPOSITIONS = ("silver", "quarantined", "deduped", "superseded")
DQ_REASONS = (
    "corrupt_record",
    "invalid_month",
    "unexpected_crime_id_presence",
    "invalid_coordinates",
    "conflicting_duplicate_crime_id",
)


class SilverError(RuntimeError):
    """An expected, reportable silver failure."""


class VocabularyError(SilverError):
    """A value outside PLACEHOLDER/PROGRESSED_OUTCOMES or CRIME_TYPES was seen."""


class ReconciliationError(SilverError):
    """Bronze rows do not equal the sum of the silver row classes."""


class ConsistencyError(SilverError):
    """The silver and quarantine tables do not come from the same run."""


class SchemaChangeError(SilverError):
    """An existing output table has a different schema and --allow-schema-change is off."""


class GuardrailError(SilverError):
    """One quarantine reason exceeds the allowed share of rows; nothing was written."""


@dataclass(frozen=True)
class SilverResult:
    """The three DataFrames produced by build_silver (all lazy)."""

    silver: DataFrame
    quarantine: DataFrame
    classified: DataFrame


# --------------------------------------------------------------- transformations


def select_newest_archive(bronze: DataFrame) -> DataFrame:
    """Keep, per (_force, _month), only the rows of the newest _archive_month."""
    newest = F.max("_archive_month").over(Window.partitionBy("_force", "_month"))
    return (
        bronze.withColumn("_newest", newest)
        .where(F.col("_archive_month") == F.col("_newest"))
        .drop("_newest")
    )


def clean_and_cast(df: DataFrame) -> DataFrame:
    """Trim strings, blank -> NULL, month -> DATE, longitude/latitude -> DOUBLE.

    The untouched bronze row is kept in a struct column `_raw` (used for quarantine).
    Unparseable month/coordinates become NULL; coordinate parse failures are flagged in
    _lon_bad/_lat_bad so they can be told apart from genuinely blank coordinates.
    try_cast / try_to_date are used because Spark runs in ANSI mode, where plain casts
    would raise on bad text.
    """
    bronze_columns = df.columns
    cleaned = {c: F.nullif(F.trim(F.col(c)), F.lit("")) for c in SOURCE}
    month_ok = cleaned["month"].rlike(MONTH_PATTERN)
    lon = cleaned["longitude"]
    lat = cleaned["latitude"]
    return df.select(
        F.struct(*[F.col(c) for c in bronze_columns]).alias("_raw"),
        *[
            cleaned[c].alias(c)
            for c in SOURCE
            if c not in ("month", "longitude", "latitude")
        ],
        F.when(
            month_ok, F.expr("try_to_date(concat(trim(month), '-01'), 'yyyy-MM-dd')")
        ).alias("month"),
        lon.try_cast("double").alias("longitude"),
        lat.try_cast("double").alias("latitude"),
        (lon.isNotNull() & lon.try_cast("double").isNull()).alias("_lon_bad"),
        (lat.isNotNull() & lat.try_cast("double").isNull()).alias("_lat_bad"),
        *[F.col(c) for c in METADATA],
    ).select(["_raw", *SOURCE, "_lon_bad", "_lat_bad", *METADATA])


def check_vocabulary(df: DataFrame) -> None:
    """Raise VocabularyError if an outcome or crime type is in no explicit list.

    Corrupt rows are skipped (their fields are legitimately NULL). Collects two tiny
    distinct-value sets, so it is safe on large data.
    """
    parsed = df.where(F.col("_corrupt_record").isNull())
    known_outcomes = {*PLACEHOLDER_OUTCOMES, *PROGRESSED_OUTCOMES}
    seen_outcomes = {r[0] for r in parsed.select(OUTCOME).distinct().collect()}
    unknown_outcomes = seen_outcomes - known_outcomes
    if unknown_outcomes:
        raise VocabularyError(
            f"Unknown {OUTCOME} value(s): {sorted(map(str, unknown_outcomes))}"
        )
    seen_types = {r[0] for r in parsed.select("crime_type").distinct().collect()}
    unknown_types = seen_types - set(CRIME_TYPES)
    if unknown_types:
        raise VocabularyError(
            f"Unknown crime_type value(s): {sorted(map(str, unknown_types))}"
        )


def apply_row_dq(df: DataFrame) -> DataFrame:
    """Add is_asb, has_location and _dq_reason (the FIRST failing rule, else NULL).

    Rule order is fixed: corrupt_record, invalid_month, unexpected_crime_id_presence,
    invalid_coordinates. NULL coordinates are valid (has_location = false).
    """
    lon, lat = F.col("longitude"), F.col("latitude")
    is_asb = F.coalesce(F.col("crime_type") == F.lit(ASB_CRIME_TYPE), F.lit(False))
    has_id = F.col("crime_id").isNotNull()
    inside_uk = lon.between(MIN_LONGITUDE, MAX_LONGITUDE) & lat.between(
        MIN_LATITUDE, MAX_LATITUDE
    )
    coordinates_invalid = (
        F.col("_lon_bad")
        | F.col("_lat_bad")
        | (lon.isNull() != lat.isNull())
        | (lon.isNotNull() & lat.isNotNull() & ~F.coalesce(inside_uk, F.lit(False)))
    )
    month_invalid = F.col("month").isNull() | (F.col("month") != F.col("_month"))
    reason = (
        F.when(F.col("_corrupt_record").isNotNull(), "corrupt_record")
        .when(month_invalid, "invalid_month")
        .when(is_asb == has_id, "unexpected_crime_id_presence")
        .when(coordinates_invalid, "invalid_coordinates")
    )
    return (
        df.withColumn("is_asb", is_asb)
        .withColumn("has_location", lon.isNotNull() & lat.isNotNull())
        .withColumn("_dq_reason", reason)
        .drop("_lon_bad", "_lat_bad")
    )


def resolve_duplicates(df: DataFrame) -> DataFrame:
    """Apply the R5 duplicate rule to DQ-passing crime rows; add _disposition.

    Per (_force, _month, crime_id): identical copies collapse (deduped); if variants
    differ and exactly one has a progressed outcome it is kept and the rest are
    superseded (dq_attribute_conflict marks attribute differences); otherwise every copy
    is quarantined. Rows that failed DQ are quarantined; ASB rows pass through.
    """
    passing = F.col("_dq_reason").isNull()
    crimes = df.where(passing & ~F.col("is_asb"))
    others = df.where(~(passing & ~F.col("is_asb")))

    by_crime = Window.partitionBy("_force", "_month", "crime_id")
    variant = F.struct(*[F.col(c) for c in SOURCE])
    attributes = F.struct(
        "crime_type", "location", "longitude", "latitude", "lsoa_code", "lsoa_name"
    )
    progressed = F.coalesce(F.col(OUTCOME).isin(*PROGRESSED_OUTCOMES), F.lit(False))
    copy_number = F.row_number().over(
        Window.partitionBy("_force", "_month", *SOURCE).orderBy(
            "_archive_month", "_source_file"
        )
    )

    measured = (
        crimes.withColumn("_n_variants", F.size(F.collect_set(variant).over(by_crime)))
        .withColumn(
            "_n_progressed",
            F.size(F.collect_set(F.when(progressed, variant)).over(by_crime)),
        )
        .withColumn("_n_attributes", F.size(F.collect_set(attributes).over(by_crime)))
        .withColumn("_copy", copy_number)
    )
    single = F.col("_n_variants") == 1
    resolvable = single | (F.col("_n_progressed") == 1)
    kept_variant = single | progressed
    disposition = (
        F.when(~resolvable, "quarantined")
        .when(~kept_variant, "superseded")
        .when(F.col("_copy") > 1, "deduped")
        .otherwise("silver")
    )
    resolved = (
        measured.withColumn("_disposition", disposition)
        .withColumn(
            "_dq_reason",
            F.when(~resolvable, F.lit("conflicting_duplicate_crime_id")),
        )
        .withColumn(
            "dq_attribute_conflict",
            (F.col("_disposition") == "silver") & (F.col("_n_attributes") > 1),
        )
        .drop("_n_variants", "_n_progressed", "_n_attributes", "_copy")
    )
    passthrough = others.withColumn(
        "_disposition",
        F.when(F.col("_dq_reason").isNotNull(), "quarantined").otherwise("silver"),
    ).withColumn("dq_attribute_conflict", F.lit(False))
    return resolved.unionByName(passthrough)


def add_record_key(df: DataFrame) -> DataFrame:
    """Add record_key: crime_id for crimes; deterministic sha2 for ASB rows.

    The ASB key hashes force, month, every source field (NULLs coalesced to a sentinel,
    because concat_ws would silently skip them) and an occurrence number, so identical
    ASB rows stay distinct and the same bronze always yields the same keys.
    """
    occurrence = F.row_number().over(
        Window.partitionBy("_force", "_month", *SOURCE).orderBy(
            "_archive_month", "_source_file"
        )
    )
    fields = [F.coalesce(F.col(c).cast("string"), F.lit(NULL_SENTINEL)) for c in SOURCE]
    asb_key = F.sha2(
        F.concat_ws(
            FIELD_SEPARATOR,
            F.col("_force"),
            F.col("_month").cast("string"),
            *fields,
            occurrence.cast("string"),
        ),
        256,
    )
    return df.withColumn(
        "record_key", F.when(F.col("is_asb"), asb_key).otherwise(F.col("crime_id"))
    )


def build_silver(
    bronze: DataFrame, quarantined_at: datetime, run_id: str
) -> SilverResult:
    """Run the whole transformation on a bronze DataFrame (no writes).

    Raises VocabularyError (triggers a small Spark job) on unknown outcome/crime type.
    quarantined_at and run_id stamp the output rows (one value per run); run_id is added
    to both tables so assert_silver_consistent can prove they were written together.
    """
    cleaned = clean_and_cast(select_newest_archive(bronze))
    check_vocabulary(cleaned)
    classified = resolve_duplicates(apply_row_dq(cleaned))

    silver = (
        add_record_key(classified.where(F.col("_disposition") == "silver"))
        .withColumn("_run_id", F.lit(run_id))
        .select(
            *[
                F.col("_force").alias("force") if c == "force" else F.col(c)
                for c in SILVER_COLUMNS
            ]
        )
    )
    quarantine = classified.where(F.col("_disposition") == "quarantined").select(
        "_raw.*",
        "_dq_reason",
        F.lit(quarantined_at).cast("timestamp").alias("_quarantined_at"),
        F.lit(run_id).alias("_run_id"),
    )
    return SilverResult(silver=silver, quarantine=quarantine, classified=classified)


# -------------------------------------------------------------------- accounting


def count_classes(classified: DataFrame) -> dict[str, int]:
    """Return rows per disposition (silver/quarantined/deduped/superseded)."""
    counts = {d: 0 for d in DISPOSITIONS}
    for row in classified.groupBy("_disposition").count().collect():
        counts[row["_disposition"]] = row["count"]
    return counts


def count_quarantine_reasons(classified: DataFrame) -> dict[str, int]:
    """Return quarantined rows per _dq_reason."""
    rows = (
        classified.where(F.col("_disposition") == "quarantined")
        .groupBy("_dq_reason")
        .count()
    )
    return {r["_dq_reason"]: r["count"] for r in rows.collect()}


def reconcile(
    bronze_rows: int, selected_rows: int, counts: dict[str, int]
) -> dict[str, int]:
    """Check bronze = silver + quarantined + deduped + superseded + archive_replaced.

    archive_replaced is computed independently (bronze minus rows of the newest archives),
    so the check proves the classification covered every selected row exactly once.
    """
    classes = {**counts, "archive_replaced": bronze_rows - selected_rows}
    if sum(classes.values()) != bronze_rows:
        raise ReconciliationError(
            f"bronze_rows={bronze_rows} but classes sum to {classes}"
        )
    return classes


def check_quarantine_guardrail(
    reasons: dict[str, int],
    total_rows: int,
    max_fraction: float = MAX_QUARANTINE_FRACTION,
) -> None:
    """Raise GuardrailError if any single quarantine reason exceeds max_fraction of rows."""
    over = {
        r: n for r, n in reasons.items() if total_rows and n / total_rows > max_fraction
    }
    if over:
        raise GuardrailError(
            f"Quarantine guardrail ({max_fraction:.2%} of {total_rows:,} rows) exceeded; "
            f"nothing written. Counts by reason: {reasons}"
        )


def checksums(silver: DataFrame) -> dict[str, int]:
    """Order-independent fingerprints of silver: rows, key checksum, full-row checksum.

    The row checksum covers every column except _run_id, so two runs can be compared.
    """
    total = F.sum(F.xxhash64("record_key").cast("decimal(38,0)"))
    content = [F.col(c) for c in silver.columns if c != "_run_id"]
    full = F.sum(F.xxhash64(*content).cast("decimal(38,0)"))
    row = silver.agg(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct("record_key").alias("distinct_keys"),
        total.alias("key_checksum"),
        full.alias("row_checksum"),
    ).first()
    return {
        "rows": row["rows"],
        "distinct_keys": row["distinct_keys"],
        "key_checksum": int(row["key_checksum"] or 0),
        "row_checksum": int(row["row_checksum"] or 0),
    }


# ------------------------------------------------------------------------ schemas


@dataclass(frozen=True)
class SchemaDiff:
    """Column differences between an existing table schema and the expected one."""

    added: dict[str, str]  # in the expected schema, not in the table
    removed: dict[str, str]  # in the table, not in the expected schema
    changed: dict[str, tuple[str, str]]  # name -> (table type, expected type)

    def is_empty(self) -> bool:
        return not (self.added or self.removed or self.changed)

    def describe(self) -> str:
        parts = [f"added {n} ({t})" for n, t in self.added.items()]
        parts += [f"removed {n} ({t})" for n, t in self.removed.items()]
        parts += [f"changed {n} ({a} -> {b})" for n, (a, b) in self.changed.items()]
        return "; ".join(parts)


def diff_schemas(existing: StructType, expected: StructType) -> SchemaDiff:
    """Compare column names and types (nullability and column order are ignored)."""
    have = {f.name: f.dataType.simpleString() for f in existing.fields}
    want = {f.name: f.dataType.simpleString() for f in expected.fields}
    return SchemaDiff(
        added={n: t for n, t in want.items() if n not in have},
        removed={n: t for n, t in have.items() if n not in want},
        changed={
            n: (have[n], want[n]) for n in want if n in have and have[n] != want[n]
        },
    )


def existing_schema_diffs(spark: SparkSession) -> dict[str, SchemaDiff]:
    """Return the non-empty schema diffs of the existing output tables (metadata only).

    A table that does not exist yet has no diff.
    """
    diffs: dict[str, SchemaDiff] = {}
    for name, path, expected in (
        ("crimes_street", config.SILVER_STREET_TABLE, SILVER_SCHEMA),
        ("crimes_street_quarantine", config.SILVER_QUARANTINE_TABLE, QUARANTINE_SCHEMA),
    ):
        if DeltaTable.isDeltaTable(spark, str(path)):
            diff = diff_schemas(
                spark.read.format("delta").load(str(path)).schema, expected
            )
            if not diff.is_empty():
                diffs[name] = diff
    return diffs


def check_output_schema(df: DataFrame, expected: StructType, name: str) -> None:
    """Raise SilverError if the DataFrame about to be written differs from the code's schema."""
    diff = diff_schemas(df.schema, expected)
    if not diff.is_empty():
        raise SilverError(
            f"{name} output does not match its expected schema: {diff.describe()}"
        )


# --------------------------------------------------------------------------- I/O


def overwrite_table(
    df: DataFrame, path: Path, run_id: str, overwrite_schema: bool = False
) -> None:
    """Atomically replace a Delta table, also tagging the commit with run_id.

    The commit tag lets assert_silver_consistent identify the run of an empty table.
    overwrite_schema is only ever True for an explicit --allow-schema-change run.
    """
    writer = df.write.format("delta").mode("overwrite").option("userMetadata", run_id)
    if overwrite_schema:
        writer = writer.option("overwriteSchema", "true")
    writer.save(str(path))


def table_run_ids(spark: SparkSession, path: Path) -> set[str]:
    """Return the run ids a table holds.

    That is the distinct _run_id values of its rows or, for an empty table, the run id on
    its latest Delta commit.
    """
    try:
        table = spark.read.format("delta").load(str(path))
    except AnalysisException as err:
        raise ConsistencyError(f"{path} is missing or not a Delta table") from err
    ids = {r[0] for r in table.select("_run_id").distinct().collect()}
    if ids:
        return ids
    latest = (
        DeltaTable.forPath(spark, str(path)).history(1).select("userMetadata").first()
    )
    return {latest[0]} if latest and latest[0] else set()


def assert_silver_consistent(spark: SparkSession) -> str:
    """Raise ConsistencyError unless silver and quarantine come from one and the same run.

    The two tables are written one after the other (not in one transaction), so a crash or
    a partial rewrite can leave them from different runs. Gold calls this first.
    Returns the shared _run_id.
    """
    found = {
        "crimes_street": table_run_ids(spark, config.SILVER_STREET_TABLE),
        "crimes_street_quarantine": table_run_ids(
            spark, config.SILVER_QUARANTINE_TABLE
        ),
    }
    all_ids = set().union(*found.values())
    if any(len(ids) != 1 for ids in found.values()) or len(all_ids) != 1:
        raise ConsistencyError(
            f"silver tables are not from one run; run ids found: {found}"
        )
    return all_ids.pop()


def run_silver(
    spark: SparkSession,
    max_quarantine_fraction: float = MAX_QUARANTINE_FRACTION,
    allow_schema_change: bool = False,
) -> dict[str, object]:
    """Rebuild silver and its quarantine table from the whole bronze table.

    Checks the existing tables' schemas first (SchemaChangeError unless
    allow_schema_change), reconciles and applies the quarantine guardrail BEFORE writing;
    each table is then replaced by one atomic Delta overwrite. Returns the run statistics
    (also logged).
    """
    schema_diffs = existing_schema_diffs(spark)
    if schema_diffs and not allow_schema_change:
        details = "; ".join(f"{n}: {d.describe()}" for n, d in schema_diffs.items())
        raise SchemaChangeError(
            f"Existing table schema differs from the expected schema ({details}). "
            "Nothing was written. Silver is fully derived from bronze, so re-run with "
            "--allow-schema-change to rebuild the table(s) with the new schema."
        )
    for name, diff in schema_diffs.items():
        logger.warning("Schema change allowed for %s: %s", name, diff.describe())

    bronze = spark.read.format("delta").load(str(config.BRONZE_STREET_TABLE))
    bronze_rows = bronze.count()
    selected_rows = select_newest_archive(bronze).count()
    run_id = str(uuid.uuid4())
    result = build_silver(bronze, datetime.now(UTC), run_id)
    classified = result.classified.cache()
    try:
        counts = count_classes(classified)
        reasons = count_quarantine_reasons(classified)
        classes = reconcile(bronze_rows, selected_rows, counts)
        logger.info("bronze_rows=%d reconciliation=%s", bronze_rows, classes)
        logger.info("quarantined by reason=%s", reasons)
        check_quarantine_guardrail(reasons, selected_rows, max_quarantine_fraction)

        check_output_schema(result.silver, SILVER_SCHEMA, "crimes_street")
        check_output_schema(
            result.quarantine, QUARANTINE_SCHEMA, "crimes_street_quarantine"
        )
        overwrite_table(
            result.quarantine,
            config.SILVER_QUARANTINE_TABLE,
            run_id,
            overwrite_schema="crimes_street_quarantine" in schema_diffs,
        )
        overwrite_table(
            result.silver,
            config.SILVER_STREET_TABLE,
            run_id,
            overwrite_schema="crimes_street" in schema_diffs,
        )
        if assert_silver_consistent(spark) != run_id:
            raise ConsistencyError(
                "silver tables hold a run id other than the one just written"
            )
        logger.info("run_id=%s written to both silver tables", run_id)

        written = spark.read.format("delta").load(str(config.SILVER_STREET_TABLE))
        fingerprint = checksums(written)
        if fingerprint["rows"] != classes["silver"]:
            raise ReconciliationError(
                f"wrote {fingerprint['rows']} silver rows, expected {classes['silver']}"
            )
        if fingerprint["distinct_keys"] != fingerprint["rows"]:
            raise SilverError(
                f"record_key is not unique: {fingerprint['distinct_keys']} distinct "
                f"keys for {fingerprint['rows']} rows"
            )
        flags = written.agg(
            F.sum((~F.col("has_location")).cast("int")).alias("no_location"),
            F.sum(F.col("dq_attribute_conflict").cast("int")).alias(
                "attribute_conflict"
            ),
            F.sum(F.col("is_asb").cast("int")).alias("asb"),
        ).first()
        logger.info(
            "silver rows=%d asb=%d has_location=false:%d dq_attribute_conflict=true:%d",
            fingerprint["rows"], flags["asb"], flags["no_location"], flags["attribute_conflict"],
        )  # fmt: skip
        logger.info(
            "fingerprint key_checksum=%d row_checksum=%d",
            fingerprint["key_checksum"], fingerprint["row_checksum"],
        )  # fmt: skip
    finally:
        classified.unpersist()
    return {
        "run_id": run_id,
        "bronze_rows": bronze_rows,
        "classes": classes,
        "quarantine_reasons": reasons,
        "no_location": flags["no_location"],
        "attribute_conflict": flags["attribute_conflict"],
        **fingerprint,
    }


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point. Returns the process exit code (non-zero on failure)."""
    parser = argparse.ArgumentParser(
        prog="python -m uk_crime.silver", description=__doc__
    )
    parser.add_argument(
        "--all", action="store_true", required=True, help="rebuild silver"
    )
    parser.add_argument(
        "--allow-schema-change",
        action="store_true",
        help="rebuild a table whose existing schema differs from the expected one",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    spark = get_spark()
    try:
        run_silver(spark, allow_schema_change=args.allow_schema_change)
    except SilverError as err:
        logger.error("Silver failed: %s", err)
        return 1
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
