"""Export a small snapshot of gold for the dashboard.

Reads the gold tables (after gold.assert_gold_consistent()) plus silver's run summary,
and writes a handful of small single-file Parquet tables and snapshot.json to
config.DASHBOARD_DATA_DIR (dashboard/data/, committed to git). The dashboard reads only
these files, so it needs no Spark and no data/ directory.
"""

import argparse
import json
import logging
import os
import shutil
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from uk_crime import config, gold
from uk_crime.silver import SilverError
from uk_crime.spark import get_spark

logger = logging.getLogger(__name__)

MAX_TOTAL_BYTES = 1_000_000  # "well under 1 MB": the export fails beyond this


class ExportError(RuntimeError):
    """An expected, reportable export failure (stale summary, snapshot too large)."""


# The columns each exported Parquet file contains (the dashboard relies on them).
EXPORT_COLUMNS: dict[str, list[str]] = {
    "mart_monthly_crimes": [
        "month", "force_key", "force_name", "crime_type", "crime_count",
    ],
    "mart_outcome_rates": [
        "crime_type", "outcome_group", "crime_count", "share_of_crime_type",
        "first_month", "last_month", "months_used", "excluded_months",
    ],
    "mart_lsoa_ranking": [
        "lsoa_code", "lsoa_name", "local_authority", "crime_count", "asb_count",
        "rank_in_local_authority", "first_month", "last_month", "latitude", "longitude",
    ],
    "mart_borough_monthly": [
        "local_authority", "month", "crime_type", "is_asb", "crime_count",
    ],
    "mart_borough_outcomes": [
        "local_authority", "crime_type", "outcome_group", "crime_count",
        "share_of_crime_type", "first_month", "last_month", "months_used",
        "excluded_months",
    ],
    "dq_volume_anomalies": [
        "force", "crime_type", "month", "crime_count", "baseline_median",
        "pct_change", "is_acknowledged", "note",
    ],
    "reconciliation": ["group", "item", "rows", "share"],
}  # fmt: skip
SNAPSHOT_FILE = "snapshot.json"

RECONCILIATION_SCHEMA = StructType(
    [
        StructField("group", StringType(), False),
        StructField("item", StringType(), False),
        StructField("rows", LongType(), False),
        StructField("share", DoubleType(), True),
    ]
)
CLASS_ORDER = ["silver", "quarantined", "deduped", "superseded", "archive_replaced"]


# ------------------------------------------------------------------ transformations


def lsoa_centroids(
    fct: DataFrame, dim_date: DataFrame, ranking_months: int
) -> DataFrame:
    """Mean latitude/longitude per LSOA over the newest `ranking_months` months.

    Uses fct_crimes rows that have coordinates, over the same window as
    mart_lsoa_ranking. Output columns: lsoa_code, latitude, longitude.
    """
    dated = fct.join(dim_date.select("month_key", "month"), "month_key")
    latest = dated.agg(F.max("month")).first()[0]
    if latest is None:
        return fct.sparkSession.createDataFrame(
            [],
            StructType(
                [
                    StructField("lsoa_code", StringType()),
                    StructField("latitude", DoubleType()),
                    StructField("longitude", DoubleType()),
                ]
            ),
        )
    window = dated.where(
        (F.col("month") > F.add_months(F.lit(latest), -ranking_months))
        & F.col("has_location")
        & (F.col("lsoa_code") != gold.UNKNOWN_LSOA)
    )
    return window.groupBy("lsoa_code").agg(
        F.avg("latitude").alias("latitude"), F.avg("longitude").alias("longitude")
    )


def reconciliation_table(
    spark: SparkSession,
    summary: dict[str, object],
    fct_rows: int,
    orphan_keys: int,
    anomalies: DataFrame,
) -> DataFrame:
    """Silver's reconciliation (from its run summary), two gold checks and the volume-anomaly
    counts (from the small dq_volume_anomalies table), as a table."""
    classes: dict[str, int] = summary["classes"]  # type: ignore[assignment]
    bronze_rows: int = summary["bronze_rows"]  # type: ignore[assignment]
    silver_rows = classes["silver"]
    rows: list[tuple[str, str, int, float | None]] = [
        ("reconciliation", "bronze_rows", bronze_rows, 1.0)
    ]
    rows += [
        ("reconciliation", c, classes[c], classes[c] / bronze_rows) for c in CLASS_ORDER
    ]
    reasons: dict[str, int] = summary["quarantine_reasons"]  # type: ignore[assignment]
    rows += [("quarantine", r, n, n / bronze_rows) for r, n in sorted(reasons.items())]
    for key in ("asb_rows", "no_location_rows", "attribute_conflict_rows"):
        n: int = summary[key]  # type: ignore[assignment]
        rows.append(("silver_flags", key, n, n / silver_rows))
    rows += [
        ("gold", "fct_crimes_rows", fct_rows, fct_rows / silver_rows),
        ("gold", "orphan_fact_keys", orphan_keys, None),
    ]
    flagged = anomalies.count()
    unacknowledged = anomalies.where(~F.col("is_acknowledged")).count()
    rows += [
        ("volume_anomalies", "flagged", flagged, None),
        ("volume_anomalies", "unacknowledged", unacknowledged, None),
    ]
    return spark.createDataFrame(rows, RECONCILIATION_SCHEMA)


def export_tables(
    tables: dict[str, DataFrame], summary: dict[str, object]
) -> dict[str, DataFrame]:
    """Build the exported tables from the gold tables and silver's run summary.

    Raises ExportError if the summary does not describe the silver run that gold was
    built from, or if the fact table does not match the summary's silver row count.
    """
    silver_run_id = tables["fct_crimes"].select("_silver_run_id").first()[0]
    if summary["silver_run_id"] != silver_run_id:
        raise ExportError(
            f"silver run summary is for run {summary['silver_run_id']} but gold was "
            f"built from run {silver_run_id}; re-run silver and gold"
        )
    fct = tables["fct_crimes"]
    fct_rows = fct.count()
    classes: dict[str, int] = summary["classes"]  # type: ignore[assignment]
    if fct_rows != classes["silver"]:
        raise ExportError(
            f"fct_crimes has {fct_rows} rows, silver run had {classes['silver']}"
        )
    orphans = sum(gold.check_referential_integrity(tables).values())

    ranking = tables["mart_lsoa_ranking"].join(
        lsoa_centroids(fct, tables["dim_date"], config.LSOA_RANKING_MONTHS),
        "lsoa_code",
        "left",
    )
    exports = {
        "mart_monthly_crimes": tables["mart_monthly_crimes"].orderBy(
            "month", "force_key", "crime_type"
        ),
        "mart_outcome_rates": tables["mart_outcome_rates"].orderBy(
            "crime_type", "outcome_group"
        ),
        "mart_lsoa_ranking": ranking.orderBy("lsoa_code"),
        "mart_borough_monthly": tables["mart_borough_monthly"].orderBy(
            "local_authority", "month", "crime_type", "is_asb"
        ),
        "mart_borough_outcomes": tables["mart_borough_outcomes"].orderBy(
            "local_authority", "crime_type", "outcome_group"
        ),
        "dq_volume_anomalies": tables["dq_volume_anomalies"].orderBy(
            "month", "force", "crime_type"
        ),
        "reconciliation": reconciliation_table(
            fct.sparkSession, summary, fct_rows, orphans, tables["dq_volume_anomalies"]
        ),
    }
    return {name: df.select(*EXPORT_COLUMNS[name]) for name, df in exports.items()}


def build_snapshot(
    exports: dict[str, DataFrame],
    summary: dict[str, object],
    exported_at: datetime,
    file_sizes: dict[str, int],
) -> dict[str, object]:
    """The contents of snapshot.json: provenance and the windows the marts cover."""
    monthly = exports["mart_monthly_crimes"].agg(F.min("month"), F.max("month")).first()
    outcome = (
        exports["mart_outcome_rates"]
        .select("first_month", "last_month", "months_used", "excluded_months")
        .first()
    )
    lsoa = exports["mart_lsoa_ranking"].select("first_month", "last_month").first()
    archive_months: list[str] = summary["archive_months"]  # type: ignore[assignment]
    excluded = outcome["excluded_months"] if outcome else ""
    return {
        "archive_month": max(archive_months),
        "archive_months": archive_months,
        "silver_run_id": summary["silver_run_id"],
        "silver_run_at": summary["created_at"],
        "exported_at": exported_at.astimezone(UTC).isoformat(timespec="seconds"),
        "months": {"first": f"{monthly[0]:%Y-%m}", "last": f"{monthly[1]:%Y-%m}"},
        "outcome": {
            "first_month": f"{outcome['first_month']:%Y-%m}" if outcome else None,
            "last_month": f"{outcome['last_month']:%Y-%m}" if outcome else None,
            "months_used": outcome["months_used"] if outcome else 0,
            "excluded_months": [m for m in excluded.split(",") if m],
            "maturity_months": config.OUTCOME_MATURITY_MONTHS,
        },
        "lsoa_window": {
            "first": f"{lsoa['first_month']:%Y-%m}" if lsoa else None,
            "last": f"{lsoa['last_month']:%Y-%m}" if lsoa else None,
        },
        "volume_anomaly": {
            "threshold_pct": config.VOLUME_ANOMALY_THRESHOLD_PCT,
            "min_baseline": config.VOLUME_ANOMALY_MIN_BASELINE,
        },
        "file_bytes": file_sizes,
    }


# ------------------------------------------------------------------------------ I/O


def write_single_parquet(df: DataFrame, dest: Path, work_dir: Path) -> int:
    """Write df as ONE Parquet file at dest (via a temp folder) and return its size.

    Spark writes a folder of part files, so the single part file is copied next to dest
    and renamed into place, which means a failed export never leaves a half-written file.
    """
    work = work_dir / f"export-{dest.stem}"
    shutil.rmtree(work, ignore_errors=True)
    df.coalesce(1).write.mode("overwrite").parquet(str(work))
    parts = list(work.glob("part-*.parquet"))
    if len(parts) != 1:
        raise ExportError(
            f"expected one Parquet part file for {dest.name}, got {len(parts)}"
        )
    dest.parent.mkdir(parents=True, exist_ok=True)
    temp = dest.with_suffix(".parquet.tmp")
    shutil.copyfile(parts[0], temp)
    os.replace(temp, dest)
    shutil.rmtree(work)
    return dest.stat().st_size


def read_run_summary(path: Path) -> dict[str, object]:
    """Read silver's run summary JSON; raise ExportError if it is missing."""
    if not path.is_file():
        raise ExportError(f"{path} not found; run silver (and gold) first")
    return json.loads(path.read_text())


def run_export(spark: SparkSession, out_dir: Path | None = None) -> dict[str, object]:
    """Export the dashboard snapshot. Returns the snapshot dictionary.

    Order: gold consistency check, read gold and the silver run summary, build the
    tables, write them, write snapshot.json, check the total size.
    """
    out_dir = out_dir or config.DASHBOARD_DATA_DIR
    gold.assert_gold_consistent(spark)
    summary = read_run_summary(config.SILVER_RUN_SUMMARY)
    tables = {
        name: spark.read.format("delta").load(str(gold.table_path(name)))
        for name in gold.TABLES
    }
    exports = export_tables(tables, summary)

    sizes = {
        f"{name}.parquet": write_single_parquet(
            df, out_dir / f"{name}.parquet", config.TMP_DIR
        )
        for name, df in exports.items()
    }
    snapshot = build_snapshot(exports, summary, datetime.now(UTC), sizes)
    snapshot_path = out_dir / SNAPSHOT_FILE
    temp = snapshot_path.with_suffix(".json.tmp")
    temp.write_text(json.dumps(snapshot, indent=2, sort_keys=True) + "\n")
    os.replace(temp, snapshot_path)

    total = sum(sizes.values()) + snapshot_path.stat().st_size
    for name, size in sizes.items():
        logger.info("%s %d bytes", name, size)
    logger.info("total %d bytes in %s", total, out_dir)
    if total > MAX_TOTAL_BYTES:
        raise ExportError(
            f"snapshot is {total} bytes, over the {MAX_TOTAL_BYTES} limit"
        )
    return snapshot


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point. Returns the process exit code (non-zero on failure)."""
    parser = argparse.ArgumentParser(
        prog="python -m uk_crime.export", description=__doc__
    )
    parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    spark = get_spark()
    try:
        run_export(spark)
    except (ExportError, gold.GoldError, SilverError) as err:
        logger.error("Export failed: %s", err)
        return 1
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
