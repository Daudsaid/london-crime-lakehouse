"""Profile outcome maturity and recommend OUTCOME_MATURITY_MONTHS.

Read-only analysis (not part of the pipeline). Reads silver and prints, for every month,
the share of non-ASB rows whose outcome is still a placeholder (NULL, "Under
investigation" or "Status update unavailable"), the baseline, and the recommended number
of most-recent months to leave out of the outcome mart.

    uv run python analyses/profile_outcome_maturity.py [--baseline-age 18] \
        [--tolerance-pp 2] [--outlier 2024-10]

Rule: exclude the smallest number of most-recent months (N) so that every remaining month
newer than the baseline window has a placeholder share within the tolerance of the
baseline. The baseline is the median share over months older than --baseline-age months;
the baseline months define the band, so they are not judged against it. Outlier months are
ignored when judging. Re-run when a new archive lands.
"""

import argparse
import statistics
from dataclasses import dataclass
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from uk_crime import config
from uk_crime.spark import get_spark


@dataclass(frozen=True)
class MonthShare:
    month: date
    age: int  # months before the latest month
    rows: int  # non-ASB rows
    placeholder_rows: int
    status_unavailable: int
    under_investigation: int
    null_outcome: int

    @property
    def share(self) -> float:
        return self.placeholder_rows / self.rows


def monthly_shares(spark: SparkSession) -> list[MonthShare]:
    """Placeholder-outcome counts per month for non-ASB silver rows (36 small rows)."""
    outcome = F.col("last_outcome_category")
    placeholder = outcome.isNull() | outcome.isin(
        "Under investigation", "Status update unavailable"
    )
    rows = (
        spark.read.format("delta")
        .load(str(config.SILVER_STREET_TABLE))
        .where(~F.col("is_asb"))
        .groupBy("month")
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum(placeholder.cast("int")).alias("placeholder"),
            F.sum((outcome == "Status update unavailable").cast("int")).alias("suu"),
            F.sum((outcome == "Under investigation").cast("int")).alias("ui"),
            F.sum(outcome.isNull().cast("int")).alias("null"),
        )
        .orderBy("month")
        .collect()
    )
    latest = rows[-1]["month"]
    return [
        MonthShare(
            month=r["month"],
            age=(latest.year - r["month"].year) * 12 + latest.month - r["month"].month,
            rows=r["rows"],
            placeholder_rows=r["placeholder"],
            status_unavailable=r["suu"],
            under_investigation=r["ui"],
            null_outcome=r["null"],
        )
        for r in rows
    ]


def baseline_share(months: list[MonthShare], baseline_age: int) -> float:
    """Median placeholder share over months older than baseline_age months."""
    return statistics.median(m.share for m in months if m.age > baseline_age)


def recommend_n(
    months: list[MonthShare],
    baseline: float,
    baseline_age: int,
    tolerance: float,
    outliers: set[str],
) -> tuple[int | None, list[tuple[int, float, MonthShare]]]:
    """Return (smallest N that satisfies the rule, per-N worst deviation).

    Judged months: age <= baseline_age (newer than the baseline window), age >= N, not an
    outlier. Returns None if no N up to the oldest judged month satisfies the rule.
    """
    judged = [
        m
        for m in months
        if m.age <= baseline_age and f"{m.month:%Y-%m}" not in outliers
    ]
    table: list[tuple[int, float, MonthShare]] = []
    recommended: int | None = None
    for n in range(baseline_age + 1):
        kept = [m for m in judged if m.age >= n]
        if not kept:
            break
        worst = max(kept, key=lambda m: abs(m.share - baseline))
        deviation = abs(worst.share - baseline)
        table.append((n, deviation, worst))
        if recommended is None and deviation <= tolerance:
            recommended = n
    return recommended, table


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-age", type=int, default=18)
    parser.add_argument("--tolerance-pp", type=float, default=2.0)
    parser.add_argument("--outlier", action="append", default=None, metavar="YYYY-MM")
    args = parser.parse_args()
    outliers = set(args.outlier if args.outlier is not None else ["2024-10"])
    tolerance = args.tolerance_pp / 100

    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")
    months = monthly_shares(spark)
    spark.stop()

    print(
        "month   | age | non-ASB rows | placeholder | share  | status unavailable | under investigation | NULL"
    )
    for m in months:
        mark = "  <- outlier" if f"{m.month:%Y-%m}" in outliers else ""
        print(
            f"{m.month:%Y-%m} | {m.age:3d} | {m.rows:12,d} | {m.placeholder_rows:11,d} | "
            f"{m.share * 100:5.1f}% | {m.status_unavailable:18,d} | {m.under_investigation:19,d} | "
            f"{m.null_outcome}{mark}"
        )

    baseline = baseline_share(months, args.baseline_age)
    base_months = [m for m in months if m.age > args.baseline_age]
    print(
        f"\nbaseline: median share over months older than {args.baseline_age} months "
        f"({base_months[0].month:%Y-%m} to {base_months[-1].month:%Y-%m}, "
        f"{len(base_months)} months) = {baseline * 100:.3f}%"
    )
    print(
        f"band: {baseline * 100:.2f}% +/- {args.tolerance_pp:g}pp = "
        f"{max(0.0, baseline - tolerance) * 100:.2f}% to {(baseline + tolerance) * 100:.2f}%"
    )
    outside = [
        f"{m.month:%Y-%m} ({m.share * 100:.1f}%)"
        for m in base_months
        if abs(m.share - baseline) > tolerance and f"{m.month:%Y-%m}" not in outliers
    ]
    print(f"baseline-window months outside the band (not judged): {outside or 'none'}")

    recommended, table = recommend_n(
        months, baseline, args.baseline_age, tolerance, outliers
    )
    total = sum(m.rows for m in months)
    print(
        "\nN  | newest month kept | worst kept month (share)   | deviation | within tolerance | non-ASB rows kept"
    )
    for n, deviation, worst in table:
        kept = [m for m in months if m.age >= n and f"{m.month:%Y-%m}" not in outliers]
        newest = max(m.month for m in months if m.age >= n)
        print(
            f"{n:2d} | {newest:%Y-%m}           | {worst.month:%Y-%m} ({worst.share * 100:4.1f}%)"
            f"        | {deviation * 100:6.2f}pp | {'yes' if deviation <= tolerance else 'no ':3s}"
            f"              | {sum(m.rows for m in kept):,d} of {total:,d}"
        )
    if recommended is None:
        print("\nrecommended OUTCOME_MATURITY_MONTHS: none satisfies the rule")
    else:
        print(
            f"\nrecommended OUTCOME_MATURITY_MONTHS = {recommended} "
            f"(configured: {config.OUTCOME_MATURITY_MONTHS}; excluded outliers: "
            f"{sorted(outliers) or 'none'}; configured: {list(config.OUTCOME_EXCLUDED_MONTHS)})"
        )


if __name__ == "__main__":
    main()
