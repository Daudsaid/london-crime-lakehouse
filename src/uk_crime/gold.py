"""Gold (T): star schema and marts built only from silver.

Full rebuild on every run: dimensions, fct_crimes and five marts, each written as one
atomic Delta overwrite and tagged with _silver_run_id (silver's _run_id) for lineage.
gold first calls silver.assert_silver_consistent(); every transformation is Spark SQL over
temp views wrapped in a function that takes and returns DataFrames.

Vocabulary mappings (force names, crime-type severity, outcome groups) are explicit; a
value in the data that is not mapped raises GoldError.
"""

import argparse
import logging
import re
from collections.abc import Sequence
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from uk_crime import config, silver
from uk_crime.silver import SilverError
from uk_crime.spark import get_spark

logger = logging.getLogger(__name__)

# ----------------------------------------------------------------- vocabulary

UNKNOWN_INT = -1
UNKNOWN_KEY = "unknown"
UNKNOWN_LSOA = "UNKNOWN"

# Slug -> display name, for the forces we ingest. Add a force here when you ingest it.
FORCE_NAMES: dict[str, str] = {
    "metropolitan": "Metropolitan Police Service",
    "city-of-london": "City of London Police",
}

# Severity tiers are a portfolio judgement, not an official police classification. They
# follow the logic of the ONS Crime Severity Score (harm to the person and offence
# seriousness): violence, sexual offences, robbery, burglary and weapons are high;
# offences against property or order with a victim or public impact are medium; volume
# crimes with low individual harm, and the unclassified "Other crime", are low.
SEVERITY_TIERS: dict[str, str] = {
    "Violence and sexual offences": "high",
    "Robbery": "high",
    "Burglary": "high",
    "Possession of weapons": "high",
    "Criminal damage and arson": "medium",
    "Drugs": "medium",
    "Theft from the person": "medium",
    "Vehicle crime": "medium",
    "Public order": "medium",
    "Anti-social behaviour": "low",
    "Bicycle theft": "low",
    "Other theft": "low",
    "Shoplifting": "low",
    "Other crime": "low",
}

# Outcome category -> outcome group. "pending" means no outcome yet. Status
# (placeholder/progressed) comes from the silver lists, not from here.
OUTCOME_GROUPS: dict[str, str] = {
    "Awaiting court outcome": "charged_or_court",
    "Court result unavailable": "charged_or_court",
    "Suspect charged as part of another case": "charged_or_court",
    "Local resolution": "out_of_court",
    "Offender given a caution": "out_of_court",
    "Offender given penalty notice": "out_of_court",
    "Offender given a drugs possession warning": "out_of_court",
    "Investigation complete; no suspect identified": "no_further_action",
    "Unable to prosecute suspect": "no_further_action",
    "Formal action is not in the public interest": "no_further_action",
    "Further investigation is not in the public interest": "no_further_action",
    "Further action is not in the public interest": "no_further_action",
    "Action to be taken by another organisation": "other_organisation",
    "Under investigation": "pending",
    "Status update unavailable": "pending",
}
# A NULL outcome has no category. For ASB it is not applicable; for any other crime it
# means no outcome has been recorded yet.
NOT_APPLICABLE_CATEGORY = "Not applicable"
NO_OUTCOME_CATEGORY = "No outcome recorded"

DIMENSIONS = ("dim_date", "dim_force", "dim_crime_type", "dim_outcome", "dim_lsoa")
TABLES = (
    *DIMENSIONS,
    "fct_crimes",
    "mart_monthly_crimes",
    "mart_outcome_rates",
    "mart_lsoa_ranking",
    "mart_borough_monthly",
    "mart_borough_outcomes",
)
MONTH_NAMES = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
]
MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
LSOA_NAME_RE = re.compile(r"^(.+) \d{3}[A-Z]$")

# The 32 London boroughs plus the City of London. dim_lsoa.is_london is a NAME match
# against local_authority (parsed from lsoa_name). Known limitation: an ONS LSOA -> local
# authority lookup would replace this list and the name parsing.
LONDON_AUTHORITIES: tuple[str, ...] = (
    "Barking and Dagenham", "Barnet", "Bexley", "Brent", "Bromley", "Camden",
    "City of London", "Croydon", "Ealing", "Enfield", "Greenwich", "Hackney",
    "Hammersmith and Fulham", "Haringey", "Harrow", "Havering", "Hillingdon",
    "Hounslow", "Islington", "Kensington and Chelsea", "Kingston upon Thames",
    "Lambeth", "Lewisham", "Merton", "Newham", "Redbridge", "Richmond upon Thames",
    "Southwark", "Sutton", "Tower Hamlets", "Waltham Forest", "Wandsworth",
    "Westminster",
)  # fmt: skip


class GoldError(RuntimeError):
    """An expected, reportable gold failure."""


class GoldConsistencyError(GoldError):
    """The gold tables do not all come from one silver run."""


class GoldSchemaChangeError(GoldError):
    """An existing gold table has a different schema and --allow-schema-change is off."""


class GoldReconciliationError(GoldError):
    """A gold table does not reconcile to its source or has broken keys."""


def _schema(*columns: tuple[str, DataType]) -> StructType:
    return StructType([StructField(name, kind, True) for name, kind in columns])


_LINEAGE = ("_silver_run_id", StringType())
SCHEMAS: dict[str, StructType] = {
    "dim_date": _schema(
        ("month_key", IntegerType()), ("month", DateType()), ("year", IntegerType()),
        ("month_num", IntegerType()), ("month_name", StringType()),
        ("quarter", IntegerType()), ("uk_financial_year", StringType()), _LINEAGE,
    ),
    "dim_force": _schema(
        ("force_key", StringType()), ("force_name", StringType()), _LINEAGE
    ),
    "dim_crime_type": _schema(
        ("crime_type_key", StringType()), ("crime_type", StringType()),
        ("is_asb", BooleanType()), ("severity_tier", StringType()), _LINEAGE,
    ),
    "dim_outcome": _schema(
        ("outcome_key", StringType()), ("outcome_category", StringType()),
        ("outcome_status", StringType()), ("outcome_group", StringType()), _LINEAGE,
    ),
    "dim_lsoa": _schema(
        ("lsoa_code", StringType()), ("lsoa_name", StringType()),
        ("local_authority", StringType()), ("is_london", BooleanType()), _LINEAGE,
    ),
    "fct_crimes": _schema(
        ("record_key", StringType()), ("month_key", IntegerType()),
        ("force_key", StringType()), ("crime_type_key", StringType()),
        ("outcome_key", StringType()), ("lsoa_code", StringType()),
        ("is_asb", BooleanType()), ("has_location", BooleanType()),
        ("dq_attribute_conflict", BooleanType()), ("longitude", DoubleType()),
        ("latitude", DoubleType()), _LINEAGE,
    ),
    "mart_monthly_crimes": _schema(
        ("month_key", IntegerType()), ("month", DateType()), ("force_key", StringType()),
        ("force_name", StringType()), ("crime_type_key", StringType()),
        ("crime_type", StringType()), ("crime_count", LongType()), _LINEAGE,
    ),
    "mart_outcome_rates": _schema(
        ("crime_type_key", StringType()), ("crime_type", StringType()),
        ("outcome_group", StringType()), ("crime_count", LongType()),
        ("share_of_crime_type", DoubleType()), ("first_month", DateType()),
        ("last_month", DateType()), ("months_used", IntegerType()),
        ("excluded_months", StringType()), _LINEAGE,
    ),
    "mart_lsoa_ranking": _schema(
        ("lsoa_code", StringType()), ("lsoa_name", StringType()),
        ("local_authority", StringType()), ("crime_count", LongType()),
        ("asb_count", LongType()), ("rank_in_local_authority", IntegerType()),
        ("first_month", DateType()),
        ("last_month", DateType()), _LINEAGE,
    ),
    "mart_borough_monthly": _schema(
        ("local_authority", StringType()), ("month", DateType()),
        ("crime_type", StringType()), ("is_asb", BooleanType()),
        ("crime_count", LongType()), _LINEAGE,
    ),
    "mart_borough_outcomes": _schema(
        ("local_authority", StringType()), ("crime_type", StringType()),
        ("outcome_group", StringType()), ("crime_count", LongType()),
        ("share_of_crime_type", DoubleType()), ("first_month", DateType()),
        ("last_month", DateType()), ("months_used", IntegerType()),
        ("excluded_months", StringType()), _LINEAGE,
    ),
}  # fmt: skip


def table_path(name: str) -> Path:
    return config.GOLD_DIR / name


# ------------------------------------------------------------------ helpers


def slug(column: str) -> str:
    """SQL expression turning a text column into a stable lower-case key."""
    return f"regexp_replace(lower({column}), '[^a-z0-9]+', '-')"


def _mapping_view(
    spark: SparkSession, name: str, mapping: dict[str, str], key: str, value: str
) -> None:
    schema = _schema((key, StringType()), (value, StringType()))
    spark.createDataFrame(list(mapping.items()), schema).createOrReplaceTempView(name)


def _distinct_values(crimes: DataFrame, column: str) -> set[str | None]:
    return {r[0] for r in crimes.select(column).distinct().collect()}


def _raise_if_unmapped(seen: set[str | None], mapped: set[str], what: str) -> None:
    unmapped = seen - mapped
    if unmapped:
        raise GoldError(f"Unmapped {what}: {sorted(map(str, unmapped))}")


def outcome_status(category: str | None) -> str:
    """placeholder or progressed, from the silver outcome lists."""
    if category in silver.PLACEHOLDER_OUTCOMES:
        return "placeholder"
    if category in silver.PROGRESSED_OUTCOMES:
        return "progressed"
    raise GoldError(f"Unmapped outcome category: {category!r}")


# --------------------------------------------------------------- dimensions


def build_dim_date(crimes: DataFrame) -> DataFrame:
    """One row per month from the first to the last month in silver, plus UNKNOWN."""
    spark = crimes.sparkSession
    crimes.createOrReplaceTempView("silver_crimes")
    names = ", ".join(f"'{m}'" for m in MONTH_NAMES)
    return spark.sql(f"""
        WITH bounds AS (SELECT min(month) AS lo, max(month) AS hi FROM silver_crimes),
        months AS (SELECT explode(sequence(lo, hi, interval 1 month)) AS month FROM bounds)
        SELECT CAST(date_format(month, 'yyyyMM') AS INT) AS month_key,
               month,
               year(month) AS year,
               month(month) AS month_num,
               element_at(array({names}), month(month)) AS month_name,
               quarter(month) AS quarter,
               -- UK financial year runs April to March: 2026-03 is 2025/26, 2026-04 is 2026/27
               CASE WHEN month(month) >= 4
                    THEN concat(year(month), '/', lpad(CAST((year(month) + 1) % 100 AS STRING), 2, '0'))
                    ELSE concat(year(month) - 1, '/', lpad(CAST(year(month) % 100 AS STRING), 2, '0'))
               END AS uk_financial_year
        FROM months
        UNION ALL
        SELECT {UNKNOWN_INT}, CAST(NULL AS DATE), CAST(NULL AS INT), CAST(NULL AS INT),
               'Unknown', CAST(NULL AS INT), 'Unknown'
    """)


def build_dim_force(crimes: DataFrame) -> DataFrame:
    """The mapped forces plus UNKNOWN; raises if silver holds an unmapped force."""
    spark = crimes.sparkSession
    _raise_if_unmapped(_distinct_values(crimes, "force"), set(FORCE_NAMES), "force")
    _mapping_view(spark, "force_names", FORCE_NAMES, "force_key", "force_name")
    return spark.sql(f"""
        SELECT force_key, force_name FROM force_names
        UNION ALL SELECT '{UNKNOWN_KEY}', 'Unknown'
    """)


def build_dim_crime_type(crimes: DataFrame) -> DataFrame:
    """Every mapped crime type with its severity tier, plus UNKNOWN."""
    spark = crimes.sparkSession
    _raise_if_unmapped(
        _distinct_values(crimes, "crime_type"), set(SEVERITY_TIERS), "crime_type"
    )
    _mapping_view(
        spark, "severity_tiers", SEVERITY_TIERS, "crime_type", "severity_tier"
    )
    return spark.sql(f"""
        SELECT {slug("crime_type")} AS crime_type_key, crime_type,
               crime_type = '{silver.ASB_CRIME_TYPE}' AS is_asb, severity_tier
        FROM severity_tiers
        UNION ALL SELECT '{UNKNOWN_KEY}', 'Unknown', false, 'unknown'
    """)


def outcome_members() -> dict[str, tuple[str, str]]:
    """Every outcome category -> (status, group), including the two NULL-outcome members."""
    members = {c: (outcome_status(c), g) for c, g in OUTCOME_GROUPS.items()}
    members[NOT_APPLICABLE_CATEGORY] = ("placeholder", "not_applicable")
    members[NO_OUTCOME_CATEGORY] = ("placeholder", "pending")
    return members


def build_dim_outcome(crimes: DataFrame) -> DataFrame:
    """Every mapped outcome, the NULL-outcome members, plus UNKNOWN.

    Raises if silver holds an outcome that is not in OUTCOME_GROUPS.
    """
    spark = crimes.sparkSession
    seen = _distinct_values(crimes, "last_outcome_category") - {None}
    _raise_if_unmapped(seen, set(OUTCOME_GROUPS), "last_outcome_category")
    rows = [(c, s, g) for c, (s, g) in outcome_members().items()]
    schema = _schema(
        ("outcome_category", StringType()),
        ("outcome_status", StringType()),
        ("outcome_group", StringType()),
    )
    spark.createDataFrame(rows, schema).createOrReplaceTempView("outcome_members")
    return spark.sql(f"""
        SELECT {slug("outcome_category")} AS outcome_key, outcome_category,
               outcome_status, outcome_group
        FROM outcome_members
        UNION ALL SELECT '{UNKNOWN_KEY}', 'Unknown', 'unknown', 'unknown'
    """)


def build_dim_lsoa(crimes: DataFrame) -> DataFrame:
    """One row per LSOA code in silver, with local authority and an is_london flag.

    The local authority is parsed from the name ("Hounslow 021D" -> "Hounslow"); raises if
    a name does not end in a 3-digit + letter code. is_london is true when the authority is
    in LONDON_AUTHORITIES. UNKNOWN covers rows with no LSOA (is_london false).
    """
    spark = crimes.sparkSession
    names = _distinct_values(crimes.where("lsoa_name IS NOT NULL"), "lsoa_name")
    bad = sorted(n for n in names if n is not None and not LSOA_NAME_RE.match(n))
    if bad:
        raise GoldError(f"Cannot parse local authority from lsoa_name: {bad[:5]}")
    _mapping_view(
        spark,
        "london_authorities",
        {a: a for a in LONDON_AUTHORITIES},
        "local_authority",
        "unused",
    )
    crimes.createOrReplaceTempView("silver_crimes")
    return spark.sql(f"""
        WITH lsoas AS (
            SELECT lsoa_code, min(lsoa_name) AS lsoa_name,
                   regexp_extract(min(lsoa_name), '^(.+) [0-9]{{3}}[A-Z]$', 1) AS local_authority
            FROM silver_crimes
            WHERE lsoa_code IS NOT NULL AND lsoa_name IS NOT NULL
            GROUP BY lsoa_code
        )
        SELECT lsoa_code, lsoa_name, local_authority,
               local_authority IN (SELECT local_authority FROM london_authorities) AS is_london
        FROM lsoas
        UNION ALL SELECT '{UNKNOWN_LSOA}', 'Unknown', 'Unknown', false
    """)


# --------------------------------------------------------------------- fact


def build_fct_crimes(crimes: DataFrame) -> DataFrame:
    """One row per silver record, with surrogate/dimension keys and the flags."""
    crimes.createOrReplaceTempView("silver_crimes")
    return crimes.sparkSession.sql(f"""
        SELECT record_key,
               CAST(date_format(month, 'yyyyMM') AS INT) AS month_key,
               force AS force_key,
               {slug("crime_type")} AS crime_type_key,
               CASE WHEN last_outcome_category IS NOT NULL
                         THEN {slug("last_outcome_category")}
                    WHEN is_asb THEN '{slug_value(NOT_APPLICABLE_CATEGORY)}'
                    ELSE '{slug_value(NO_OUTCOME_CATEGORY)}' END AS outcome_key,
               coalesce(lsoa_code, '{UNKNOWN_LSOA}') AS lsoa_code,
               is_asb, has_location, dq_attribute_conflict, longitude, latitude
        FROM silver_crimes
    """)


def slug_value(text: str) -> str:
    """Python twin of slug(): the key a category text turns into."""
    return re.sub(r"[^a-z0-9]+", "-", text.lower())


# -------------------------------------------------------------------- marts


def _register(**views: DataFrame) -> SparkSession:
    spark = next(iter(views.values())).sparkSession
    for name, df in views.items():
        df.createOrReplaceTempView(name)
    return spark


def build_mart_monthly_crimes(
    fct: DataFrame, dim_date: DataFrame, dim_force: DataFrame, dim_crime_type: DataFrame
) -> DataFrame:
    """Crime count per month, force and crime type (ASB included)."""
    spark = _register(
        fct_crimes=fct,
        dim_date=dim_date,
        dim_force=dim_force,
        dim_crime_type=dim_crime_type,
    )
    return spark.sql("""
        SELECT f.month_key, d.month, f.force_key, fo.force_name,
               f.crime_type_key, ct.crime_type, count(*) AS crime_count
        FROM fct_crimes f
        JOIN dim_date d ON d.month_key = f.month_key
        JOIN dim_force fo ON fo.force_key = f.force_key
        JOIN dim_crime_type ct ON ct.crime_type_key = f.crime_type_key
        GROUP BY f.month_key, d.month, f.force_key, fo.force_name,
                 f.crime_type_key, ct.crime_type
    """)


def _excluded_months_sql(excluded_months: Sequence[str]) -> tuple[list[str], str]:
    """Validate YYYY-MM exclusions; return them sorted and as a SQL condition on f.month."""
    bad = [m for m in excluded_months if not MONTH_RE.match(m)]
    if bad:
        raise GoldError(f"excluded months must look like YYYY-MM: {bad}")
    excluded = sorted(set(excluded_months))
    if not excluded:
        return excluded, ""
    listed = ", ".join(f"'{m}'" for m in excluded)
    return excluded, f"AND date_format(f.month, 'yyyy-MM') NOT IN ({listed})"


def build_mart_outcome_rates(
    fct: DataFrame,
    dim_date: DataFrame,
    dim_crime_type: DataFrame,
    dim_outcome: DataFrame,
    maturity_months: int,
    excluded_months: Sequence[str] = (),
) -> DataFrame:
    """Outcome-group counts and shares per crime type.

    Excludes ASB (no outcomes), the newest `maturity_months` months (outcomes not mature
    yet) and the `excluded_months` (YYYY-MM outliers). first_month/last_month/months_used
    record the months actually used and excluded_months the listed exclusions.
    """
    excluded, exclusion_sql = _excluded_months_sql(excluded_months)
    spark = _register(
        fct_crimes=fct, dim_date=dim_date, dim_crime_type=dim_crime_type,
        dim_outcome=dim_outcome,
    )  # fmt: skip
    return spark.sql(f"""
        WITH f AS (
            SELECT fc.*, d.month FROM fct_crimes fc
            JOIN dim_date d ON d.month_key = fc.month_key
        ),
        latest AS (SELECT max(month) AS latest FROM f),
        mature AS (
            SELECT f.* FROM f CROSS JOIN latest
            WHERE NOT f.is_asb AND f.month <= add_months(latest.latest, -{int(maturity_months)})
              {exclusion_sql}
        ),
        counted AS (
            SELECT m.crime_type_key, o.outcome_group, count(*) AS crime_count,
                   min(m.month) AS first_month, max(m.month) AS last_month
            FROM mature m JOIN dim_outcome o ON o.outcome_key = m.outcome_key
            GROUP BY m.crime_type_key, o.outcome_group
        )
        SELECT c.crime_type_key, ct.crime_type, c.outcome_group, c.crime_count,
               c.crime_count / sum(c.crime_count) OVER (PARTITION BY c.crime_type_key)
                   AS share_of_crime_type,
               min(c.first_month) OVER () AS first_month,
               max(c.last_month) OVER () AS last_month,
               CAST((SELECT count(DISTINCT month) FROM mature) AS INT) AS months_used,
               '{",".join(excluded)}' AS excluded_months
        FROM counted c JOIN dim_crime_type ct ON ct.crime_type_key = c.crime_type_key
    """)


def build_mart_lsoa_ranking(
    fct: DataFrame, dim_date: DataFrame, dim_lsoa: DataFrame, ranking_months: int
) -> DataFrame:
    """Crimes per London LSOA over the newest `ranking_months` months, ranked in its authority.

    London LSOAs only (dim_lsoa.is_london). crime_count excludes ASB and asb_count is its
    own column; the rank is on crime_count, ties share a rank. Counts, not rates (no
    population data yet).
    """
    spark = _register(fct_crimes=fct, dim_date=dim_date, dim_lsoa=dim_lsoa)
    months = int(ranking_months)
    return spark.sql(f"""
        WITH f AS (
            SELECT fc.*, d.month FROM fct_crimes fc
            JOIN dim_date d ON d.month_key = fc.month_key
        ),
        latest AS (SELECT max(month) AS latest FROM f),
        recent AS (
            SELECT f.* FROM f CROSS JOIN latest
            JOIN dim_lsoa l ON l.lsoa_code = f.lsoa_code
            WHERE f.month > add_months(latest.latest, -{months}) AND l.is_london
        ),
        counted AS (
            SELECT lsoa_code,
                   count(CASE WHEN NOT is_asb THEN 1 END) AS crime_count,
                   count(CASE WHEN is_asb THEN 1 END) AS asb_count
            FROM recent GROUP BY lsoa_code
        )
        SELECT c.lsoa_code, l.lsoa_name, l.local_authority, c.crime_count, c.asb_count,
               CAST(rank() OVER (PARTITION BY l.local_authority
                                 ORDER BY c.crime_count DESC) AS INT) AS rank_in_local_authority,
               (SELECT add_months(latest, {1 - months}) FROM latest) AS first_month,
               (SELECT latest FROM latest) AS last_month
        FROM counted c JOIN dim_lsoa l ON l.lsoa_code = c.lsoa_code
    """)


def build_mart_borough_monthly(
    fct: DataFrame, dim_date: DataFrame, dim_lsoa: DataFrame, dim_crime_type: DataFrame
) -> DataFrame:
    """Record counts per London borough, month and crime type (ASB flagged, not removed).

    London LSOAs only (dim_lsoa.is_london); rows outside London or with no LSOA are not
    in this mart. Counts, not rates.
    """
    spark = _register(
        fct_crimes=fct,
        dim_date=dim_date,
        dim_lsoa=dim_lsoa,
        dim_crime_type=dim_crime_type,
    )
    return spark.sql("""
        SELECT l.local_authority, d.month, ct.crime_type, f.is_asb, count(*) AS crime_count
        FROM fct_crimes f
        JOIN dim_date d ON d.month_key = f.month_key
        JOIN dim_lsoa l ON l.lsoa_code = f.lsoa_code AND l.is_london
        JOIN dim_crime_type ct ON ct.crime_type_key = f.crime_type_key
        GROUP BY l.local_authority, d.month, ct.crime_type, f.is_asb
    """)


def build_mart_borough_outcomes(
    fct: DataFrame,
    dim_date: DataFrame,
    dim_lsoa: DataFrame,
    dim_crime_type: DataFrame,
    dim_outcome: DataFrame,
    maturity_months: int,
    excluded_months: Sequence[str] = (),
) -> DataFrame:
    """Outcome-group counts and shares per London borough and crime type.

    The same window as mart_outcome_rates (non-ASB, the newest `maturity_months` months
    and the `excluded_months` left out), restricted to London LSOAs. The share is within
    a borough and crime type.
    """
    excluded, exclusion_sql = _excluded_months_sql(excluded_months)
    spark = _register(
        fct_crimes=fct, dim_date=dim_date, dim_lsoa=dim_lsoa,
        dim_crime_type=dim_crime_type, dim_outcome=dim_outcome,
    )  # fmt: skip
    return spark.sql(f"""
        WITH f AS (
            SELECT fc.*, d.month FROM fct_crimes fc
            JOIN dim_date d ON d.month_key = fc.month_key
        ),
        latest AS (SELECT max(month) AS latest FROM f),
        mature AS (
            SELECT f.*, l.local_authority FROM f CROSS JOIN latest
            JOIN dim_lsoa l ON l.lsoa_code = f.lsoa_code AND l.is_london
            WHERE NOT f.is_asb AND f.month <= add_months(latest.latest, -{int(maturity_months)})
              {exclusion_sql}
        ),
        counted AS (
            SELECT m.local_authority, m.crime_type_key, o.outcome_group, count(*) AS crime_count,
                   min(m.month) AS first_month, max(m.month) AS last_month
            FROM mature m JOIN dim_outcome o ON o.outcome_key = m.outcome_key
            GROUP BY m.local_authority, m.crime_type_key, o.outcome_group
        )
        SELECT c.local_authority, ct.crime_type, c.outcome_group, c.crime_count,
               c.crime_count / sum(c.crime_count)
                   OVER (PARTITION BY c.local_authority, c.crime_type_key)
                   AS share_of_crime_type,
               min(c.first_month) OVER () AS first_month,
               max(c.last_month) OVER () AS last_month,
               CAST((SELECT count(DISTINCT month) FROM mature) AS INT) AS months_used,
               '{",".join(excluded)}' AS excluded_months
        FROM counted c JOIN dim_crime_type ct ON ct.crime_type_key = c.crime_type_key
    """)


# ---------------------------------------------------------------- orchestration


def build_gold(crimes: DataFrame) -> dict[str, DataFrame]:
    """Build every gold table (without lineage columns) from the silver crimes DataFrame."""
    dim_date = build_dim_date(crimes)
    dim_force = build_dim_force(crimes)
    dim_crime_type = build_dim_crime_type(crimes)
    dim_outcome = build_dim_outcome(crimes)
    dim_lsoa = build_dim_lsoa(crimes)
    fct = build_fct_crimes(crimes)
    return {
        "dim_date": dim_date,
        "dim_force": dim_force,
        "dim_crime_type": dim_crime_type,
        "dim_outcome": dim_outcome,
        "dim_lsoa": dim_lsoa,
        "fct_crimes": fct,
        "mart_monthly_crimes": build_mart_monthly_crimes(
            fct, dim_date, dim_force, dim_crime_type
        ),
        "mart_outcome_rates": build_mart_outcome_rates(
            fct,
            dim_date,
            dim_crime_type,
            dim_outcome,
            config.OUTCOME_MATURITY_MONTHS,
            config.OUTCOME_EXCLUDED_MONTHS,
        ),
        "mart_lsoa_ranking": build_mart_lsoa_ranking(
            fct, dim_date, dim_lsoa, config.LSOA_RANKING_MONTHS
        ),
        "mart_borough_monthly": build_mart_borough_monthly(
            fct, dim_date, dim_lsoa, dim_crime_type
        ),
        "mart_borough_outcomes": build_mart_borough_outcomes(
            fct,
            dim_date,
            dim_lsoa,
            dim_crime_type,
            dim_outcome,
            config.OUTCOME_MATURITY_MONTHS,
            config.OUTCOME_EXCLUDED_MONTHS,
        ),
    }


def with_lineage(
    tables: dict[str, DataFrame], silver_run_id: str
) -> dict[str, DataFrame]:
    """Add _silver_run_id to every table."""
    return {
        n: df.withColumn("_silver_run_id", F.lit(silver_run_id))
        for n, df in tables.items()
    }


FACT_KEYS = {
    "month_key": "dim_date",
    "force_key": "dim_force",
    "crime_type_key": "dim_crime_type",
    "outcome_key": "dim_outcome",
    "lsoa_code": "dim_lsoa",
}
DIMENSION_KEYS = {
    "dim_date": "month_key",
    "dim_force": "force_key",
    "dim_crime_type": "crime_type_key",
    "dim_outcome": "outcome_key",
    "dim_lsoa": "lsoa_code",
}


def check_referential_integrity(tables: dict[str, DataFrame]) -> dict[str, int]:
    """Return, per fact key, the number of fact rows whose key is missing from its dimension."""
    fct = tables["fct_crimes"]
    return {
        f"fct_crimes.{key} -> {dim}": fct.join(
            tables[dim], fct[key] == tables[dim][DIMENSION_KEYS[dim]], "left_anti"
        ).count()
        for key, dim in FACT_KEYS.items()
    }


def reconcile_gold(
    tables: dict[str, DataFrame],
    silver_rows: int,
    outcome_months: int,
    ranking_months: int,
    excluded_months: Sequence[str] = (),
) -> dict[str, int]:
    """Check keys, referential integrity and that facts and marts reconcile.

    Returns the checked numbers (also logged by the caller). Raises GoldReconciliationError
    on the first discrepancy.
    """
    fct, dim_date = tables["fct_crimes"], tables["dim_date"]
    stats: dict[str, int] = {"silver_rows": silver_rows, "fct_rows": fct.count()}

    for dim, key in DIMENSION_KEYS.items():
        rows = tables[dim].count()
        distinct = tables[dim].select(key).distinct().count()
        if rows != distinct or tables[dim].where(F.col(key).isNull()).count():
            raise GoldReconciliationError(f"{dim}.{key} is not unique/non-null")
    if stats["fct_rows"] != silver_rows:
        raise GoldReconciliationError(
            f"fct_crimes {stats['fct_rows']} != silver {silver_rows}"
        )
    if fct.select("record_key").distinct().count() != silver_rows:
        raise GoldReconciliationError("fct_crimes.record_key is not unique")
    broken = {k: n for k, n in check_referential_integrity(tables).items() if n}
    if broken:
        raise GoldReconciliationError(f"Orphan fact keys: {broken}")

    dated = fct.join(dim_date.select("month_key", "month"), "month_key")
    latest = dated.agg(F.max("month")).first()[0]
    monthly = tables["mart_monthly_crimes"].agg(F.sum("crime_count")).first()[0] or 0
    if monthly != silver_rows:
        raise GoldReconciliationError(
            f"mart_monthly_crimes {monthly} != fct {silver_rows}"
        )
    stats["mart_monthly_crimes"] = monthly

    if latest is not None:
        non_asb = dated.where(~F.col("is_asb"))
        cutoff = F.add_months(F.lit(latest), -outcome_months)
        listed = F.date_format("month", "yyyy-MM").isin(*excluded_months)
        in_window = F.col("month") <= cutoff
        mature = non_asb.where(in_window & ~listed).count()
        immature = non_asb.where(~in_window).count()
        excluded_rows = non_asb.where(in_window & listed).count()
        if mature + immature + excluded_rows != non_asb.count():
            raise GoldReconciliationError(
                "non-ASB rows do not split into mature/immature/excluded"
            )
        mart = tables["mart_outcome_rates"]
        rates = mart.agg(F.sum("crime_count")).first()[0] or 0
        if rates != mature:
            raise GoldReconciliationError(
                f"mart_outcome_rates {rates} != fct (mature, non-ASB, not excluded) {mature}"
            )
        used = non_asb.where(in_window & ~listed).select("month").distinct().count()
        meta = mart.select("months_used", "excluded_months").distinct().collect()
        if meta and [tuple(r) for r in meta] != [
            (used, ",".join(sorted(set(excluded_months))))
        ]:
            raise GoldReconciliationError(
                f"mart_outcome_rates months metadata {meta} != {used}"
            )
        stats.update(
            non_asb_rows=non_asb.count(),
            mart_outcome_rates=rates,
            outcome_rows_immature=immature,
            outcome_rows_excluded_months=excluded_rows,
            outcome_months_used=used,
        )

        # Borough marts: London LSOAs only, so rows outside London and rows with no LSOA
        # are the difference to the fact table.
        located = dated.join(
            tables["dim_lsoa"].select("lsoa_code", "is_london"), "lsoa_code"
        )
        non_london = located.where(
            (F.col("lsoa_code") != UNKNOWN_LSOA) & (~F.col("is_london"))
        ).count()
        unknown_lsoa = located.where(F.col("lsoa_code") == UNKNOWN_LSOA).count()
        borough_rows = (
            tables["mart_borough_monthly"].agg(F.sum("crime_count")).first()[0] or 0
        )
        if borough_rows + non_london + unknown_lsoa != silver_rows:
            raise GoldReconciliationError(
                f"mart_borough_monthly {borough_rows} + non-London {non_london} + "
                f"unknown LSOA {unknown_lsoa} != fct {silver_rows}"
            )
        mature_not_london = located.where(
            (~F.col("is_asb")) & in_window & ~listed & ~F.col("is_london")
        ).count()
        borough_outcomes = (
            tables["mart_borough_outcomes"].agg(F.sum("crime_count")).first()[0] or 0
        )
        if borough_outcomes != mature - mature_not_london:
            raise GoldReconciliationError(
                f"mart_borough_outcomes {borough_outcomes} != mature non-ASB rows "
                f"{mature} minus non-London/unknown-LSOA rows {mature_not_london}"
            )
        stats.update(
            borough_monthly_rows=borough_rows,
            non_london_rows=non_london,
            unknown_lsoa_rows_all_months=unknown_lsoa,
            borough_outcome_rows=borough_outcomes,
            mature_non_london_rows=mature_not_london,
        )

        recent = dated.where(
            F.col("month") > F.add_months(F.lit(latest), -ranking_months)
        ).join(tables["dim_lsoa"].select("lsoa_code", "is_london"), "lsoa_code")
        window_rows = recent.count()
        unknown = recent.where(F.col("lsoa_code") == UNKNOWN_LSOA).count()
        outside = recent.where(
            (F.col("lsoa_code") != UNKNOWN_LSOA) & (~F.col("is_london"))
        ).count()
        ranking = (
            tables["mart_lsoa_ranking"]
            .agg(F.sum("crime_count"), F.sum("asb_count"))
            .first()
        )
        ranked_crimes, ranked_asb = ranking[0] or 0, ranking[1] or 0
        if ranked_crimes + ranked_asb + outside + unknown != window_rows:
            raise GoldReconciliationError(
                f"mart_lsoa_ranking {ranked_crimes} + asb {ranked_asb} + outside London "
                f"{outside} + unknown LSOA {unknown} != window rows {window_rows}"
            )
        london = recent.where(F.col("is_london"))
        if ranked_asb != london.where(F.col("is_asb")).count() or ranked_crimes != (
            london.where(~F.col("is_asb")).count()
        ):
            raise GoldReconciliationError(
                "mart_lsoa_ranking ASB/non-ASB split is wrong"
            )
        stats.update(
            window_rows=window_rows,
            ranked_crimes=ranked_crimes,
            ranked_asb=ranked_asb,
            outside_london_rows=outside,
            unknown_lsoa_rows=unknown,
        )
    return stats


def existing_schema_diffs(spark: SparkSession) -> dict[str, silver.SchemaDiff]:
    """Return the non-empty schema diffs of existing gold tables (metadata only)."""
    diffs: dict[str, silver.SchemaDiff] = {}
    for name in TABLES:
        path = table_path(name)
        if DeltaTable.isDeltaTable(spark, str(path)):
            diff = silver.diff_schemas(
                spark.read.format("delta").load(str(path)).schema, SCHEMAS[name]
            )
            if not diff.is_empty():
                diffs[name] = diff
    return diffs


def assert_gold_consistent(spark: SparkSession) -> str:
    """Raise GoldConsistencyError unless every gold table holds one and the same _silver_run_id.

    Returns the shared silver run id.
    """
    found: dict[str, set[str]] = {}
    for name in TABLES:
        try:
            found[name] = silver.table_run_ids(
                spark, table_path(name), "_silver_run_id"
            )
        except silver.ConsistencyError as err:
            raise GoldConsistencyError(str(err)) from err
    all_ids = set().union(*found.values())
    if any(len(ids) != 1 for ids in found.values()) or len(all_ids) != 1:
        raise GoldConsistencyError(f"gold tables are not from one silver run: {found}")
    return all_ids.pop()


def table_fingerprint(df: DataFrame) -> dict[str, int]:
    """Order-independent rows + checksum of a table (includes _silver_run_id)."""
    row = df.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.xxhash64(*[F.col(c) for c in df.columns]).cast("decimal(38,0)")).alias(
            "checksum"
        ),
    ).first()
    return {"rows": row["rows"], "checksum": int(row["checksum"] or 0)}


def run_gold(
    spark: SparkSession, allow_schema_change: bool = False
) -> dict[str, object]:
    """Rebuild every gold table from silver.

    Order: silver consistency check, schema check, build, reconcile, write (one atomic
    overwrite per table), gold consistency check. Returns the run statistics.
    """
    silver_run_id = silver.assert_silver_consistent(spark)

    diffs = existing_schema_diffs(spark)
    if diffs and not allow_schema_change:
        details = "; ".join(f"{n}: {d.describe()}" for n, d in diffs.items())
        raise GoldSchemaChangeError(
            f"Existing gold table schema differs from the expected schema ({details}). "
            "Nothing was written. Gold is fully derived from silver, so re-run with "
            "--allow-schema-change to rebuild the table(s) with the new schema."
        )
    for name, diff in diffs.items():
        logger.warning("Schema change allowed for %s: %s", name, diff.describe())

    crimes = spark.read.format("delta").load(str(config.SILVER_STREET_TABLE))
    silver_rows = crimes.count()
    tables = build_gold(crimes)
    stats = reconcile_gold(
        tables,
        silver_rows,
        config.OUTCOME_MATURITY_MONTHS,
        config.LSOA_RANKING_MONTHS,
        config.OUTCOME_EXCLUDED_MONTHS,
    )
    outcome_mart = tables["mart_outcome_rates"].first()
    if outcome_mart is not None:
        logger.info(
            "mart_outcome_rates: months %s to %s (%d used), excluded months: %s, "
            "newest %d months left out as immature",
            outcome_mart["first_month"], outcome_mart["last_month"],
            outcome_mart["months_used"], outcome_mart["excluded_months"] or "none",
            config.OUTCOME_MATURITY_MONTHS,
        )  # fmt: skip
    tables = with_lineage(tables, silver_run_id)
    for name, df in tables.items():
        silver.check_output_schema(df, SCHEMAS[name], name)

    counts: dict[str, int] = {}
    for name, df in tables.items():
        silver.overwrite_table(
            df, table_path(name), silver_run_id, overwrite_schema=name in diffs
        )
        counts[name] = spark.read.format("delta").load(str(table_path(name))).count()
        logger.info("%s rows=%d", name, counts[name])
    if assert_gold_consistent(spark) != silver_run_id:
        raise GoldConsistencyError(
            "gold holds a silver run id other than the one just used"
        )
    logger.info(
        "silver_run_id=%s written to all %d gold tables", silver_run_id, len(TABLES)
    )
    logger.info("reconciliation=%s", stats)
    return {"silver_run_id": silver_run_id, "counts": counts, "reconciliation": stats}


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point. Returns the process exit code (non-zero on failure)."""
    parser = argparse.ArgumentParser(
        prog="python -m uk_crime.gold", description=__doc__
    )
    parser.add_argument(
        "--all", action="store_true", required=True, help="rebuild gold"
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
        run_gold(spark, allow_schema_change=args.allow_schema_change)
    except (GoldError, SilverError) as err:
        logger.error("Gold failed: %s", err)
        return 1
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
