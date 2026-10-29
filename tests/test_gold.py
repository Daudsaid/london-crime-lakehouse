"""Gold tests: small in-memory silver DataFrames, plus a few end-to-end runs on Delta."""

from datetime import date
from itertools import count
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from uk_crime import config, gold, silver

_ids = count(1)
DEFAULT_MATURITY_MONTHS = (
    config.OUTCOME_MATURITY_MONTHS
)  # read before any test patches it
DEFAULT_EXCLUDED_MONTHS = config.OUTCOME_EXCLUDED_MONTHS


@pytest.fixture(autouse=True)
def small_outcome_window(monkeypatch: pytest.MonkeyPatch) -> None:
    """Most tests use a 6-month window and no excluded months; tests override as needed."""
    monkeypatch.setattr(config, "OUTCOME_MATURITY_MONTHS", 6)
    monkeypatch.setattr(config, "OUTCOME_EXCLUDED_MONTHS", ())


def crime(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "record_key": f"k{next(_ids)}",
        "crime_id": "c1",
        "month": date(2026, 8, 1),
        "force": "metropolitan",
        "reported_by": "Metropolitan Police Service",
        "falls_within": "Metropolitan Police Service",
        "longitude": -0.1,
        "latitude": 51.5,
        "location": "On or near X",
        "lsoa_code": "E01000001",
        "lsoa_name": "Camden 001A",
        "crime_type": "Burglary",
        "last_outcome_category": "Local resolution",
        "context": None,
        "is_asb": False,
        "has_location": True,
        "dq_attribute_conflict": False,
        "_archive_month": date(2026, 8, 1),
        "_source_file": "archive=2026-08/month=2026-08/x.csv",
        "_run_id": "silver-run-1",
    }
    return {**base, **overrides}


def asb(**overrides: Any) -> dict[str, Any]:
    return crime(
        crime_type=silver.ASB_CRIME_TYPE,
        last_outcome_category=None,
        is_asb=True,
        **overrides,
    )


def silver_df(spark: SparkSession, rows: list[dict[str, Any]]) -> DataFrame:
    names = silver.SILVER_SCHEMA.fieldNames()
    data = [tuple(r[n] for n in names) for r in rows]
    return spark.createDataFrame(data, silver.SILVER_SCHEMA)


def values(df: DataFrame, *columns: str) -> list[tuple[Any, ...]]:
    return sorted((tuple(r[c] for c in columns) for r in df.collect()), key=str)


# ------------------------------------------------------------------ dimensions


def test_every_dimension_has_exactly_one_unknown_member(spark: SparkSession) -> None:
    crimes = silver_df(spark, [crime()])
    expected_unknown = {
        "dim_date": ("month_key", -1),
        "dim_force": ("force_key", gold.UNKNOWN_KEY),
        "dim_crime_type": ("crime_type_key", gold.UNKNOWN_KEY),
        "dim_outcome": ("outcome_key", gold.UNKNOWN_KEY),
        "dim_lsoa": ("lsoa_code", gold.UNKNOWN_LSOA),
    }
    built = gold.build_gold(crimes)
    for dim, (key, unknown) in expected_unknown.items():
        assert built[dim].where(f"{key} = '{unknown}'").count() == 1, dim


def test_dim_date_has_every_month_between_first_and_last(spark: SparkSession) -> None:
    crimes = silver_df(
        spark, [crime(month=date(2026, 1, 1)), crime(month=date(2026, 4, 1))]
    )
    months = values(gold.build_dim_date(crimes).where("month IS NOT NULL"), "month_key")
    assert months == [(202601,), (202602,), (202603,), (202604,)]


@pytest.mark.parametrize(
    ("month", "financial_year"),
    [
        (date(2026, 3, 1), "2025/26"),  # last month of the financial year
        (date(2026, 4, 1), "2026/27"),  # first month of the next one
        (date(2025, 12, 1), "2025/26"),
        (date(2026, 1, 1), "2025/26"),
        (date(2000, 3, 1), "1999/00"),
        (date(2099, 12, 1), "2099/00"),
    ],
)
def test_uk_financial_year_boundaries(
    spark: SparkSession, month: date, financial_year: str
) -> None:
    dim = gold.build_dim_date(silver_df(spark, [crime(month=month)]))
    row = dim.where(f"month = DATE'{month.isoformat()}'").first()
    assert row["uk_financial_year"] == financial_year
    assert row["month_key"] == month.year * 100 + month.month


def test_dim_date_attributes(spark: SparkSession) -> None:
    dim = gold.build_dim_date(silver_df(spark, [crime(month=date(2026, 5, 1))]))
    row = dim.where("month_key = 202605").first()
    assert (row["year"], row["month_num"], row["month_name"], row["quarter"]) == (
        2026, 5, "May", 2,
    )  # fmt: skip


def test_local_authority_is_the_lsoa_name_without_its_trailing_code(
    spark: SparkSession,
) -> None:
    crimes = silver_df(
        spark,
        [
            crime(lsoa_code="E01002652", lsoa_name="Hounslow 021D"),
            crime(lsoa_code="E01000027", lsoa_name="Barking and Dagenham 001A"),
            crime(lsoa_code="E01000001", lsoa_name="City of London 001A"),
            crime(lsoa_code=None, lsoa_name=None),
        ],
    )
    dim = gold.build_dim_lsoa(crimes)
    assert values(dim, "lsoa_code", "local_authority") == sorted(
        [
            ("E01002652", "Hounslow"),
            ("E01000027", "Barking and Dagenham"),
            ("E01000001", "City of London"),
            ("UNKNOWN", "Unknown"),
        ],
        key=str,
    )


def test_unparseable_lsoa_name_raises(spark: SparkSession) -> None:
    crimes = silver_df(spark, [crime(lsoa_name="Hounslow")])
    with pytest.raises(gold.GoldError, match="Hounslow"):
        gold.build_dim_lsoa(crimes)


def test_unmapped_crime_type_raises(spark: SparkSession) -> None:
    with pytest.raises(gold.GoldError, match="Cyber crime"):
        gold.build_dim_crime_type(silver_df(spark, [crime(crime_type="Cyber crime")]))


def test_unmapped_outcome_raises(spark: SparkSession) -> None:
    with pytest.raises(gold.GoldError, match="Brand new outcome"):
        gold.build_dim_outcome(
            silver_df(spark, [crime(last_outcome_category="Brand new outcome")])
        )


def test_unmapped_force_raises(spark: SparkSession) -> None:
    with pytest.raises(gold.GoldError, match="kent"):
        gold.build_dim_force(silver_df(spark, [crime(force="kent")]))


def test_mappings_cover_the_whole_silver_vocabulary() -> None:
    assert set(gold.SEVERITY_TIERS) == set(silver.CRIME_TYPES)
    assert set(gold.SEVERITY_TIERS.values()) == {"high", "medium", "low"}
    named = {c for c in silver.PLACEHOLDER_OUTCOMES if c is not None}
    assert set(gold.OUTCOME_GROUPS) == named | set(silver.PROGRESSED_OUTCOMES)


def test_outcome_dimension_status_and_group(spark: SparkSession) -> None:
    dim = gold.build_dim_outcome(silver_df(spark, [crime()]))
    by_key = {r["outcome_key"]: r for r in dim.collect()}
    assert by_key["awaiting-court-outcome"]["outcome_status"] == "progressed"
    assert by_key["awaiting-court-outcome"]["outcome_group"] == "charged_or_court"
    assert by_key["under-investigation"]["outcome_status"] == "placeholder"
    assert by_key["under-investigation"]["outcome_group"] == "pending"
    assert by_key["not-applicable"]["outcome_group"] == "not_applicable"
    assert by_key["no-outcome-recorded"]["outcome_group"] == "pending"
    assert len(by_key) == len(gold.OUTCOME_GROUPS) + 3  # + two NULL members + UNKNOWN


def test_crime_type_dimension_flags_asb_and_severity(spark: SparkSession) -> None:
    dim = gold.build_dim_crime_type(silver_df(spark, [crime()]))
    by_key = {r["crime_type_key"]: r for r in dim.collect()}
    assert by_key["anti-social-behaviour"]["is_asb"] is True
    assert by_key["anti-social-behaviour"]["severity_tier"] == "low"
    assert by_key["violence-and-sexual-offences"]["severity_tier"] == "high"
    assert len(by_key) == len(silver.CRIME_TYPES) + 1
    assert sum(r["is_asb"] for r in by_key.values()) == 1


# ------------------------------------------------------------------------ fact


def test_fact_has_one_row_per_silver_record_with_resolved_keys(
    spark: SparkSession,
) -> None:
    crimes = silver_df(
        spark,
        [
            crime(record_key="a", lsoa_code=None, lsoa_name=None, has_location=False),
            asb(record_key="b"),
            crime(record_key="c", last_outcome_category=None),  # non-ASB, no outcome
        ],
    )
    fct = gold.build_fct_crimes(crimes)
    by_key = {r["record_key"]: r for r in fct.collect()}
    assert len(by_key) == crimes.count() == 3
    assert by_key["a"]["lsoa_code"] == "UNKNOWN"
    assert by_key["a"]["outcome_key"] == "local-resolution"
    assert (
        by_key["a"]["month_key"] == 202608
        and by_key["a"]["force_key"] == "metropolitan"
    )
    assert by_key["b"]["outcome_key"] == "not-applicable"
    assert by_key["c"]["outcome_key"] == "no-outcome-recorded"
    assert all(None not in (r["month_key"], r["force_key"], r["crime_type_key"], r["outcome_key"], r["lsoa_code"]) for r in by_key.values())  # fmt: skip


def test_referential_integrity_holds_and_detects_orphans(spark: SparkSession) -> None:
    built = gold.build_gold(
        silver_df(spark, [crime(), asb(lsoa_code=None, lsoa_name=None)])
    )
    assert set(gold.check_referential_integrity(built).values()) == {0}

    broken = dict(built)
    broken["fct_crimes"] = built["fct_crimes"].withColumn("outcome_key", F.lit("bogus"))
    assert (
        gold.check_referential_integrity(broken)[
            "fct_crimes.outcome_key -> dim_outcome"
        ]
        == 2
    )


# ----------------------------------------------------------------------- marts


def monthly_series(months: list[date], **overrides: Any) -> list[dict[str, Any]]:
    return [crime(month=m, **overrides) for m in months]


def test_marts_reconcile_to_the_fact(spark: SparkSession) -> None:
    rows = (
        monthly_series([date(2025, 9, 1), date(2026, 8, 1)])
        + [asb(month=date(2026, 8, 1))]
        + [crime(month=date(2026, 8, 1), lsoa_code=None, lsoa_name=None)]
    )
    crimes = silver_df(spark, rows)
    built = gold.build_gold(crimes)
    stats = gold.reconcile_gold(built, crimes.count(), 6, 12)
    assert (
        stats["fct_rows"] == stats["silver_rows"] == stats["mart_monthly_crimes"] == 4
    )


def test_reconcile_raises_when_the_fact_does_not_match_silver(
    spark: SparkSession,
) -> None:
    crimes = silver_df(spark, [crime()])
    built = gold.build_gold(crimes)
    with pytest.raises(gold.GoldReconciliationError, match="fct_crimes"):
        gold.reconcile_gold(built, 2, 6, 12)


def test_outcome_mart_excludes_asb_and_the_newest_six_months(
    spark: SparkSession,
) -> None:
    rows = [
        crime(
            month=date(2026, 8, 1), last_outcome_category="Under investigation"
        ),  # newest
        crime(
            month=date(2026, 3, 1), last_outcome_category="Local resolution"
        ),  # too recent
        crime(
            month=date(2026, 2, 1), last_outcome_category="Local resolution"
        ),  # first included
        crime(month=date(2026, 2, 1), last_outcome_category="Under investigation"),
        crime(
            month=date(2025, 6, 1), last_outcome_category="Unable to prosecute suspect"
        ),
        asb(month=date(2025, 6, 1)),  # ASB: never in the mart
    ]
    built = gold.build_gold(silver_df(spark, rows))
    mart = built["mart_outcome_rates"]

    got = {(r["outcome_group"]): r["crime_count"] for r in mart.collect()}
    assert got == {"out_of_court": 1, "pending": 1, "no_further_action": 1}
    assert {r["first_month"] for r in mart.collect()} == {date(2025, 6, 1)}
    assert {r["last_month"] for r in mart.collect()} == {date(2026, 2, 1)}
    assert abs(sum(r["share_of_crime_type"] for r in mart.collect()) - 1.0) < 1e-9
    assert mart.where("crime_type = 'Anti-social behaviour'").count() == 0


def test_outcome_mart_excludes_listed_months(
    spark: SparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(config, "OUTCOME_EXCLUDED_MONTHS", ("2025-06",))
    rows = [
        crime(month=date(2025, 5, 1), last_outcome_category="Local resolution"),
        crime(
            month=date(2025, 6, 1), last_outcome_category="Under investigation"
        ),  # listed
        crime(month=date(2025, 6, 1), last_outcome_category="Under investigation"),
        crime(
            month=date(2025, 7, 1), last_outcome_category="Unable to prosecute suspect"
        ),
        crime(month=date(2026, 8, 1)),  # latest month: immature
    ]
    built = gold.build_gold(silver_df(spark, rows))
    mart = built["mart_outcome_rates"]

    assert {r["outcome_group"]: r["crime_count"] for r in mart.collect()} == {
        "out_of_court": 1,
        "no_further_action": 1,
    }
    first = mart.first()
    assert (first["first_month"], first["last_month"]) == (
        date(2025, 5, 1),
        date(2025, 7, 1),
    )
    assert (first["months_used"], first["excluded_months"]) == (2, "2025-06")
    # every other table keeps the listed month
    assert built["fct_crimes"].where("month_key = 202506").count() == 2
    assert built["mart_monthly_crimes"].where("month_key = 202506").count() == 1


def test_outcome_mart_records_no_exclusions_as_an_empty_string(
    spark: SparkSession,
) -> None:
    rows = [crime(month=date(2025, 5, 1)), crime(month=date(2026, 8, 1))]
    mart = gold.build_gold(silver_df(spark, rows))["mart_outcome_rates"]
    assert (mart.first()["months_used"], mart.first()["excluded_months"]) == (1, "")


def test_outcome_exclusion_must_look_like_a_month(spark: SparkSession) -> None:
    built = gold.build_gold(silver_df(spark, [crime()]))
    with pytest.raises(gold.GoldError, match="YYYY-MM"):
        gold.build_mart_outcome_rates(
            built["fct_crimes"],
            built["dim_date"],
            built["dim_crime_type"],
            built["dim_outcome"],
            6,
            ["2025-6' OR 1=1"],
        )


def test_reconciliation_accounts_for_the_maturity_window_and_excluded_months(
    spark: SparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(config, "OUTCOME_EXCLUDED_MONTHS", ("2025-06", "2026-07"))
    rows = [
        crime(month=date(2025, 5, 1)),  # mature, used
        crime(month=date(2025, 5, 1)),
        crime(month=date(2025, 6, 1)),  # mature but listed
        crime(month=date(2026, 7, 1)),  # listed AND immature: counted as immature only
        crime(month=date(2026, 8, 1)),  # immature
        asb(month=date(2025, 5, 1)),  # ASB: not in the non-ASB split
    ]
    crimes = silver_df(spark, rows)
    stats = gold.reconcile_gold(
        gold.build_gold(crimes), crimes.count(), 6, 12, ("2025-06", "2026-07")
    )
    assert stats["non_asb_rows"] == 5
    assert (
        stats["mart_outcome_rates"],
        stats["outcome_rows_immature"],
        stats["outcome_rows_excluded_months"],
    ) == (2, 2, 1)
    assert (
        stats["mart_outcome_rates"]
        + stats["outcome_rows_immature"]
        + stats["outcome_rows_excluded_months"]
        == stats["non_asb_rows"]
    )


def test_reconcile_raises_when_the_mart_ignored_an_excluded_month(
    spark: SparkSession,
) -> None:
    rows = [crime(month=date(2025, 5, 1)), crime(month=date(2025, 6, 1)), crime()]
    crimes = silver_df(spark, rows)
    built = gold.build_gold(crimes)  # built with NO exclusions
    with pytest.raises(gold.GoldReconciliationError, match="mart_outcome_rates"):
        gold.reconcile_gold(built, crimes.count(), 6, 12, ("2025-06",))


def test_default_outcome_window_is_16_months_excluding_2024_10() -> None:
    assert DEFAULT_MATURITY_MONTHS == 16
    assert DEFAULT_EXCLUDED_MONTHS == ("2024-10",)


def test_outcome_shares_are_within_each_crime_type(spark: SparkSession) -> None:
    rows = [
        crime(month=date(2025, 1, 1), crime_type="Burglary", last_outcome_category="Local resolution"),
        crime(month=date(2025, 1, 1), crime_type="Burglary", last_outcome_category="Local resolution"),
        crime(month=date(2025, 1, 1), crime_type="Burglary", last_outcome_category="Under investigation"),
        crime(month=date(2025, 1, 1), crime_type="Robbery", last_outcome_category="Local resolution"),
        crime(month=date(2026, 8, 1)),
    ]  # fmt: skip
    mart = gold.build_gold(silver_df(spark, rows))["mart_outcome_rates"]
    shares = {
        (r["crime_type"], r["outcome_group"]): r["share_of_crime_type"]
        for r in mart.collect()
    }
    assert shares[("Burglary", "out_of_court")] == pytest.approx(2 / 3)
    assert shares[("Burglary", "pending")] == pytest.approx(1 / 3)
    assert shares[("Robbery", "out_of_court")] == pytest.approx(1.0)


def test_lsoa_ranking_uses_the_newest_twelve_months_and_ranks_within_authority(
    spark: SparkSession,
) -> None:
    camden_a = {"lsoa_code": "E01A", "lsoa_name": "Camden 001A"}
    camden_b = {"lsoa_code": "E01B", "lsoa_name": "Camden 002B"}
    hounslow = {"lsoa_code": "E01C", "lsoa_name": "Hounslow 021D"}
    rows = (
        [crime(month=date(2026, 8, 1), **camden_a) for _ in range(3)]
        + [
            crime(month=date(2025, 9, 1), **camden_b) for _ in range(5)
        ]  # oldest month in window
        + [
            crime(month=date(2025, 8, 1), **camden_b) for _ in range(9)
        ]  # outside the window
        + [crime(month=date(2026, 8, 1), **hounslow)]
        + [
            crime(month=date(2026, 8, 1), lsoa_code=None, lsoa_name=None)
        ]  # UNKNOWN: not ranked
    )
    mart = gold.build_gold(silver_df(spark, rows))["mart_lsoa_ranking"]
    got = {
        r["lsoa_code"]: (r["crime_count"], r["asb_count"], r["rank_in_local_authority"])
        for r in mart.collect()
    }
    assert got == {"E01A": (3, 0, 2), "E01B": (5, 0, 1), "E01C": (1, 0, 1)}
    assert {(r["first_month"], r["last_month"]) for r in mart.collect()} == {
        (date(2025, 9, 1), date(2026, 8, 1))
    }


def test_lsoa_ranking_counts_asb_separately_and_ranks_on_crime_only(
    spark: SparkSession,
) -> None:
    a = {"lsoa_code": "E01A", "lsoa_name": "Camden 001A"}
    b = {"lsoa_code": "E01B", "lsoa_name": "Camden 002B"}
    rows = (
        [crime(**a) for _ in range(2)]
        + [asb(**a) for _ in range(1)]
        + [crime(**b) for _ in range(1)]
        + [asb(**b) for _ in range(10)]  # lots of ASB must not lift B's rank
        + [asb(lsoa_code="E01Z", lsoa_name="Camden 003C")]  # only ASB: crime_count 0
    )
    mart = gold.build_gold(silver_df(spark, rows))["mart_lsoa_ranking"]
    got = {
        r["lsoa_code"]: (r["crime_count"], r["asb_count"], r["rank_in_local_authority"])
        for r in mart.collect()
    }
    assert got == {"E01A": (2, 1, 1), "E01B": (1, 10, 2), "E01Z": (0, 1, 3)}


def test_lsoa_ranking_covers_london_lsoas_only(spark: SparkSession) -> None:
    rows = [
        crime(lsoa_code="E01L", lsoa_name="Camden 001A"),
        crime(lsoa_code="E01X", lsoa_name="Epping Forest 001A"),
        asb(lsoa_code="E01Y", lsoa_name="Birmingham 010B"),
    ]
    mart = gold.build_gold(silver_df(spark, rows))["mart_lsoa_ranking"]
    assert [r["lsoa_code"] for r in mart.collect()] == ["E01L"]


def test_lsoa_ranking_ties_share_a_rank(spark: SparkSession) -> None:
    a = {"lsoa_code": "E01A", "lsoa_name": "Camden 001A"}
    b = {"lsoa_code": "E01B", "lsoa_name": "Camden 002B"}
    mart = gold.build_gold(silver_df(spark, [crime(**a), crime(**b)]))[
        "mart_lsoa_ranking"
    ]
    assert {r["rank_in_local_authority"] for r in mart.collect()} == {1}


def test_lsoa_window_reconciles_with_outside_london_and_unknown_rows(
    spark: SparkSession,
) -> None:
    rows = [
        crime(lsoa_code="E01A", lsoa_name="Camden 001A"),
        crime(lsoa_code="E01A", lsoa_name="Camden 001A"),
        asb(lsoa_code="E01A", lsoa_name="Camden 001A"),
        crime(lsoa_code="E01X", lsoa_name="Epping Forest 001A"),  # outside London
        crime(lsoa_code="E01X", lsoa_name="Epping Forest 001A"),
        crime(lsoa_code=None, lsoa_name=None),  # unknown LSOA
        crime(month=date(2024, 1, 1)),  # outside the 12-month window
    ]
    crimes = silver_df(spark, rows)
    stats = gold.reconcile_gold(gold.build_gold(crimes), crimes.count(), 6, 12)
    assert (
        stats["ranked_crimes"],
        stats["ranked_asb"],
        stats["outside_london_rows"],
        stats["unknown_lsoa_rows"],
        stats["window_rows"],
    ) == (2, 1, 2, 1, 6)


def test_lsoa_dimension_flags_london_authorities(spark: SparkSession) -> None:
    crimes = silver_df(
        spark,
        [
            crime(lsoa_code="E01A", lsoa_name="Hounslow 021D"),
            crime(lsoa_code="E01X", lsoa_name="Epping Forest 001A"),
        ],
    )
    dim = {
        r["lsoa_code"]: r["is_london"] for r in gold.build_dim_lsoa(crimes).collect()
    }
    assert dim == {"E01A": True, "E01X": False, "UNKNOWN": False}


def test_all_33_london_authorities_appear_in_dim_lsoa(spark: SparkSession) -> None:
    assert len(gold.LONDON_AUTHORITIES) == len(set(gold.LONDON_AUTHORITIES)) == 33
    rows = [
        crime(lsoa_code=f"E01{i:04d}", lsoa_name=f"{authority} 001A")
        for i, authority in enumerate(gold.LONDON_AUTHORITIES)
    ]
    dim = gold.build_dim_lsoa(silver_df(spark, rows))
    london = {r["local_authority"] for r in dim.where("is_london").collect()}
    assert london == set(gold.LONDON_AUTHORITIES)
    assert dim.where("is_london").count() == 33


def test_monthly_mart_counts_by_month_force_and_crime_type(spark: SparkSession) -> None:
    rows = [
        crime(),
        crime(),
        asb(),
        crime(force="city-of-london", month=date(2026, 7, 1)),
    ]
    mart = gold.build_gold(silver_df(spark, rows))["mart_monthly_crimes"]
    got = {
        (r["month"], r["force_key"], r["crime_type"]): r["crime_count"]
        for r in mart.collect()
    }
    assert got == {
        (date(2026, 8, 1), "metropolitan", "Burglary"): 2,
        (date(2026, 8, 1), "metropolitan", "Anti-social behaviour"): 1,
        (date(2026, 7, 1), "city-of-london", "Burglary"): 1,
    }
    assert {r["force_name"] for r in mart.collect()} == {
        "Metropolitan Police Service",
        "City of London Police",
    }


# ------------------------------------------------------------------ borough marts

CAMDEN = {"lsoa_code": "E01A", "lsoa_name": "Camden 001A"}
HOUNSLOW = {"lsoa_code": "E01H", "lsoa_name": "Hounslow 021D"}
EPPING = {"lsoa_code": "E01X", "lsoa_name": "Epping Forest 001A"}  # outside London
NO_LSOA = {"lsoa_code": None, "lsoa_name": None}


def test_borough_monthly_counts_london_boroughs_only_and_flags_asb(
    spark: SparkSession,
) -> None:
    rows = [
        crime(month=date(2026, 8, 1), **CAMDEN),
        crime(month=date(2026, 8, 1), **CAMDEN),
        asb(month=date(2026, 8, 1), **CAMDEN),
        crime(month=date(2026, 7, 1), **CAMDEN),
        crime(month=date(2026, 8, 1), **HOUNSLOW),
        crime(month=date(2026, 8, 1), **EPPING),  # outside London: not in the mart
        crime(month=date(2026, 8, 1), **NO_LSOA),  # unknown LSOA: not in the mart
    ]
    mart = gold.build_gold(silver_df(spark, rows))["mart_borough_monthly"]
    got = {
        (r["local_authority"], r["month"], r["crime_type"], r["is_asb"]): r[
            "crime_count"
        ]
        for r in mart.collect()
    }
    assert got == {
        ("Camden", date(2026, 8, 1), "Burglary", False): 2,
        ("Camden", date(2026, 8, 1), "Anti-social behaviour", True): 1,
        ("Camden", date(2026, 7, 1), "Burglary", False): 1,
        ("Hounslow", date(2026, 8, 1), "Burglary", False): 1,
    }


def test_all_33_boroughs_appear_in_both_borough_marts(spark: SparkSession) -> None:
    rows = [
        crime(
            lsoa_code=f"E01{i:04d}",
            lsoa_name=f"{authority} 001A",
            month=date(2025, 1, 1),  # mature under the 6-month test window
        )
        for i, authority in enumerate(gold.LONDON_AUTHORITIES)
    ]
    rows.append(crime(month=date(2026, 8, 1)))  # sets the latest month
    built = gold.build_gold(silver_df(spark, rows))

    for name in ("mart_borough_monthly", "mart_borough_outcomes"):
        boroughs = {
            r[0] for r in built[name].select("local_authority").distinct().collect()
        }
        assert boroughs == set(gold.LONDON_AUTHORITIES), name


def test_borough_marts_reconcile_to_the_fact(spark: SparkSession) -> None:
    rows = [
        crime(month=date(2025, 1, 1), **CAMDEN),
        crime(month=date(2025, 1, 1), **CAMDEN),
        asb(month=date(2025, 1, 1), **CAMDEN),
        crime(month=date(2025, 1, 1), **EPPING),
        crime(month=date(2025, 1, 1), **EPPING),
        crime(month=date(2025, 1, 1), **NO_LSOA),
        crime(month=date(2026, 8, 1), **HOUNSLOW),  # immature: only in the monthly mart
        asb(month=date(2026, 8, 1), **EPPING),
    ]
    crimes = silver_df(spark, rows)
    stats = gold.reconcile_gold(gold.build_gold(crimes), crimes.count(), 6, 12)

    assert stats["borough_monthly_rows"] == 4  # Camden x3, Hounslow x1
    assert (stats["non_london_rows"], stats["unknown_lsoa_rows_all_months"]) == (3, 1)
    assert stats["borough_monthly_rows"] + 3 + 1 == crimes.count()
    assert stats["borough_outcome_rows"] == 2  # Camden's two mature non-ASB crimes
    assert stats["mature_non_london_rows"] == 3  # Epping x2 and the unknown LSOA


def test_borough_outcomes_use_the_outcome_mart_window_and_exclusions(
    spark: SparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(config, "OUTCOME_EXCLUDED_MONTHS", ("2025-06",))
    rows = [
        crime(
            month=date(2025, 5, 1), last_outcome_category="Local resolution", **CAMDEN
        ),
        crime(
            month=date(2025, 6, 1), last_outcome_category="Local resolution", **CAMDEN
        ),
        crime(
            month=date(2025, 7, 1),
            last_outcome_category="Unable to prosecute suspect",
            **CAMDEN,
        ),
        crime(
            month=date(2025, 7, 1),
            last_outcome_category="Unable to prosecute suspect",
            **HOUNSLOW,
        ),
        crime(
            month=date(2026, 3, 1), last_outcome_category="Local resolution", **CAMDEN
        ),  # immature
        asb(month=date(2025, 7, 1), **CAMDEN),  # ASB: never
        crime(
            month=date(2025, 7, 1), last_outcome_category="Local resolution", **EPPING
        ),
        crime(month=date(2026, 8, 1), **CAMDEN),
    ]
    built = gold.build_gold(silver_df(spark, rows))
    mart = built["mart_borough_outcomes"]

    got = {
        (r["local_authority"], r["outcome_group"]): r["crime_count"]
        for r in mart.collect()
    }
    assert got == {
        ("Camden", "out_of_court"): 1,
        ("Camden", "no_further_action"): 1,
        ("Hounslow", "no_further_action"): 1,
    }
    first = mart.first()
    assert (first["first_month"], first["last_month"]) == (
        date(2025, 5, 1),
        date(2025, 7, 1),
    )
    assert (first["months_used"], first["excluded_months"]) == (2, "2025-06")
    assert mart.where("crime_type = 'Anti-social behaviour'").count() == 0
    # the same rows as mart_outcome_rates, less the one outside-London crime
    total = built["mart_outcome_rates"].agg(F.sum("crime_count")).first()[0]
    assert total == mart.agg(F.sum("crime_count")).first()[0] + 1


def test_borough_outcome_shares_sum_to_one_within_each_borough_and_crime_type(
    spark: SparkSession,
) -> None:
    rows = [
        crime(
            month=date(2025, 1, 1), last_outcome_category="Local resolution", **CAMDEN
        ),
        crime(
            month=date(2025, 1, 1), last_outcome_category="Local resolution", **CAMDEN
        ),
        crime(
            month=date(2025, 1, 1),
            last_outcome_category="Under investigation",
            **CAMDEN,
        ),
        crime(
            month=date(2025, 1, 1), last_outcome_category="Local resolution", **HOUNSLOW
        ),
        crime(month=date(2026, 8, 1), **CAMDEN),
    ]
    mart = gold.build_gold(silver_df(spark, rows))["mart_borough_outcomes"]
    shares = {
        (r["local_authority"], r["outcome_group"]): r["share_of_crime_type"]
        for r in mart.collect()
    }
    assert shares[("Camden", "out_of_court")] == pytest.approx(2 / 3)
    assert shares[("Camden", "pending")] == pytest.approx(1 / 3)
    assert shares[("Hounslow", "out_of_court")] == pytest.approx(1.0)


def test_reconcile_raises_when_a_borough_mart_loses_rows(spark: SparkSession) -> None:
    rows = [
        crime(month=date(2025, 1, 1), **CAMDEN),
        crime(month=date(2026, 8, 1), **CAMDEN),
    ]
    crimes = silver_df(spark, rows)
    built = gold.build_gold(crimes)

    broken = {**built, "mart_borough_monthly": built["mart_borough_monthly"].limit(1)}
    with pytest.raises(gold.GoldReconciliationError, match="mart_borough_monthly"):
        gold.reconcile_gold(broken, crimes.count(), 6, 12)

    broken = {**built, "mart_borough_outcomes": built["mart_borough_outcomes"].limit(0)}
    with pytest.raises(gold.GoldReconciliationError, match="mart_borough_outcomes"):
        gold.reconcile_gold(broken, crimes.count(), 6, 12)


# ---------------------------------------------------------- dq_volume_anomalies

MONTHLY_MART_COLUMNS = (
    "force_key string, crime_type string, month date, crime_count long"
)
NO_ACK: tuple[config.AcknowledgedAnomaly, ...] = ()
MET_BURGLARY = ("metropolitan", "Burglary")
MET_ACK = config.AcknowledgedAnomaly(
    force="metropolitan", crime_type="Burglary", from_month="2026-04", note="known"
)


def month_n(n: int) -> date:
    """The n-th month (0-based) from 2026-01."""
    return date(2026 + n // 12, n % 12 + 1, 1)


def monthly_mart(
    spark: SparkSession, series: dict[tuple[str, str], list[int | None]]
) -> DataFrame:
    """A mart_monthly_crimes stand-in: one row per (force, crime type, month).

    A None count leaves the row out (the crime type had no rows that month).
    """
    rows = [
        (force, crime_type, month_n(i), count_)
        for (force, crime_type), counts in series.items()
        for i, count_ in enumerate(counts)
        if count_ is not None
    ]
    return spark.createDataFrame(rows, MONTHLY_MART_COLUMNS)


def anomalies(
    df: DataFrame,
    ack: tuple[config.AcknowledgedAnomaly, ...] = NO_ACK,
    threshold: float = 50.0,
    min_baseline: int = 500,
) -> DataFrame:
    return gold.build_dq_volume_anomalies(df, threshold, min_baseline, ack)


def flags(df: DataFrame) -> list[tuple[Any, ...]]:
    return sorted(
        (r["force"], r["crime_type"], r["month"], r["crime_count"])
        for r in df.collect()
    )


def test_volume_check_flags_a_drop_against_the_previous_three_months(
    spark: SparkSession,
) -> None:
    mart = monthly_mart(spark, {MET_BURGLARY: [1000, 1000, 1000, 400, 1000]})
    result = anomalies(mart)

    assert flags(result) == [("metropolitan", "Burglary", month_n(3), 400)]
    row = result.first()
    assert (row["baseline_median"], row["pct_change"]) == (1000.0, -60.0)
    assert (row["is_acknowledged"], row["note"]) == (False, None)


def test_volume_check_flags_a_spike_too(spark: SparkSession) -> None:
    mart = monthly_mart(spark, {MET_BURGLARY: [1000, 1000, 1000, 1800]})
    assert [r["pct_change"] for r in anomalies(mart).collect()] == [80.0]


def test_volume_check_ignores_small_baselines(spark: SparkSession) -> None:
    mart = monthly_mart(spark, {("city-of-london", "Burglary"): [100, 100, 100, 5]})
    assert anomalies(mart).count() == 0


def test_volume_check_ignores_normal_seasonal_swings(spark: SparkSession) -> None:
    mart = monthly_mart(
        spark, {MET_BURGLARY: [1000, 1200, 1000, 1300, 800, 1250, 1500, 1100]}
    )
    assert anomalies(mart).count() == 0


@pytest.mark.parametrize(
    ("count_", "flagged"), [(500, False), (499, True), (1500, False), (1501, True)]
)
def test_volume_check_threshold_is_strictly_more_than_50_percent(
    spark: SparkSession, count_: int, flagged: bool
) -> None:
    mart = monthly_mart(spark, {MET_BURGLARY: [1000, 1000, 1000, count_]})
    assert (anomalies(mart).count() == 1) is flagged


@pytest.mark.parametrize(("baseline", "flagged"), [(500, True), (499, False)])
def test_volume_check_baseline_minimum_is_inclusive(
    spark: SparkSession, baseline: int, flagged: bool
) -> None:
    mart = monthly_mart(spark, {MET_BURGLARY: [baseline] * 3 + [10]})
    assert (anomalies(mart).count() == 1) is flagged


def test_volume_check_baseline_is_a_median_not_a_mean(spark: SparkSession) -> None:
    """One spiky month in the baseline must not move it (a mean would be 2333)."""
    mart = monthly_mart(spark, {MET_BURGLARY: [1000, 5000, 1000, 1000]})
    assert anomalies(mart).count() == 0


def test_volume_check_thresholds_come_from_the_arguments(spark: SparkSession) -> None:
    mart = monthly_mart(spark, {MET_BURGLARY: [1000, 1000, 1000, 800]})
    assert anomalies(mart).count() == 0
    assert anomalies(mart, threshold=10.0).count() == 1
    assert anomalies(mart, threshold=10.0, min_baseline=2000).count() == 0


def test_volume_check_treats_a_missing_crime_type_month_as_zero(
    spark: SparkSession,
) -> None:
    """The force has data that month (another type), so the missing type fell to 0."""
    mart = monthly_mart(
        spark,
        {
            MET_BURGLARY: [1000, 1000, 1000, None],
            ("metropolitan", "Robbery"): [900, 900, 900, 900],
        },
    )
    result = anomalies(mart)
    assert flags(result) == [("metropolitan", "Burglary", month_n(3), 0)]
    assert result.first()["pct_change"] == -100.0


def test_volume_check_needs_three_consecutive_months_of_history(
    spark: SparkSession,
) -> None:
    """A force with no data in 2026-03 has no baseline for the months that follow."""
    mart = monthly_mart(spark, {MET_BURGLARY: [1000, 1000, None, 1000, 100]})
    assert anomalies(mart).count() == 0


def test_volume_check_compares_each_force_only_with_itself(spark: SparkSession) -> None:
    mart = monthly_mart(
        spark,
        {
            MET_BURGLARY: [1000, 1000, 1000, 1000],
            ("city-of-london", "Burglary"): [800, 800, 800, 300],
        },
    )
    assert flags(anomalies(mart)) == [("city-of-london", "Burglary", month_n(3), 300)]


def test_acknowledged_entries_mark_flags_from_their_month_but_do_not_hide_them(
    spark: SparkSession,
) -> None:
    mart = monthly_mart(
        spark,
        {
            MET_BURGLARY: [1000, 1000, 1000, 400, 1000, 1000, 1000, 300],
            ("metropolitan", "Robbery"): [
                1000,
                1000,
                1000,
                400,
                1000,
                1000,
                1000,
                1000,
            ],
            ("city-of-london", "Burglary"): [1000, 1000, 1000, 400],
        },
    )
    result = {
        (r["force"], r["crime_type"], r["month"]): (r["is_acknowledged"], r["note"])
        for r in anomalies(mart, (MET_ACK,)).collect()
    }
    assert result == {
        # 2026-04 is the entry's from_month: acknowledged, with its note.
        ("metropolitan", "Burglary", month_n(3)): (True, "known"),
        # 2026-08 is later: still covered (the entry is open-ended).
        ("metropolitan", "Burglary", month_n(7)): (True, "known"),
        # Another crime type or another force is not covered.
        ("metropolitan", "Robbery", month_n(3)): (False, None),
        ("city-of-london", "Burglary", month_n(3)): (False, None),
    }


def test_a_flag_before_the_acknowledged_month_is_not_acknowledged(
    spark: SparkSession,
) -> None:
    mart = monthly_mart(spark, {MET_BURGLARY: [1000, 1000, 1000, 400]})
    late = config.AcknowledgedAnomaly(*MET_ACK[:2], "2026-05", "known")
    assert [r["is_acknowledged"] for r in anomalies(mart, (late,)).collect()] == [False]


def test_acknowledged_list_must_be_unique_and_use_yyyy_mm(spark: SparkSession) -> None:
    mart = monthly_mart(spark, {MET_BURGLARY: [1000, 1000, 1000, 400]})
    with pytest.raises(gold.GoldError, match="duplicate"):
        anomalies(mart, (MET_ACK, MET_ACK))
    bad = config.AcknowledgedAnomaly(*MET_ACK[:2], "2026-4", "x")
    with pytest.raises(gold.GoldError, match="YYYY-MM"):
        anomalies(mart, (bad,))


def test_every_flag_is_logged_at_warning_and_counted(
    spark: SparkSession, caplog: pytest.LogCaptureFixture
) -> None:
    mart = monthly_mart(
        spark,
        {
            MET_BURGLARY: [1000, 1000, 1000, 400],
            ("metropolitan", "Robbery"): [1000, 1000, 1000, 400],
        },
    )
    with caplog.at_level("WARNING", logger="uk_crime.gold"):
        counts = gold.log_volume_anomalies(anomalies(mart, (MET_ACK,)))

    assert counts == {"flagged": 2, "acknowledged": 1, "unacknowledged": 1}
    messages = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert len(messages) == 2
    assert any("Burglary" in m and "(acknowledged)" in m for m in messages)
    assert any("Robbery" in m and "NOT acknowledged" in m for m in messages)


def test_reconciliation_is_unaffected_by_the_check(spark: SparkSession) -> None:
    """The table is not an input to reconcile_gold: the numbers match without it."""
    rows = [crime(month=month_n(i)) for i in range(3) for _ in range(3)] + [
        crime(month=month_n(3))
    ]
    crimes = silver_df(spark, rows)
    built = gold.build_gold(crimes)
    assert built["dq_volume_anomalies"].count() == 0  # baselines are far below 500

    with_check = gold.reconcile_gold(built, crimes.count(), 6, 12)
    without = gold.reconcile_gold(
        {n: df for n, df in built.items() if n != "dq_volume_anomalies"},
        crimes.count(),
        6,
        12,
    )
    assert with_check == without


def test_default_thresholds_and_acknowledged_entry() -> None:
    assert config.VOLUME_ANOMALY_THRESHOLD_PCT == 50.0
    assert config.VOLUME_ANOMALY_MIN_BASELINE == 500
    (entry,) = config.ACKNOWLEDGED_VOLUME_ANOMALIES
    assert (entry.force, entry.crime_type, entry.from_month) == (
        "metropolitan",
        "Criminal damage and arson",
        "2026-05",
    )
    assert "unverified" in entry.note


def test_the_table_has_the_expected_columns_and_is_part_of_gold(
    spark: SparkSession,
) -> None:
    assert "dq_volume_anomalies" in gold.TABLES
    mart = monthly_mart(spark, {MET_BURGLARY: [1000, 1000, 1000, 400]})
    expected = [f.name for f in gold.SCHEMAS["dq_volume_anomalies"]][:-1]
    assert anomalies(mart).columns == expected


# ------------------------------------------------------------ primary-key uniqueness


def pk_fixture(spark: SparkSession) -> dict[str, DataFrame]:
    """Many records sharing the same month, force, crime type, outcome and LSOA."""
    rows = [
        crime(),
        crime(),
        crime(month=date(2026, 7, 1)),
        asb(),
        asb(),
        crime(force="city-of-london", lsoa_code=None, lsoa_name=None),
        crime(lsoa_code="E01X", lsoa_name="Epping Forest 001A"),
    ]
    return gold.build_gold(silver_df(spark, rows))


@pytest.mark.parametrize(
    ("table", "key"),
    [*gold.DIMENSION_KEYS.items(), ("fct_crimes", "record_key")],
)
def test_primary_key_is_unique_and_never_null(
    spark: SparkSession, table: str, key: str
) -> None:
    df = pk_fixture(spark)[table]
    assert df.where(F.col(key).isNull()).count() == 0
    assert df.count() == df.select(key).distinct().count() > 0


def test_reconcile_raises_on_a_duplicated_dimension_key(spark: SparkSession) -> None:
    crimes = silver_df(spark, [crime()])
    built = gold.build_gold(crimes)
    built["dim_force"] = built["dim_force"].unionByName(built["dim_force"].limit(1))
    with pytest.raises(gold.GoldReconciliationError, match="dim_force.force_key"):
        gold.reconcile_gold(built, 1, 6, 12)


def test_reconcile_raises_on_a_duplicated_fact_record_key(spark: SparkSession) -> None:
    crimes = silver_df(spark, [crime(record_key="same"), crime(record_key="same")])
    built = gold.build_gold(crimes)
    with pytest.raises(gold.GoldReconciliationError, match="record_key"):
        gold.reconcile_gold(built, crimes.count(), 6, 12)


# ------------------------------------------------------------- end to end on Delta


@pytest.fixture
def lakehouse(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        config, "SILVER_STREET_TABLE", tmp_path / "silver" / "crimes_street"
    )
    monkeypatch.setattr(
        config,
        "SILVER_QUARANTINE_TABLE",
        tmp_path / "silver" / "crimes_street_quarantine",
    )
    monkeypatch.setattr(config, "GOLD_DIR", tmp_path / "gold")


def write_silver(
    spark: SparkSession,
    rows: list[dict[str, Any]],
    run_id: str = "silver-run-1",
    quarantine_run_id: str | None = None,
) -> None:
    rows = [{**r, "_run_id": run_id} for r in rows]
    silver.overwrite_table(silver_df(spark, rows), config.SILVER_STREET_TABLE, run_id)
    empty = spark.createDataFrame([], silver.QUARANTINE_SCHEMA)
    silver.overwrite_table(
        empty, config.SILVER_QUARANTINE_TABLE, quarantine_run_id or run_id
    )


def sample_rows() -> list[dict[str, Any]]:
    return [
        crime(),
        crime(month=date(2026, 1, 1), lsoa_code="E01B", lsoa_name="Hounslow 021D"),
        asb(),
        crime(lsoa_code=None, lsoa_name=None, has_location=False),
    ]


def gold_table(spark: SparkSession, name: str) -> DataFrame:
    return spark.read.format("delta").load(str(gold.table_path(name)))


@pytest.mark.slow
def test_gold_refuses_to_run_when_silver_is_inconsistent(
    spark: SparkSession, lakehouse: None
) -> None:
    write_silver(spark, sample_rows(), run_id="run-A", quarantine_run_id="run-B")
    with pytest.raises(silver.ConsistencyError):
        gold.run_gold(spark)
    assert not config.GOLD_DIR.exists()


@pytest.fixture
def one_volume_flag(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """Rows that make dq_volume_anomalies hold exactly one flag.

    Metropolitan Burglary has 2 rows a month for 2026-02..04 and none in 2026-05, a month
    in which the force does have data (a Robbery row). The baseline minimum is lowered to 1
    so that such a small count can be flagged.
    """
    monkeypatch.setattr(config, "VOLUME_ANOMALY_MIN_BASELINE", 1)
    return [crime(month=date(2026, m, 1)) for m in (2, 3, 4) for _ in range(2)] + [
        crime(month=date(2026, 5, 1), crime_type="Robbery")
    ]


@pytest.mark.slow
def test_run_gold_writes_every_table_with_the_same_silver_run_id(
    spark: SparkSession, lakehouse: None, one_volume_flag: list[dict[str, Any]]
) -> None:
    rows = sample_rows() + one_volume_flag
    write_silver(spark, rows, run_id="run-A")
    stats = gold.run_gold(spark)

    assert stats["silver_run_id"] == "run-A"
    assert gold.assert_gold_consistent(spark) == "run-A"
    assert set(stats["counts"]) == set(gold.TABLES)
    assert stats["counts"]["fct_crimes"] == len(rows)
    assert stats["counts"]["dq_volume_anomalies"] == 1
    assert stats["volume_anomalies"] == {
        "flagged": 1,
        "acknowledged": 0,
        "unacknowledged": 1,
    }
    for name in gold.TABLES:
        ids = gold_table(spark, name).select("_silver_run_id").distinct().collect()
        assert [r[0] for r in ids] == ["run-A"], name


@pytest.mark.slow
def test_gold_consistency_fails_when_one_table_has_another_run_id(
    spark: SparkSession, lakehouse: None
) -> None:
    write_silver(spark, sample_rows(), run_id="run-A")
    gold.run_gold(spark)

    other = gold_table(spark, "dim_force").withColumn("_silver_run_id", F.lit("run-B"))
    silver.overwrite_table(other, gold.table_path("dim_force"), "run-B")

    with pytest.raises(gold.GoldConsistencyError, match="run-B"):
        gold.assert_gold_consistent(spark)


@pytest.mark.slow
def test_gold_consistency_fails_when_a_table_is_missing(
    spark: SparkSession, lakehouse: None
) -> None:
    with pytest.raises(gold.GoldConsistencyError, match="missing"):
        gold.assert_gold_consistent(spark)


@pytest.mark.slow
def test_an_empty_anomaly_table_still_carries_the_silver_run_id(
    spark: SparkSession, lakehouse: None
) -> None:
    """No flags is the normal case: the table is empty, and its run id is in the commit."""
    write_silver(spark, sample_rows(), run_id="run-A")
    stats = gold.run_gold(spark)

    assert stats["counts"]["dq_volume_anomalies"] == 0
    assert stats["volume_anomalies"] == {
        "flagged": 0,
        "acknowledged": 0,
        "unacknowledged": 0,
    }
    assert gold.assert_gold_consistent(spark) == "run-A"


@pytest.mark.slow
def test_rerun_gives_identical_output(
    spark: SparkSession, lakehouse: None, one_volume_flag: list[dict[str, Any]]
) -> None:
    write_silver(spark, sample_rows() + one_volume_flag)
    gold.run_gold(spark)
    first = {n: gold.table_fingerprint(gold_table(spark, n)) for n in gold.TABLES}
    gold.run_gold(spark)
    second = {n: gold.table_fingerprint(gold_table(spark, n)) for n in gold.TABLES}
    assert first == second
    assert all(f["rows"] > 0 for f in first.values())


@pytest.mark.slow
def test_schema_mismatch_without_the_flag_raises_and_writes_nothing(
    spark: SparkSession, lakehouse: None
) -> None:
    write_silver(spark, sample_rows())
    gold.run_gold(spark)
    old = gold_table(spark, "fct_crimes").drop("latitude")
    silver.overwrite_table(
        old, gold.table_path("fct_crimes"), "run-old", overwrite_schema=True
    )

    with pytest.raises(gold.GoldSchemaChangeError) as exc:
        gold.run_gold(spark)
    assert "--allow-schema-change" in str(
        exc.value
    ) and "added latitude (double)" in str(exc.value)
    assert "latitude" not in gold_table(spark, "fct_crimes").columns  # untouched

    gold.run_gold(spark, allow_schema_change=True)
    assert "latitude" in gold_table(spark, "fct_crimes").columns
    assert gold.assert_gold_consistent(spark) == "silver-run-1"


@pytest.mark.slow
def test_gold_reads_only_silver(
    spark: SparkSession, lakehouse: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    write_silver(spark, sample_rows())
    monkeypatch.setattr(
        config, "BRONZE_STREET_TABLE", config.GOLD_DIR / "does-not-exist"
    )
    gold.run_gold(spark)  # would fail if gold touched bronze


def test_main_requires_all() -> None:
    with pytest.raises(SystemExit) as exc:
        gold.main([])
    assert exc.value.code == 2
