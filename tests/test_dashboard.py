"""Dashboard tests: the data-loading functions in dashboard/app.py and the page itself.

These need the optional `dashboard` dependency group (pandas, plotly, streamlit); they are
skipped where it is not installed. The page tests run the real app headlessly against the
committed snapshot in dashboard/data/.
"""

import importlib.util
import json
from pathlib import Path

import pytest

pd = pytest.importorskip("pandas")
pytest.importorskip("plotly")
pytest.importorskip("streamlit")

import streamlit as st
from streamlit.testing.v1 import AppTest

from tests.test_export import prepare_gold, sample_rows
from uk_crime import export, gold

APP_PATH = Path(__file__).resolve().parents[1] / "dashboard" / "app.py"
COMMITTED_DATA = APP_PATH.parent / "data"

spec = importlib.util.spec_from_file_location("dashboard_app", APP_PATH)
app = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
spec.loader.exec_module(app)  # type: ignore[union-attr]

JULY, AUGUST = pd.Timestamp("2026-07-01"), pd.Timestamp("2026-08-01")
BOUNDS = (JULY, AUGUST)


def write_fake_snapshot(folder: Path) -> None:
    """Small Parquet files with the exported columns, written with pandas.

    Two boroughs: Camden (70 crimes + 30 ASB) and Westminster (100 crimes + 20 ASB) in the
    12-month window, so London is 170 crimes + 50 ASB.
    """
    folder.mkdir(parents=True)
    months = [JULY.date(), AUGUST.date()]
    pd.DataFrame(
        {
            "month": [months[0], months[0], months[1]],
            "force_key": ["metropolitan"] * 3,
            "force_name": ["Metropolitan Police Service"] * 3,
            "crime_type": ["Burglary", app.ASB, "Burglary"],
            "crime_count": [60, 30, 10],
        }
    ).to_parquet(folder / "mart_monthly_crimes.parquet")
    pd.DataFrame(
        {
            "crime_type": ["Burglary", "Burglary"],
            "outcome_group": ["charged_or_court", "no_further_action"],
            "crime_count": [40, 160],
            "share_of_crime_type": [0.2, 0.8],
            "first_month": [months[0]] * 2,
            "last_month": [months[1]] * 2,
            "months_used": [2, 2],
            "excluded_months": ["2024-10"] * 2,
        }
    ).to_parquet(folder / "mart_outcome_rates.parquet")
    pd.DataFrame(
        {
            "lsoa_code": ["E01A", "E01B", "E01C"],
            "lsoa_name": ["Camden 001A", "Camden 002B", "Westminster 003C"],
            "local_authority": ["Camden", "Camden", "Westminster"],
            "crime_count": [50, 20, 100],
            "asb_count": [20, 10, 20],
            "rank_in_local_authority": [1, 2, 1],
            "first_month": [months[0]] * 3,
            "last_month": [months[1]] * 3,
            "latitude": [51.54, None, 51.50],
            "longitude": [-0.14, None, -0.13],
        }
    ).to_parquet(folder / "mart_lsoa_ranking.parquet")
    pd.DataFrame(
        {
            "local_authority": ["Camden"] * 3 + ["Westminster"] * 2,
            "month": [months[0], months[0], months[1], months[1], months[1]],
            "crime_type": ["Burglary", app.ASB, "Burglary", "Burglary", app.ASB],
            "is_asb": [False, True, False, False, True],
            "crime_count": [60, 30, 10, 100, 20],
        }
    ).to_parquet(folder / "mart_borough_monthly.parquet")
    pd.DataFrame(
        {
            "local_authority": ["Camden", "Camden", "Westminster", "Westminster"],
            "crime_type": ["Burglary"] * 4,
            "outcome_group": ["charged_or_court", "no_further_action"] * 2,
            "crime_count": [10, 90, 30, 70],
            "share_of_crime_type": [0.1, 0.9, 0.3, 0.7],
            "first_month": [months[0]] * 4,
            "last_month": [months[1]] * 4,
            "months_used": [2] * 4,
            "excluded_months": ["2024-10"] * 4,
        }
    ).to_parquet(folder / "mart_borough_outcomes.parquet")
    pd.DataFrame(
        {
            "force": ["metropolitan", "metropolitan"],
            "crime_type": ["Criminal damage and arson", "Robbery"],
            "month": pd.to_datetime(["2026-05-01", "2026-08-01"]),
            "crime_count": [221, 2000],
            "baseline_median": [4322.0, 1000.0],
            "pct_change": [-94.9, 100.0],
            "is_acknowledged": [True, False],
            "note": ["Source change (unverified).", None],
        }
    ).to_parquet(folder / "dq_volume_anomalies.parquet")
    pd.DataFrame(
        {
            "group": ["reconciliation", "reconciliation"],
            "item": ["bronze_rows", "quarantined"],
            "rows": [101, 1],
            "share": [1.0, 1 / 101],
        }
    ).to_parquet(folder / "reconciliation.parquet")
    (folder / "snapshot.json").write_text(
        json.dumps(
            {
                "archive_month": "2026-08",
                "exported_at": "2026-10-28T18:13:28+00:00",
                "silver_run_id": "run-1",
                "lsoa_window": {"first": "2026-07", "last": "2026-08"},
            }
        )
    )


@pytest.fixture
def fake(tmp_path: Path) -> dict:
    write_fake_snapshot(tmp_path / "data")
    return app.load_tables(data_dir=tmp_path / "data")


def committed_tables() -> dict:
    if not COMMITTED_DATA.is_dir():
        pytest.skip("no committed snapshot")
    return app.load_tables(data_dir=COMMITTED_DATA)


# ------------------------------------------------------------- loading functions


def test_loaders_read_the_exported_files(tmp_path: Path) -> None:
    write_fake_snapshot(tmp_path / "data")
    tables = app.load_tables(data_dir=tmp_path / "data")

    assert set(tables) == set(app.TABLE_NAMES) == set(export.EXPORT_COLUMNS)
    assert tables["mart_borough_monthly"]["month"].dtype.kind == "M"  # real datetimes
    for name, columns in export.EXPORT_COLUMNS.items():
        assert list(tables[name].columns) == columns
    assert app.load_snapshot(tmp_path / "data")["archive_month"] == "2026-08"


# ------------------------------------------------------------------ cache keys


@pytest.fixture
def cached_loader():
    """A fresh st.cache_data wrapper over load_tables, with the cache cleared around it."""
    st.cache_data.clear()
    yield st.cache_data(app.load_tables)
    st.cache_data.clear()


def test_snapshot_version_changes_with_export_time_and_silver_run() -> None:
    base = {"exported_at": "2026-10-28T18:13:28+00:00", "silver_run_id": "run-1"}

    assert app.snapshot_version(base) == app.snapshot_version(dict(base))
    assert app.snapshot_version(base) != app.snapshot_version(
        {**base, "silver_run_id": "run-2"}
    )
    assert app.snapshot_version(base) != app.snapshot_version(
        {**base, "exported_at": "2026-10-29T09:00:00+00:00"}
    )


def test_cached_loader_is_keyed_on_the_table_list(
    tmp_path: Path, cached_loader
) -> None:
    write_fake_snapshot(tmp_path / "data")
    data_dir = tmp_path / "data"
    one = app.TABLE_NAMES[:1]

    assert set(cached_loader(one, "v1", data_dir)) == set(one)
    # The regression: a longer list must not be served the earlier, shorter cached dict.
    assert set(cached_loader(app.TABLE_NAMES, "v1", data_dir)) == set(app.TABLE_NAMES)


def test_cached_loader_is_keyed_on_the_snapshot_version(
    tmp_path: Path, cached_loader
) -> None:
    write_fake_snapshot(tmp_path / "data")
    data_dir = tmp_path / "data"
    name = "mart_monthly_crimes"
    before = cached_loader((name,), "v1", data_dir)[name]
    pd.DataFrame({"month": ["2026-08"], "crime_count": [999]}).to_parquet(
        data_dir / f"{name}.parquet"
    )

    same_version = cached_loader((name,), "v1", data_dir)[name]
    new_version = cached_loader((name,), "v2", data_dir)[name]

    assert same_version.equals(before)  # same key: the cached copy
    assert new_version["crime_count"].tolist() == [
        999
    ]  # new snapshot version: fresh read


# ------------------------------------------------------------ borough selection


def test_selector_lists_all_london_then_the_boroughs_sorted(fake: dict) -> None:
    assert app.borough_options(fake["mart_borough_monthly"]) == [
        "All London",
        "Camden",
        "Westminster",
    ]


def test_committed_snapshot_offers_all_london_plus_the_33_boroughs() -> None:
    options = app.borough_options(committed_tables()["mart_borough_monthly"])
    assert options[0] == app.ALL_LONDON and len(options) == 34
    assert options[1:] == sorted(gold.LONDON_AUTHORITIES)


@pytest.mark.parametrize(
    ("requested", "expected"),
    [
        (None, "All London"),
        ("", "All London"),
        ("Camden", "Camden"),
        ("camden", "Camden"),  # case-insensitive match
        (" Westminster ", "Westminster"),
        ("Atlantis", "All London"),  # unknown names fall back
        ("All London", "All London"),
    ],
)
def test_url_borough_resolves_or_falls_back(
    requested: str | None, expected: str
) -> None:
    assert (
        app.resolve_borough(requested, ["All London", "Camden", "Westminster"])
        == expected
    )


# --------------------------------------------------------- figures per selection


def test_headline_figures_for_a_borough_and_for_london(fake: dict) -> None:
    monthly, outcomes = fake["mart_borough_monthly"], fake["mart_borough_outcomes"]

    camden = app.borough_headline(monthly, outcomes, BOUNDS, "Camden")
    assert (camden["crimes"], camden["asb"]) == (70, 30)
    assert camden["asb_share"] == pytest.approx(0.30)
    assert camden["charge_rate"] == pytest.approx(0.10)

    london = app.borough_headline(monthly, outcomes, BOUNDS, app.ALL_LONDON)
    assert (london["crimes"], london["asb"]) == (170, 50)
    assert london["charge_rate"] == pytest.approx(40 / 200)


def test_the_window_limits_the_headline_to_the_last_12_months(fake: dict) -> None:
    monthly, outcomes = fake["mart_borough_monthly"], fake["mart_borough_outcomes"]
    august_only = app.borough_headline(monthly, outcomes, (AUGUST, AUGUST), "Camden")
    assert august_only["crimes"] == 10 and august_only["asb"] == 0


def test_london_comparison_line(fake: dict) -> None:
    monthly = fake["mart_borough_monthly"]
    assert "41.2%" in app.london_comparison(monthly, BOUNDS, "Camden")
    assert "70 of 170" in app.london_comparison(monthly, BOUNDS, "Camden")


def test_the_london_comparison_is_hidden_for_all_london(fake: dict) -> None:
    assert (
        app.london_comparison(fake["mart_borough_monthly"], BOUNDS, app.ALL_LONDON)
        is None
    )


def trend(monthly: pd.DataFrame, borough: str, include_asb: bool = True, types=None):
    """trend_figure with every crime type of the selection chosen, unless told otherwise."""
    colours = app.crime_type_colours(monthly)
    if types is None:
        types = app.top_types(monthly, BOUNDS, borough, include_asb)
    return app.trend_figure(monthly, borough, include_asb, types, colours)


def line(figure, name: str):
    (trace,) = [t for t in figure.data if t.name == name]
    return trace


def test_trend_figure_filters_by_borough_and_asb(fake: dict) -> None:
    monthly = fake["mart_borough_monthly"]
    total = lambda figure: int(sum(line(figure, app.TOTAL_LABEL).y))

    assert total(trend(monthly, "Camden", True)) == 100
    assert total(trend(monthly, "Camden", False)) == 70
    assert total(trend(monthly, "Westminster", True)) == 120
    assert total(trend(monthly, app.ALL_LONDON, True)) == 220
    names = {t.name for t in trend(monthly, "Camden", False).data}
    assert names == {app.TOTAL_LABEL, "Burglary"}


def test_the_total_line_is_always_there_and_covers_unselected_types(fake: dict) -> None:
    monthly = fake["mart_borough_monthly"]
    only_burglary = trend(monthly, "Camden", True, types=["Burglary"])
    assert [t.name for t in only_burglary.data] == [app.TOTAL_LABEL, "Burglary"]
    assert list(line(only_burglary, app.TOTAL_LABEL).y) == [90, 10]  # includes ASB
    no_types = trend(monthly, "Camden", True, types=[])
    assert [t.name for t in no_types.data] == [app.TOTAL_LABEL]


def test_the_default_selection_is_the_four_biggest_types_of_the_selection() -> None:
    types = ["A", "B", "C", "D", "E", "F"]
    sizes = [50, 10, 40, 30, 20, 5]  # last 12 months; A is the biggest
    monthly = pd.DataFrame(
        {
            "local_authority": "Camden",
            "month": [JULY] * 6,
            "crime_type": types,
            "is_asb": [False] * 5 + [True],
            "crime_count": sizes,
        }
    )
    assert app.TOP_TYPES == 4
    ranked = app.top_types(monthly, BOUNDS, "Camden", include_asb=True)
    assert ranked == ["A", "C", "D", "E", "B", "F"]
    assert ranked[: app.TOP_TYPES] == ["A", "C", "D", "E"]
    # Unticking ASB removes it from the choices.
    assert "F" not in app.top_types(monthly, BOUNDS, "Camden", include_asb=False)
    # Another borough has its own ranking; types outside the window do not count.
    hackney = pd.DataFrame(
        {
            "local_authority": ["Hackney", "Hackney"],
            "month": [JULY, pd.Timestamp("2025-01-01")],
            "crime_type": ["Drugs", "A"],  # A is only recorded before the window
            "is_asb": [False, False],
            "crime_count": [3, 99],
        }
    )
    both = pd.concat([monthly, hackney], ignore_index=True)
    assert app.top_types(both, BOUNDS, "Hackney", True) == ["Drugs"]


def test_the_default_selection_follows_the_borough(fake: dict) -> None:
    monthly = fake["mart_borough_monthly"]
    assert app.top_types(monthly, BOUNDS, "Camden", True) == ["Burglary", app.ASB]
    assert app.top_types(monthly, BOUNDS, app.ALL_LONDON, True) == ["Burglary", app.ASB]
    assert app.top_types(monthly, BOUNDS, "Camden", False) == ["Burglary"]


# ------------------------------------------------------------------- palette


def test_the_palette_has_no_repeated_colours_and_covers_every_crime_type() -> None:
    lowered = [c.lower() for c in app.PALETTE]
    assert len(lowered) == len(set(lowered))
    assert app.TOTAL_COLOUR.lower() not in lowered
    assert len(app.PALETTE) >= 14  # the 14 crime types, ASB included


def test_each_crime_type_has_its_own_colour_whatever_the_selection(fake: dict) -> None:
    monthly = fake["mart_borough_monthly"]
    colours = app.crime_type_colours(monthly)
    assert len(set(colours.values())) == len(colours) == 2
    # Colours depend on the whole table, not on the borough or the chosen types.
    assert (
        app.crime_type_colours(monthly[monthly["local_authority"] == "Camden"])
        == colours
    )
    figure = trend(monthly, "Westminster")
    assert line(figure, "Burglary").line.color == colours["Burglary"]
    bars = app.top_crime_types_figure(monthly, BOUNDS, "Camden", colours)
    assert bars.data[0].marker.color == colours["Burglary"]


def test_more_crime_types_than_colours_raises() -> None:
    monthly = pd.DataFrame(
        {"crime_type": [f"t{i}" for i in range(len(app.PALETTE) + 1)]}
    )
    with pytest.raises(ValueError, match="colours"):
        app.crime_type_colours(monthly)


def long_monthly(first: str, last: str) -> pd.DataFrame:
    """One Camden row per month from first to last (borough monthly columns)."""
    months = pd.date_range(first, last, freq="MS")
    return pd.DataFrame(
        {
            "local_authority": "Camden",
            "month": months,
            "crime_type": "Burglary",
            "is_asb": False,
            "crime_count": 10,
        }
    )


def met_change_lines(figure) -> list:
    return [
        shape
        for shape in figure.layout.shapes
        if shape.type == "line" and shape.x0 == app.MET_CHANGE_MONTH
    ]


def test_trend_marks_the_met_recording_change_when_the_month_is_on_the_chart() -> None:
    monthly = long_monthly("2026-01-01", "2026-08-01")
    for borough in ("Camden", app.ALL_LONDON):
        figure = trend(monthly, borough)
        assert len(met_change_lines(figure)) == 1
        assert [a.text for a in figure.layout.annotations] == ["Met recording change"]
        assert figure.layout.annotations[0].x == pd.Timestamp("2026-05-01")


def test_trend_has_no_line_when_the_month_is_outside_the_data(fake: dict) -> None:
    figure = trend(fake["mart_borough_monthly"], "Camden")  # Jul-Aug
    assert not figure.layout.shapes and not figure.layout.annotations


def test_trend_has_no_met_line_for_the_city_of_london() -> None:
    monthly = long_monthly("2026-01-01", "2026-08-01").assign(
        local_authority="City of London"
    )
    figure = trend(monthly, "City of London")
    assert not figure.layout.shapes and not figure.layout.annotations


def test_anomalies_table_is_formatted_newest_first(fake: dict) -> None:
    shown = app.anomalies_table(fake["dq_volume_anomalies"])

    assert list(shown.columns) == [
        "Force", "Crime type", "Month", "Count", "Baseline median", "Change",
        "Status", "Note",
    ]  # fmt: skip
    assert shown["Month"].tolist() == ["2026-08", "2026-05"]
    assert shown["Change"].tolist() == ["+100.0%", "-94.9%"]
    assert shown["Status"].tolist() == ["Not acknowledged", "Acknowledged"]
    assert shown["Note"].tolist() == ["", "Source change (unverified)."]


def test_top_crime_types_figure_filters_by_borough_and_drops_asb(fake: dict) -> None:
    monthly = fake["mart_borough_monthly"]
    colours = app.crime_type_colours(monthly)
    camden = app.top_crime_types_figure(monthly, BOUNDS, "Camden", colours)
    assert list(camden.data[0].y) == ["Burglary"] and list(camden.data[0].x) == [70]
    london = app.top_crime_types_figure(monthly, BOUNDS, app.ALL_LONDON, colours)
    assert list(london.data[0].x) == [170]


def test_borough_outcomes_filters_and_recomputes_shares(fake: dict) -> None:
    outcomes = fake["mart_borough_outcomes"]
    westminster = app.borough_outcomes(outcomes, "Westminster").set_index(
        "outcome_group"
    )
    assert westminster.loc["charged_or_court", "crime_count"] == 30
    assert westminster.loc["charged_or_court", "share_of_crime_type"] == pytest.approx(
        0.3
    )

    london = app.borough_outcomes(outcomes, app.ALL_LONDON).set_index("outcome_group")
    assert london.loc["charged_or_court", "crime_count"] == 40
    assert london.loc["charged_or_court", "share_of_crime_type"] == pytest.approx(0.2)
    assert london["share_of_crime_type"].sum() == pytest.approx(1.0)


def test_outcome_figure_has_one_trace_per_outcome_group(fake: dict) -> None:
    figure = app.outcome_figure(
        app.borough_outcomes(fake["mart_borough_outcomes"], "Camden")
    )
    assert {t.name for t in figure.data} == {"Charged or at court", "No further action"}


def outcomes_for_sorting() -> pd.DataFrame:
    """Three crime types with charged shares 10%, 40% and none (no charged row at all)."""
    rows = [
        ("Robbery", "charged_or_court", 10, 0.10),
        ("Robbery", "no_further_action", 90, 0.90),
        ("Drugs", "charged_or_court", 40, 0.40),
        ("Drugs", "no_further_action", 60, 0.60),
        ("Bicycle theft", "no_further_action", 100, 1.00),
    ]
    return pd.DataFrame(
        rows,
        columns=["crime_type", "outcome_group", "crime_count", "share_of_crime_type"],
    )


def test_outcome_types_are_sorted_by_charged_share_highest_first() -> None:
    outcomes = outcomes_for_sorting()
    assert app.outcome_order(outcomes) == ["Drugs", "Robbery", "Bicycle theft"]

    figure = app.outcome_figure(outcomes)
    # A horizontal plotly axis lists categories from the bottom up, so the last one is on top.
    bottom_to_top = list(figure.layout.yaxis.categoryarray)
    assert bottom_to_top == ["Bicycle theft", "Robbery", "Drugs"]
    assert figure.layout.yaxis.autorange is None  # not reversed: Drugs really is on top


def test_outcome_order_breaks_ties_by_name() -> None:
    rows = [("B", "charged_or_court", 1, 0.2), ("A", "charged_or_court", 1, 0.2)]
    frame = pd.DataFrame(
        rows,
        columns=["crime_type", "outcome_group", "crime_count", "share_of_crime_type"],
    )
    assert app.outcome_order(frame) == ["A", "B"]


def test_hotspot_figure_filters_by_borough_and_drops_missing_coordinates(
    fake: dict,
) -> None:
    ranking = fake["mart_lsoa_ranking"]
    count = lambda figure: sum(len(t.lat) for t in figure.data)

    assert count(app.hotspot_figure(ranking, "Camden")) == 1  # E01B has no coordinates
    assert count(app.hotspot_figure(ranking, "Westminster")) == 1
    assert count(app.hotspot_figure(ranking, app.ALL_LONDON)) == 2


def test_top_lsoas_are_the_borough_s_biggest(fake: dict) -> None:
    ranking = fake["mart_lsoa_ranking"]
    assert list(app.top_lsoas(ranking, "Camden")["lsoa_code"]) == ["E01A", "E01B"]
    assert list(app.top_lsoas(ranking, app.ALL_LONDON, n=1)["lsoa_code"]) == ["E01C"]


def test_map_zooms_further_in_for_a_smaller_area(fake: dict) -> None:
    ranking = fake["mart_lsoa_ranking"]
    london_points = ranking[ranking["latitude"].notna()]
    wide = pd.DataFrame({"latitude": [51.29, 51.69], "longitude": [-0.50, 0.30]})
    narrow = pd.DataFrame({"latitude": [51.52, 51.56], "longitude": [-0.18, -0.10]})

    wide_centre, wide_zoom = app.map_view(wide)
    narrow_centre, narrow_zoom = app.map_view(narrow)
    assert narrow_zoom > wide_zoom + 1
    assert 8 <= wide_zoom <= 15 and 8 <= narrow_zoom <= 15
    assert narrow_centre == {"lat": pytest.approx(51.54), "lon": pytest.approx(-0.14)}
    assert app.map_view(london_points.iloc[0:0])[1] == 9.0  # no points: default view
    assert wide_centre["lat"] == pytest.approx(51.49)


def test_all_london_map_fits_greater_london_and_a_borough_zooms_to_fit(
    fake: dict,
) -> None:
    centre, zoom = app.london_view()
    box = app.GREATER_LONDON
    assert centre["lat"] == pytest.approx(sum(box["lat"]) / 2)
    assert centre["lon"] == pytest.approx(sum(box["lon"]) / 2)
    assert 9.0 < zoom < 10.0  # Greater London tall enough to fill a 560 px frame
    # No more padding than before: zoomed in at least as far as fitting the same box loosely.
    assert zoom > app.fit_view(box["lat"], box["lon"])[1]

    ranking = fake["mart_lsoa_ranking"]
    all_london = app.hotspot_figure(ranking, app.ALL_LONDON).layout.map
    camden = app.hotspot_figure(ranking, "Camden").layout.map
    assert all_london.zoom == pytest.approx(zoom)
    assert camden.zoom > all_london.zoom  # a borough fits its own LSOAs


# ------------------------------------------------------------------ formatting


def test_integers_get_thousands_separators() -> None:
    assert [app.fmt_int(v) for v in (0, 7, 999, 1000, 886712, 3416073)] == [
        "0", "7", "999", "1,000", "886,712", "3,416,073",
    ]  # fmt: skip


def test_percentages_have_one_decimal_place() -> None:
    assert app.fmt_pct(0.2090) == "20.9%"
    assert app.fmt_pct(0.04915) == "4.9%"
    assert app.fmt_pct(0.5) == "50.0%"
    assert app.fmt_pct(1.0) == "100.0%"
    assert app.fmt_pct(0.0) == "0.0%"
    assert app.fmt_pct(0.0004) == "<0.1%"  # a real but tiny share is not shown as zero
    assert app.fmt_pct(float("nan")) == ""
    assert app.fmt_change(-94.886627) == "-94.9%"
    assert app.fmt_change(100.0) == "+100.0%"
    assert app.fmt_change(1234.56) == "+1,234.6%"


def test_styled_tables_show_separators_but_keep_the_numbers() -> None:
    frame = pd.DataFrame({"Crimes": [10107, 950], "Share": [0.0004, 0.5]})
    styled = app.styled(frame, {"Crimes": app.fmt_int, "Share": app.fmt_pct})
    html = styled.to_html()
    assert "10,107" in html and "<0.1%" in html and "50.0%" in html
    assert frame["Crimes"].tolist() == [10107, 950]  # the data itself is untouched


# ------------------------------------------------------------ data-quality labels


def reconciliation_frame() -> pd.DataFrame:
    rows = [
        ("reconciliation", "bronze_rows", 3440762, 1.0),
        ("reconciliation", "archive_replaced", 0, 0.0),
        ("quarantine", "conflicting_duplicate_crime_id", 1279, 0.0004),
        ("silver_flags", "no_location_rows", 12209, 0.0036),
        ("gold", "orphan_fact_keys", 0, None),
        ("volume_anomalies", "flagged", 2, None),
        ("new_group", "brand_new_item", 5, None),
    ]
    return pd.DataFrame(rows, columns=["group", "item", "rows", "share"])


def test_data_quality_uses_plain_english_headings_and_labels() -> None:
    tables = dict(app.quality_tables(reconciliation_frame()))

    assert list(tables) == [
        "Where every bronze row went", "Quarantined rows, by reason",
        "Flags on kept rows", "Gold layer checks", "Volume check", "New group",
    ]  # fmt: skip
    assert tables["Where every bronze row went"]["Item"].tolist() == [
        "Bronze rows",
        "Replaced by a newer archive",
    ]
    assert tables["Quarantined rows, by reason"]["Item"].tolist() == [
        "Conflicting duplicate Crime IDs"
    ]
    assert tables["Flags on kept rows"]["Item"].tolist() == ["Rows without location"]
    assert tables["New group"]["Item"].tolist() == ["Brand new item"]  # fallback
    for heading, table in tables.items():
        assert "_" not in heading
        assert not table["Item"].str.contains("_").any()


def test_every_item_the_export_writes_has_a_plain_label() -> None:
    committed = committed_tables()["reconciliation"]
    assert set(committed["group"]) <= set(app.QUALITY_GROUPS)
    assert set(committed["item"]) <= set(app.QUALITY_ITEMS)


# ------------------------------------------------------ All London = London totals


def test_all_london_figures_match_the_london_totals_in_the_committed_snapshot() -> None:
    tables = committed_tables()
    snapshot = app.load_snapshot(COMMITTED_DATA)
    bounds = app.window_bounds(snapshot)
    monthly, outcomes, ranking = (
        tables["mart_borough_monthly"],
        tables["mart_borough_outcomes"],
        tables["mart_lsoa_ranking"],
    )
    london = app.borough_headline(monthly, outcomes, bounds, app.ALL_LONDON)

    assert (
        london["crimes"] == ranking["crime_count"].sum()
    )  # the ranking mart's London total
    assert london["asb"] == ranking["asb_count"].sum()
    assert app.borough_outcomes(outcomes, app.ALL_LONDON)["crime_count"].sum() == (
        outcomes["crime_count"].sum()
    )
    boroughs = app.borough_options(monthly)[1:]
    assert (
        sum(
            app.borough_headline(monthly, outcomes, bounds, b)["crimes"]
            for b in boroughs
        )
        == london["crimes"]
    )
    assert sum(
        len(t.lat) for t in app.hotspot_figure(ranking, app.ALL_LONDON).data
    ) == (ranking["latitude"].notna().sum())


# -------------------------------------------------------------- the page itself


def run_page(borough: str | None = None) -> AppTest:
    if not COMMITTED_DATA.is_dir():
        pytest.skip("no committed snapshot")
    page = AppTest.from_file(str(APP_PATH), default_timeout=120)
    if borough is not None:
        page.query_params["borough"] = borough
    return page.run()


def test_the_page_defaults_to_all_london() -> None:
    page = run_page()

    assert not page.exception
    selector = page.sidebar.selectbox[0]
    assert selector.label == "Borough" and selector.value == "All London"
    assert len(selector.options) == 34
    assert "borough" not in page.query_params
    assert [m.label for m in page.metric] == [
        "Crimes, last 12 months",
        "Anti-social behaviour",
        "Charged or at court",
    ]
    assert [h.value for h in page.header] == [
        "All London",
        "Records by month",
        "Top crime types, last 12 months",
        "Outcomes by crime type",
        "Hotspots, last 12 months",
        "Data quality",
    ]


@pytest.mark.parametrize("borough", ["Camden", "Westminster"])
def test_a_borough_in_the_url_is_selected_and_shown(borough: str) -> None:
    page = run_page(borough)

    assert not page.exception
    assert page.sidebar.selectbox[0].value == borough
    assert page.header[0].value == borough
    assert page.query_params["borough"] == borough
    assert any("of London's recorded crime" in m.value for m in page.markdown)
    captions = " ".join(c.value for c in page.caption)
    assert "Counts, not rates" in captions and "no population denominator" in captions
    assert "London LSOAs only" in captions


def test_an_unknown_borough_in_the_url_falls_back_to_all_london() -> None:
    page = run_page("Atlantis")

    assert not page.exception
    assert page.sidebar.selectbox[0].value == "All London"
    assert page.header[0].value == "All London"
    assert "borough" not in page.query_params


def test_changing_the_selector_updates_the_url_and_the_page() -> None:
    page = run_page()
    page.sidebar.selectbox[0].select("Camden").run()

    assert not page.exception
    assert page.header[0].value == "Camden"
    assert page.query_params["borough"] == "Camden"
    page.sidebar.selectbox[0].select("All London").run()
    assert page.header[0].value == "All London" and "borough" not in page.query_params


def test_every_borough_view_carries_the_counts_not_rates_caption() -> None:
    for borough in (None, "Hackney"):
        captions = " ".join(c.value for c in run_page(borough).caption)
        assert app.BOROUGH_CAPTION in captions


def test_the_data_quality_panel_and_footer_are_unchanged() -> None:
    page = run_page("Camden")
    assert page.header[-1].value == "Data quality"
    captions = " ".join(c.value for c in page.caption)
    assert "excluding 2024-10 (outlier)" in captions
    assert "not where residents are at higher risk" in captions
    assert "Snapshot of archive" in captions


def test_the_data_quality_panel_lists_the_flagged_volume_anomalies() -> None:
    page = run_page()
    assert page.subheader[-1].value == "Volume anomalies"
    committed = pd.read_parquet(COMMITTED_DATA / "dq_volume_anomalies.parquet")
    shown = page.dataframe[-1].value  # the anomalies table is the last one on the page
    assert len(shown) == len(committed) > 0
    assert set(shown["Status"]) <= {"Acknowledged", "Not acknowledged"}
    assert "Criminal damage and arson" in set(shown["Crime type"])


# ----------------------------------------------------------- committed snapshot


def test_committed_snapshot_is_small_and_has_the_documented_columns() -> None:
    if not COMMITTED_DATA.is_dir():
        pytest.skip("no committed snapshot")
    total = sum(p.stat().st_size for p in COMMITTED_DATA.iterdir())
    assert total < export.MAX_TOTAL_BYTES
    for name, columns in export.EXPORT_COLUMNS.items():
        assert (
            list(pd.read_parquet(COMMITTED_DATA / f"{name}.parquet").columns) == columns
        )


@pytest.mark.slow
def test_loaders_read_what_the_export_writes(spark, export_env: Path) -> None:
    prepare_gold(spark, sample_rows())
    export.run_export(spark)

    tables = app.load_tables(data_dir=export_env)
    snapshot = app.load_snapshot(export_env)

    for name, columns in export.EXPORT_COLUMNS.items():
        assert list(tables[name].columns) == columns
    assert snapshot["archive_month"] == "2026-08"
    options = app.borough_options(tables["mart_borough_monthly"])
    assert options == ["All London", "Camden", "Hounslow"]
    headline = app.borough_headline(
        tables["mart_borough_monthly"],
        tables["mart_borough_outcomes"],
        app.window_bounds(snapshot),
        app.ALL_LONDON,
    )
    assert headline["crimes"] + headline["asb"] > 0
    assert app.hotspot_figure(tables["mart_lsoa_ranking"], app.ALL_LONDON).data
