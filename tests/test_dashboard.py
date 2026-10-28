"""Dashboard tests: the data-loading functions in dashboard/app.py and the page itself.

These need the optional `dashboard` dependency group (pandas, plotly, streamlit); they are
skipped where it is not installed. The page test runs the real app headlessly against the
committed snapshot in dashboard/data/.
"""

import importlib.util
import json
from pathlib import Path

import pytest

pd = pytest.importorskip("pandas")
pytest.importorskip("plotly")
pytest.importorskip("streamlit")

from streamlit.testing.v1 import AppTest

from tests.test_export import prepare_gold, sample_rows
from uk_crime import export

APP_PATH = Path(__file__).resolve().parents[1] / "dashboard" / "app.py"
COMMITTED_DATA = APP_PATH.parent / "data"

spec = importlib.util.spec_from_file_location("dashboard_app", APP_PATH)
app = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
spec.loader.exec_module(app)  # type: ignore[union-attr]


def write_fake_snapshot(folder: Path) -> None:
    """Small Parquet files with the exported columns, written with pandas."""
    folder.mkdir(parents=True)
    months = pd.to_datetime(["2026-07-01", "2026-08-01"]).date
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
            "crime_count": [10, 90],
            "share_of_crime_type": [0.1, 0.9],
            "first_month": [months[0]] * 2,
            "last_month": [months[1]] * 2,
            "months_used": [2, 2],
            "excluded_months": ["2024-10"] * 2,
        }
    ).to_parquet(folder / "mart_outcome_rates.parquet")
    pd.DataFrame(
        {
            "lsoa_code": ["E01A", "E01B"],
            "lsoa_name": ["Camden 001A", "Camden 002B"],
            "local_authority": ["Camden"] * 2,
            "crime_count": [5, 3],
            "asb_count": [1, 0],
            "rank_in_local_authority": [1, 2],
            "first_month": [months[0]] * 2,
            "last_month": [months[1]] * 2,
            "latitude": [51.5, None],
            "longitude": [-0.1, None],
        }
    ).to_parquet(folder / "mart_lsoa_ranking.parquet")
    pd.DataFrame(
        {
            "group": ["reconciliation", "reconciliation"],
            "item": ["bronze_rows", "quarantined"],
            "rows": [101, 1],
            "share": [1.0, 1 / 101],
        }
    ).to_parquet(folder / "reconciliation.parquet")
    (folder / "snapshot.json").write_text(json.dumps({"archive_month": "2026-08"}))


# ------------------------------------------------------------- loading functions


def test_loaders_read_the_exported_files(tmp_path: Path) -> None:
    write_fake_snapshot(tmp_path / "data")
    tables = app.load_tables(tmp_path / "data")

    assert set(tables) == set(app.TABLE_NAMES) == set(export.EXPORT_COLUMNS)
    assert tables["mart_monthly_crimes"]["month"].dtype.kind == "M"  # real datetimes
    assert (
        list(tables["mart_lsoa_ranking"].columns)
        == export.EXPORT_COLUMNS["mart_lsoa_ranking"]
    )
    assert app.load_snapshot(tmp_path / "data") == {"archive_month": "2026-08"}


def test_headline_figures(tmp_path: Path) -> None:
    write_fake_snapshot(tmp_path / "data")
    figures = app.headline_figures(app.load_tables(tmp_path / "data"))
    assert figures["records"] == 100
    assert figures["asb_share"] == pytest.approx(0.30)
    assert figures["charge_rate"] == pytest.approx(0.10)
    assert figures["quarantined"] == 1


def test_trend_figure_filters_forces_and_asb(tmp_path: Path) -> None:
    write_fake_snapshot(tmp_path / "data")
    monthly = app.load_tables(tmp_path / "data")["mart_monthly_crimes"]
    forces = ["Metropolitan Police Service"]

    with_asb = app.trend_figure(monthly, forces, include_asb=True)
    without = app.trend_figure(monthly, forces, include_asb=False)
    assert {t.name for t in with_asb.data} == {"Burglary", app.ASB}
    assert {t.name for t in without.data} == {"Burglary"}
    assert len(app.trend_figure(monthly, ["City of London Police"], True).data) == 0


def test_hotspot_figure_drops_lsoas_without_coordinates(tmp_path: Path) -> None:
    write_fake_snapshot(tmp_path / "data")
    ranking = app.load_tables(tmp_path / "data")["mart_lsoa_ranking"]
    figure = app.hotspot_figure(ranking, ["Camden"])
    assert sum(len(t.lat) for t in figure.data) == 1


def test_outcome_figure_has_one_trace_per_outcome_group(tmp_path: Path) -> None:
    write_fake_snapshot(tmp_path / "data")
    outcomes = app.load_tables(tmp_path / "data")["mart_outcome_rates"]
    assert {t.name for t in app.outcome_figure(outcomes).data} == {
        "Charged or at court",
        "No further action",
    }


@pytest.mark.slow
def test_loaders_read_what_the_export_writes(spark, export_env: Path) -> None:
    prepare_gold(spark, sample_rows())
    export.run_export(spark)

    tables = app.load_tables(export_env)
    snapshot = app.load_snapshot(export_env)

    for name, columns in export.EXPORT_COLUMNS.items():
        assert list(tables[name].columns) == columns
    assert snapshot["archive_month"] == "2026-08"
    figures = app.headline_figures(tables)
    assert figures["records"] == len(sample_rows())
    assert app.hotspot_figure(
        tables["mart_lsoa_ranking"],
        sorted(tables["mart_lsoa_ranking"]["local_authority"].unique()),
    ).data


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


def test_the_page_renders_from_the_committed_snapshot() -> None:
    if not COMMITTED_DATA.is_dir():
        pytest.skip("no committed snapshot")
    page = AppTest.from_file(str(APP_PATH), default_timeout=120).run()

    assert not page.exception
    assert [m.label for m in page.metric] == [
        "Records, 36 months",
        "Anti-social behaviour",
        "Charged or at court",
        "Quarantined rows",
    ]
    assert [h.value for h in page.header] == [
        "Records by month",
        "Outcomes by crime type",
        "London hotspots, last 12 months",
        "Data quality",
    ]
    captions = " ".join(c.value for c in page.caption)
    assert "excluding 2024-10 (outlier)" in captions
    assert "not where residents are at higher risk" in captions
    assert "Snapshot of archive" in captions
