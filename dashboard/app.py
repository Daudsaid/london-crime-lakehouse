"""Streamlit dashboard over the committed gold snapshot.

Reads only the files in dashboard/data/ (written by `make export`); it needs no Spark and
no data/ directory. Run it with `make dashboard`. The sidebar borough selector is kept in
the URL (?borough=Camden) so a view can be shared.
"""

import json
import math
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

DATA_DIR = Path(__file__).resolve().parent / "data"
REPO_URL = "https://github.com/Daudsaid/london-crime-lakehouse"
CAVEATS_URL = f"{REPO_URL}/blob/main/docs/data-caveats.md"
ASB = "Anti-social behaviour"
ALL_LONDON = "All London"
TABLE_NAMES = (
    "mart_monthly_crimes",
    "mart_outcome_rates",
    "mart_lsoa_ranking",
    "mart_borough_monthly",
    "mart_borough_outcomes",
    "reconciliation",
)
OUTCOME_COLOURS = {
    "no_further_action": "#9aa5b1",
    "charged_or_court": "#1f6fb2",
    "out_of_court": "#2a9d8f",
    "other_organisation": "#e9c46a",
    "pending": "#e76f51",
}
OUTCOME_LABELS = {
    "no_further_action": "No further action",
    "charged_or_court": "Charged or at court",
    "out_of_court": "Out of court",
    "other_organisation": "Other organisation",
    "pending": "Pending",
}
BOROUGH_CAPTION = (
    "Counts, not rates: there is no population denominator yet. London LSOAs only, so "
    "records outside London or without an LSOA are not included."
)
LONDON_CENTRE = {"lat": 51.51, "lon": -0.11}


# ------------------------------------------------------------------ data loading


def load_snapshot(data_dir: Path = DATA_DIR) -> dict:
    """Read snapshot.json (archive month, silver run id, export time, windows)."""
    return json.loads((data_dir / "snapshot.json").read_text())


def snapshot_version(snapshot: dict) -> str:
    """Identify the snapshot's data: export time plus the silver run it came from."""
    return f"{snapshot['exported_at']}|{snapshot['silver_run_id']}"


def load_tables(
    table_names: tuple[str, ...] = TABLE_NAMES,
    version: str = "",
    data_dir: Path = DATA_DIR,
) -> dict[str, pd.DataFrame]:
    """Read the exported Parquet files; month columns become datetimes.

    `version` is not used in the body. It exists so that, once the function is wrapped in
    st.cache_data, the cache key changes with the snapshot: st.cache_data keys on the
    function's own code and its arguments, not on module globals such as TABLE_NAMES.
    """
    tables = {n: pd.read_parquet(data_dir / f"{n}.parquet") for n in table_names}
    for df in tables.values():
        for column in ("month", "first_month", "last_month"):
            if column in df.columns:
                df[column] = pd.to_datetime(df[column])
    return tables


cached_load_tables = st.cache_data(load_tables)


# --------------------------------------------------------------- borough selection


def borough_options(borough_monthly: pd.DataFrame) -> list[str]:
    """The selector list: "All London" first, then the boroughs sorted by name."""
    return [ALL_LONDON, *sorted(borough_monthly["local_authority"].unique())]


def resolve_borough(requested: str | None, options: list[str]) -> str:
    """The borough to show for a URL value; anything unknown falls back to All London."""
    if requested in options:
        return requested  # type: ignore[return-value]
    by_lower = {o.lower(): o for o in options}
    return by_lower.get((requested or "").strip().lower(), ALL_LONDON)


def for_borough(df: pd.DataFrame, borough: str) -> pd.DataFrame:
    """Rows of a borough mart for the selection (all of London keeps every borough)."""
    if borough == ALL_LONDON:
        return df
    return df[df["local_authority"] == borough]


def window_bounds(snapshot: dict) -> tuple[pd.Timestamp, pd.Timestamp]:
    """First and last month (inclusive) of the 12-month window, as timestamps."""
    window = snapshot["lsoa_window"]
    return pd.Timestamp(f"{window['first']}-01"), pd.Timestamp(f"{window['last']}-01")


def in_window(
    monthly: pd.DataFrame, bounds: tuple[pd.Timestamp, pd.Timestamp]
) -> pd.DataFrame:
    """Rows of mart_borough_monthly inside the 12-month window."""
    return monthly[(monthly["month"] >= bounds[0]) & (monthly["month"] <= bounds[1])]


def borough_headline(
    monthly: pd.DataFrame,
    outcomes: pd.DataFrame,
    bounds: tuple[pd.Timestamp, pd.Timestamp],
    borough: str,
) -> dict[str, float | int]:
    """Crimes and ASB over the last 12 months, and the charge rate over the mature window.

    "Crimes" excludes anti-social behaviour; the ASB share is of all records.
    """
    recent = for_borough(in_window(monthly, bounds), borough)
    crimes = int(recent.loc[~recent["is_asb"], "crime_count"].sum())
    asb = int(recent.loc[recent["is_asb"], "crime_count"].sum())
    chosen = for_borough(outcomes, borough)
    mature = int(chosen["crime_count"].sum())
    charged = int(
        chosen.loc[chosen["outcome_group"] == "charged_or_court", "crime_count"].sum()
    )
    return {
        "crimes": crimes,
        "asb": asb,
        "asb_share": asb / (crimes + asb) if crimes + asb else 0.0,
        "charge_rate": charged / mature if mature else 0.0,
    }


def london_comparison(
    monthly: pd.DataFrame, bounds: tuple[pd.Timestamp, pd.Timestamp], borough: str
) -> str:
    """One line placing the selection against London's recorded crime."""
    recent = in_window(monthly, bounds)
    london = int(recent.loc[~recent["is_asb"], "crime_count"].sum())
    if borough == ALL_LONDON:
        return f"All of London: {london:,} crimes recorded in the last 12 months."
    own = int(
        for_borough(recent, borough).loc[lambda d: ~d["is_asb"], "crime_count"].sum()
    )
    share = own / london if london else 0.0
    return (
        f"{borough} accounts for {share:.1%} of London's recorded crime in the last "
        f"12 months ({own:,} of {london:,} crimes)."
    )


def borough_outcomes(outcomes: pd.DataFrame, borough: str) -> pd.DataFrame:
    """Outcome counts and shares per crime type for the selection.

    For All London the boroughs are added together and the shares recomputed. The result
    has the columns of mart_outcome_rates that outcome_figure uses.
    """
    chosen = for_borough(outcomes, borough)
    counts = chosen.groupby(["crime_type", "outcome_group"], as_index=False)[
        "crime_count"
    ].sum()
    totals = counts.groupby("crime_type")["crime_count"].transform("sum")
    return counts.assign(share_of_crime_type=counts["crime_count"] / totals)


def top_lsoas(ranking: pd.DataFrame, borough: str, n: int = 10) -> pd.DataFrame:
    """The n LSOAs with the most crimes in the selection."""
    return for_borough(ranking, borough).nlargest(n, "crime_count")


# --------------------------------------------------------------------- figures


def trend_figure(monthly: pd.DataFrame, borough: str, include_asb: bool) -> go.Figure:
    """Monthly record counts by crime type for the selection."""
    data = for_borough(monthly, borough)
    if not include_asb:
        data = data[~data["is_asb"]]
    data = data.groupby(["month", "crime_type"], as_index=False)["crime_count"].sum()
    figure = px.line(
        data,
        x="month",
        y="crime_count",
        color="crime_type",
        labels={"month": "Month", "crime_count": "Records", "crime_type": "Crime type"},
    )
    figure.update_layout(legend_title_text="", margin={"t": 10}, hovermode="x unified")
    return figure


def top_crime_types_figure(
    monthly: pd.DataFrame, bounds: tuple[pd.Timestamp, pd.Timestamp], borough: str
) -> go.Figure:
    """Horizontal bars of crimes by type over the last 12 months (ASB excluded)."""
    recent = for_borough(in_window(monthly, bounds), borough)
    data = (
        recent[~recent["is_asb"]]
        .groupby("crime_type", as_index=False)["crime_count"]
        .sum()
        .sort_values("crime_count")
    )
    figure = px.bar(
        data,
        x="crime_count",
        y="crime_type",
        orientation="h",
        labels={"crime_count": "Crimes", "crime_type": ""},
    )
    figure.update_layout(margin={"t": 10}, yaxis={"categoryorder": "total ascending"})
    return figure


def outcome_figure(outcomes: pd.DataFrame) -> go.Figure:
    """Stacked horizontal bars: share of each outcome group within a crime type."""
    order = (
        outcomes.groupby("crime_type")["crime_count"].sum().sort_values().index.tolist()
    )
    data = outcomes.assign(
        outcome=outcomes["outcome_group"].map(OUTCOME_LABELS),
        percent=outcomes["share_of_crime_type"] * 100,
    )
    figure = px.bar(
        data,
        x="percent",
        y="crime_type",
        color="outcome",
        orientation="h",
        category_orders={"crime_type": order, "outcome": list(OUTCOME_LABELS.values())},
        color_discrete_map={OUTCOME_LABELS[k]: v for k, v in OUTCOME_COLOURS.items()},
        hover_data={"crime_count": ":,", "percent": ":.1f"},
        labels={"percent": "Share of crimes (%)", "crime_type": "", "outcome": ""},
    )
    figure.update_layout(barmode="stack", margin={"t": 10}, legend_title_text="")
    return figure


def map_view(points: pd.DataFrame) -> tuple[dict[str, float], float]:
    """Map centre and zoom that fit the points (centre of their bounding box).

    Uses Web Mercator scale (512-pixel tiles) for a map about 1,000 by 560 pixels, with a
    little padding; the zoom is kept between 8 and 15.
    """
    if points.empty:
        return dict(LONDON_CENTRE), 9.0
    lat_min, lat_max = points["latitude"].min(), points["latitude"].max()
    lon_min, lon_max = points["longitude"].min(), points["longitude"].max()
    centre_lat = (lat_min + lat_max) / 2
    lat_span = max(lat_max - lat_min, 0.005)
    lon_span = max(lon_max - lon_min, 0.005)
    by_height = math.log2(
        560 * 360 * math.cos(math.radians(centre_lat)) / (512 * lat_span)
    )
    by_width = math.log2(1000 * 360 / (512 * lon_span))
    zoom = min(by_height, by_width) - 0.3
    return {"lat": centre_lat, "lon": (lon_min + lon_max) / 2}, max(
        8.0, min(15.0, zoom)
    )


def hotspot_figure(ranking: pd.DataFrame, borough: str) -> go.Figure:
    """Map of the selection's LSOAs sized and coloured by non-ASB crime count, zoomed to fit."""
    data = for_borough(ranking, borough)
    data = data[data["latitude"].notna()]
    centre, zoom = map_view(data)
    figure = px.scatter_map(
        data,
        lat="latitude",
        lon="longitude",
        size="crime_count",
        color="crime_count",
        hover_name="lsoa_name",
        hover_data={
            "local_authority": True,
            "crime_count": ":,",
            "asb_count": ":,",
            "rank_in_local_authority": True,
            "latitude": False,
            "longitude": False,
        },
        size_max=28,
        zoom=zoom,
        center=centre,
        map_style="carto-positron",
        color_continuous_scale="YlOrRd",
        labels={"crime_count": "Crimes", "asb_count": "ASB"},
    )
    figure.update_layout(margin={"t": 0, "b": 0, "l": 0, "r": 0}, height=560)
    return figure


# ------------------------------------------------------------------------ page


def month_label(value: str | None) -> str:
    return value or "n/a"


def main() -> None:
    st.set_page_config(page_title="London Crime Lakehouse", layout="wide")
    snapshot = load_snapshot()  # tiny, so not cached; its version keys the tables cache
    tables = cached_load_tables(TABLE_NAMES, snapshot_version(snapshot))
    monthly = tables["mart_borough_monthly"]
    outcome = snapshot["outcome"]
    window = snapshot["lsoa_window"]
    bounds = window_bounds(snapshot)

    options = borough_options(monthly)
    if "borough" not in st.session_state:
        st.session_state["borough"] = resolve_borough(
            st.query_params.get("borough"), options
        )
    borough = st.sidebar.selectbox("Borough", options, key="borough")
    if borough == ALL_LONDON:
        st.query_params.pop("borough", None)
    else:
        st.query_params["borough"] = borough

    st.title("London Crime Lakehouse")
    st.caption(
        "Metropolitan and City of London Police street-level records, "
        f"{snapshot['months']['first']} to {snapshot['months']['last']}. "
        "A snapshot of the gold layer; see the repository for how it is built."
    )

    st.header(borough)
    st.caption(BOROUGH_CAPTION)
    figures = borough_headline(
        monthly, tables["mart_borough_outcomes"], bounds, borough
    )
    columns = st.columns(3)
    columns[0].metric(
        "Crimes, last 12 months",
        f"{figures['crimes']:,}",
        help=(
            f"Records in London LSOAs, {month_label(window['first'])} to "
            f"{month_label(window['last'])}, excluding anti-social behaviour."
        ),
    )
    columns[1].metric(
        "Anti-social behaviour",
        f"{figures['asb_share']:.1%}",
        help=f"Share of all records in the same 12 months ({figures['asb']:,} records).",
    )
    columns[2].metric(
        "Charged or at court",
        f"{figures['charge_rate']:.1%}",
        help=(
            "Share of non-ASB records in the outcome group charged_or_court, using mature "
            f"months only ({month_label(outcome['first_month'])} to "
            f"{month_label(outcome['last_month'])})."
        ),
    )
    st.markdown(london_comparison(monthly, bounds, borough))

    st.header("Records by month")
    include_asb = st.checkbox("Include anti-social behaviour", value=True)
    st.plotly_chart(trend_figure(monthly, borough, include_asb), width="stretch")

    st.header("Top crime types, last 12 months")
    st.plotly_chart(top_crime_types_figure(monthly, bounds, borough), width="stretch")

    st.header("Outcomes by crime type")
    excluded = ", ".join(outcome["excluded_months"]) or "none"
    st.plotly_chart(
        outcome_figure(borough_outcomes(tables["mart_borough_outcomes"], borough)),
        width="stretch",
    )
    st.caption(
        f"Period: {month_label(outcome['first_month'])} to {month_label(outcome['last_month'])} "
        f"({outcome['months_used']} months), excluding {excluded} (outlier). The newest "
        f"{outcome['maturity_months']} months are left out because their outcomes are not "
        "mature yet. Anti-social behaviour has no outcomes."
    )

    st.header("Hotspots, last 12 months")
    ranking = tables["mart_lsoa_ranking"]
    st.plotly_chart(hotspot_figure(ranking, borough), width="stretch")
    st.caption(
        f"Crimes per LSOA, {month_label(window['first'])} to {month_label(window['last'])}, "
        "anti-social behaviour excluded. These are counts, not rates: there is no "
        "population or footfall denominator, so busy central areas stand out. They show "
        "where crimes were recorded, not where residents are at higher risk."
    )
    top = top_lsoas(ranking, borough)
    st.dataframe(
        top[["lsoa_name", "local_authority", "crime_count", "asb_count"]].rename(
            columns={
                "lsoa_name": "LSOA",
                "local_authority": "Borough",
                "crime_count": "Crimes",
                "asb_count": "ASB",
            }
        ),
        hide_index=True,
        width="stretch",
    )

    st.header("Data quality")
    st.write(
        "Every bronze row is classified exactly once; the pipeline fails if these counts "
        "do not add up."
    )
    reconciliation = tables["reconciliation"].assign(
        share=lambda df: df["share"].map(lambda v: "" if pd.isna(v) else f"{v:.2%}")
    )
    st.dataframe(
        reconciliation.rename(
            columns={"group": "Group", "item": "Item", "rows": "Rows", "share": "Share"}
        ),
        hide_index=True,
        width="stretch",
    )

    st.divider()
    st.caption(
        f"Snapshot of archive {snapshot['archive_month']} · silver run "
        f"`{snapshot['silver_run_id']}` · exported {snapshot['exported_at']} · "
        f"[Repository]({REPO_URL}) · [Data caveats]({CAVEATS_URL})"
    )


if __name__ == "__main__":
    main()
