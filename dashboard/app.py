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
    "dq_volume_anomalies",
    "reconciliation",
)
# The month the Metropolitan Police's recording changed (see docs/data-caveats.md). Drawn on
# the trend for every selection except City of London, which is a different force.
MET_CHANGE_MONTH = pd.Timestamp("2026-05-01")
MET_CHANGE_LABEL = "Met recording change"
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
# Greater London's bounding box (lat/lon degrees): the All London map fits this, not the data.
GREATER_LONDON = {"lat": (51.286, 51.692), "lon": (-0.510, 0.334)}
CHARGED = "charged_or_court"
TOP_TYPES = 4
TOTAL_LABEL = "Total records"
TOTAL_COLOUR = "#111111"
# Qualitative palette, one colour per crime type. Dark24 minus its near-black entry (kept
# for the total line); 23 distinct colours, enough for the 14 crime types including ASB.
PALETTE = [c for c in px.colors.qualitative.Dark24 if c.lower() != "#222a2a"]
QUALITY_GROUPS = {
    "reconciliation": "Where every bronze row went",
    "quarantine": "Quarantined rows, by reason",
    "silver_flags": "Flags on kept rows",
    "gold": "Gold layer checks",
    "volume_anomalies": "Volume check",
}
QUALITY_ITEMS = {
    "bronze_rows": "Bronze rows",
    "silver": "Kept in silver",
    "quarantined": "Quarantined",
    "deduped": "Identical copies removed",
    "superseded": "Superseded copies",
    "archive_replaced": "Replaced by a newer archive",
    "conflicting_duplicate_crime_id": "Conflicting duplicate Crime IDs",
    "asb_rows": "Anti-social behaviour rows",
    "no_location_rows": "Rows without location",
    "attribute_conflict_rows": "Rows with conflicting attributes",
    "fct_crimes_rows": "Rows in the fact table",
    "orphan_fact_keys": "Fact rows missing a dimension key",
    "flagged": "Months flagged",
    "unacknowledged": "Flags not yet acknowledged",
}


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


# ------------------------------------------------------------------ formatting


def fmt_int(value: float) -> str:
    """A whole number with thousands separators: 1234567 -> '1,234,567'."""
    return f"{round(value):,}"


def fmt_pct(fraction: float) -> str:
    """A share (0 to 1) as a percentage to one decimal place; tiny non-zero shares as <0.1%."""
    if pd.isna(fraction):
        return ""
    if 0 < fraction < 0.0005:
        return "<0.1%"
    return f"{fraction:.1%}"


def fmt_change(percent: float) -> str:
    """A change already in percent, signed, one decimal: -94.88 -> '-94.9%'."""
    return f"{percent:+,.1f}%"


def styled(df: pd.DataFrame, formats: dict) -> "pd.io.formats.style.Styler":
    """df with display formats per column; the values stay numeric, so sorting still works."""
    return df.style.format(formats, na_rep="")


def crime_type_colours(monthly: pd.DataFrame) -> dict[str, str]:
    """One colour per crime type, fixed by the sorted list of all types in the data.

    Computed from the whole table (not the selection), so a crime type has the same colour
    in every chart and for every borough. Raises if there are more types than colours.
    """
    types = sorted(monthly["crime_type"].unique())
    if len(types) > len(PALETTE):
        raise ValueError(f"{len(types)} crime types but only {len(PALETTE)} colours")
    return dict(zip(types, PALETTE, strict=False))


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
) -> str | None:
    """One line placing a borough against London's recorded crime (None for All London)."""
    if borough == ALL_LONDON:
        return None
    recent = in_window(monthly, bounds)
    london = int(recent.loc[~recent["is_asb"], "crime_count"].sum())
    own = int(
        for_borough(recent, borough).loc[lambda d: ~d["is_asb"], "crime_count"].sum()
    )
    share = own / london if london else 0.0
    return (
        f"{borough} accounts for {fmt_pct(share)} of London's recorded crime in the last "
        f"12 months ({fmt_int(own)} of {fmt_int(london)} crimes)."
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


def top_types(
    monthly: pd.DataFrame,
    bounds: tuple[pd.Timestamp, pd.Timestamp],
    borough: str,
    include_asb: bool,
) -> list[str]:
    """The selection's crime types, most records first over the last 12 months."""
    recent = for_borough(in_window(monthly, bounds), borough)
    if not include_asb:
        recent = recent[~recent["is_asb"]]
    totals = recent.groupby("crime_type")["crime_count"].sum()
    return totals.sort_values(ascending=False, kind="stable").index.tolist()


def trend_figure(
    monthly: pd.DataFrame,
    borough: str,
    include_asb: bool,
    crime_types: list[str],
    colours: dict[str, str],
) -> go.Figure:
    """Monthly records for the selection: a total line plus one line per chosen crime type."""
    data = for_borough(monthly, borough)
    if not include_asb:
        data = data[~data["is_asb"]]
    by_type = data.groupby(["month", "crime_type"], as_index=False)["crime_count"].sum()
    total = data.groupby("month", as_index=False)["crime_count"].sum()

    figure = go.Figure()
    figure.add_scatter(
        x=total["month"], y=total["crime_count"], name=TOTAL_LABEL, mode="lines",
        line={"color": TOTAL_COLOUR, "width": 3},
    )  # fmt: skip
    for crime_type in crime_types:
        own = by_type[by_type["crime_type"] == crime_type]
        figure.add_scatter(
            x=own["month"], y=own["crime_count"], name=crime_type, mode="lines",
            line={"color": colours[crime_type], "width": 2},
        )  # fmt: skip
    figure.update_traces(hovertemplate="%{y:,}")
    figure.update_xaxes(title_text="Month")
    figure.update_yaxes(title_text="Records", tickformat=",", rangemode="tozero")
    figure.update_layout(legend_title_text="", margin={"t": 30}, hovermode="x unified")
    months = data["month"]
    if borough != "City of London" and months.min() <= MET_CHANGE_MONTH <= months.max():
        figure.add_shape(
            type="line", xref="x", yref="paper", x0=MET_CHANGE_MONTH,
            x1=MET_CHANGE_MONTH, y0=0, y1=1,
            line={"color": "#6b7280", "width": 1, "dash": "dash"},
        )  # fmt: skip
        figure.add_annotation(
            x=MET_CHANGE_MONTH, y=1, xref="x", yref="paper", text=MET_CHANGE_LABEL,
            showarrow=False, yanchor="bottom", font={"size": 11, "color": "#6b7280"},
        )  # fmt: skip
    return figure


def quality_tables(reconciliation: pd.DataFrame) -> list[tuple[str, pd.DataFrame]]:
    """The reconciliation table as (plain-English heading, table) pairs, one per group.

    Item labels come from QUALITY_ITEMS; a group or item that is not listed falls back to
    its name with underscores turned into spaces, so a new row never breaks the page.
    """

    def plain(name: str) -> str:
        return name.replace("_", " ").capitalize()

    groups = [g for g in QUALITY_GROUPS if g in set(reconciliation["group"])]
    groups += sorted(set(reconciliation["group"]) - set(QUALITY_GROUPS))
    tables = []
    for group in groups:
        rows = reconciliation[reconciliation["group"] == group]
        tables.append(
            (
                QUALITY_GROUPS.get(group, plain(group)),
                pd.DataFrame(
                    {
                        "Item": rows["item"].map(
                            lambda i: QUALITY_ITEMS.get(i, plain(i))
                        ),
                        "Rows": rows["rows"],
                        "Share": rows["share"],
                    }
                ),
            )
        )
    return tables


def anomalies_table(anomalies: pd.DataFrame) -> pd.DataFrame:
    """The flagged volume anomalies, newest month first (Change is already formatted)."""
    shown = anomalies.sort_values(["month", "force", "crime_type"], ascending=False)
    return pd.DataFrame(
        {
            "Force": shown["force"],
            "Crime type": shown["crime_type"],
            "Month": shown["month"].dt.strftime("%Y-%m"),
            "Count": shown["crime_count"],
            "Baseline median": shown["baseline_median"].round(0).astype(int),
            "Change": shown["pct_change"].map(fmt_change),
            "Status": shown["is_acknowledged"].map(
                lambda ok: "Acknowledged" if ok else "Not acknowledged"
            ),
            "Note": shown["note"].fillna(""),
        }
    )


def top_crime_types_figure(
    monthly: pd.DataFrame,
    bounds: tuple[pd.Timestamp, pd.Timestamp],
    borough: str,
    colours: dict[str, str],
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
        color="crime_type",
        color_discrete_map=colours,
        orientation="h",
        labels={"crime_count": "Crimes", "crime_type": ""},
    )
    figure.update_traces(hovertemplate="%{y}: %{x:,}<extra></extra>")
    figure.update_xaxes(tickformat=",")
    figure.update_layout(
        margin={"t": 10},
        showlegend=False,
        yaxis={"categoryorder": "total ascending"},
    )
    return figure


def outcome_order(outcomes: pd.DataFrame) -> list[str]:
    """Crime types by charged-or-at-court share, highest first (ties by name)."""
    charged = (
        outcomes[outcomes["outcome_group"] == CHARGED]
        .set_index("crime_type")["share_of_crime_type"]
        .reindex(sorted(outcomes["crime_type"].unique()))
        .fillna(0.0)
    )
    return charged.sort_values(ascending=False, kind="stable").index.tolist()


def outcome_figure(outcomes: pd.DataFrame) -> go.Figure:
    """Stacked horizontal bars: share of each outcome group within a crime type.

    Crime types run from the highest charged-or-at-court share at the top to the lowest.
    (px reverses category_orders for horizontal bars, so listing them highest first puts
    the highest at the top.)
    """
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
        category_orders={
            "crime_type": outcome_order(outcomes),
            "outcome": list(OUTCOME_LABELS.values()),
        },
        color_discrete_map={OUTCOME_LABELS[k]: v for k, v in OUTCOME_COLOURS.items()},
        hover_data={"crime_count": ":,", "percent": ":.1f"},
        labels={"percent": "Share of crimes (%)", "crime_type": "", "outcome": ""},
    )
    figure.update_layout(barmode="stack", margin={"t": 10}, legend_title_text="")
    return figure


def fit_view(
    lat_range: tuple[float, float],
    lon_range: tuple[float, float],
    padding: float = 0.3,
    min_zoom: float = 8.0,
) -> tuple[dict[str, float], float]:
    """Map centre and zoom that fit a lat/lon box (centre of the box).

    Uses Web Mercator scale (512-pixel tiles) for a map about 1,000 by 560 pixels; the zoom
    is reduced by `padding` and kept between min_zoom and 15.
    """
    lat_min, lat_max = lat_range
    lon_min, lon_max = lon_range
    centre_lat = (lat_min + lat_max) / 2
    lat_span = max(lat_max - lat_min, 0.005)
    lon_span = max(lon_max - lon_min, 0.005)
    by_height = math.log2(
        560 * 360 * math.cos(math.radians(centre_lat)) / (512 * lat_span)
    )
    by_width = math.log2(1000 * 360 / (512 * lon_span))
    zoom = min(by_height, by_width) - padding
    return {"lat": centre_lat, "lon": (lon_min + lon_max) / 2}, max(
        min_zoom, min(15.0, zoom)
    )


def map_view(points: pd.DataFrame) -> tuple[dict[str, float], float]:
    """Map centre and zoom that fit the points (their bounding box, a little padding)."""
    if points.empty:
        return dict(LONDON_CENTRE), 9.0
    return fit_view(
        (points["latitude"].min(), points["latitude"].max()),
        (points["longitude"].min(), points["longitude"].max()),
    )


def london_view() -> tuple[dict[str, float], float]:
    """Map centre and zoom that fill the frame with Greater London (tight padding)."""
    return fit_view(GREATER_LONDON["lat"], GREATER_LONDON["lon"], padding=0.05)


def hotspot_figure(ranking: pd.DataFrame, borough: str) -> go.Figure:
    """Map of the selection's LSOAs sized and coloured by non-ASB crime count, zoomed to fit."""
    data = for_borough(ranking, borough)
    data = data[data["latitude"].notna()]
    centre, zoom = london_view() if borough == ALL_LONDON else map_view(data)
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
    colours = crime_type_colours(monthly)

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
        fmt_int(figures["crimes"]),
        help=(
            f"Records in London LSOAs, {month_label(window['first'])} to "
            f"{month_label(window['last'])}, excluding anti-social behaviour."
        ),
    )
    columns[1].metric(
        "Anti-social behaviour",
        fmt_pct(figures["asb_share"]),
        help=(
            "Share of all records in the same 12 months "
            f"({fmt_int(figures['asb'])} records)."
        ),
    )
    columns[2].metric(
        "Charged or at court",
        fmt_pct(figures["charge_rate"]),
        help=(
            "Share of non-ASB records in the outcome group charged_or_court, using mature "
            f"months only ({month_label(outcome['first_month'])} to "
            f"{month_label(outcome['last_month'])})."
        ),
    )
    comparison = london_comparison(monthly, bounds, borough)
    if comparison:
        st.markdown(comparison)

    st.header("Records by month")
    include_asb = st.checkbox("Include anti-social behaviour", value=True)
    ranked = top_types(monthly, bounds, borough, include_asb)
    chosen = st.multiselect(
        "Crime types to show (the four biggest in the last 12 months by default)",
        ranked,
        default=ranked[:TOP_TYPES],
        key=f"trend-types-{borough}-{include_asb}",  # a new selection resets the preset
    )
    st.plotly_chart(
        trend_figure(monthly, borough, include_asb, chosen, colours), width="stretch"
    )

    st.header("Top crime types, last 12 months")
    st.plotly_chart(
        top_crime_types_figure(monthly, bounds, borough, colours), width="stretch"
    )

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
        styled(
            top[["lsoa_name", "local_authority", "crime_count", "asb_count"]].rename(
                columns={
                    "lsoa_name": "LSOA",
                    "local_authority": "Borough",
                    "crime_count": "Crimes",
                    "asb_count": "ASB",
                }
            ),
            {"Crimes": fmt_int, "ASB": fmt_int},
        ),
        hide_index=True,
        width="stretch",
    )

    st.header("Data quality")
    st.write(
        "Every bronze row is classified exactly once; the pipeline fails if these counts "
        "do not add up."
    )
    for heading, table in quality_tables(tables["reconciliation"]):
        st.markdown(f"**{heading}**")
        st.dataframe(
            styled(table, {"Rows": fmt_int, "Share": fmt_pct}),
            hide_index=True,
            width="stretch",
        )

    st.subheader("Volume anomalies")
    rule = snapshot.get("volume_anomaly", {"threshold_pct": 50, "min_baseline": 500})
    st.write(
        "Months where a crime type's count differs from the median of the previous three "
        f"months by more than {rule['threshold_pct']:g}%, for baselines of at least "
        f"{rule['min_baseline']} records. A flag is information, not an error; "
        "acknowledged ones have a known source change."
    )
    anomalies = tables["dq_volume_anomalies"]
    if anomalies.empty:
        st.write("No volume anomalies flagged.")
    else:
        st.dataframe(
            styled(
                anomalies_table(anomalies),
                {"Count": fmt_int, "Baseline median": fmt_int},
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
