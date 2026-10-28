"""Streamlit dashboard over the committed gold snapshot.

Reads only the files in dashboard/data/ (written by `make export`); it needs no Spark and
no data/ directory. Run it with `make dashboard`.
"""

import json
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

DATA_DIR = Path(__file__).resolve().parent / "data"
REPO_URL = "https://github.com/Daudsaid/uk-crime-pipeline"
CAVEATS_URL = f"{REPO_URL}/blob/main/docs/data-caveats.md"
ASB = "Anti-social behaviour"
TABLE_NAMES = (
    "mart_monthly_crimes",
    "mart_outcome_rates",
    "mart_lsoa_ranking",
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


# ------------------------------------------------------------------ data loading


def load_snapshot(data_dir: Path = DATA_DIR) -> dict:
    """Read snapshot.json (archive month, silver run id, export time, windows)."""
    return json.loads((data_dir / "snapshot.json").read_text())


def load_tables(data_dir: Path = DATA_DIR) -> dict[str, pd.DataFrame]:
    """Read the exported Parquet files; month columns become datetimes."""
    tables = {n: pd.read_parquet(data_dir / f"{n}.parquet") for n in TABLE_NAMES}
    for df in tables.values():
        for column in ("month", "first_month", "last_month"):
            if column in df.columns:
                df[column] = pd.to_datetime(df[column])
    return tables


def headline_figures(tables: dict[str, pd.DataFrame]) -> dict[str, float | int]:
    """Records, ASB share, charged-or-at-court share (mature months) and quarantined rows."""
    monthly = tables["mart_monthly_crimes"]
    records = int(monthly["crime_count"].sum())
    asb = int(monthly.loc[monthly["crime_type"] == ASB, "crime_count"].sum())
    outcomes = tables["mart_outcome_rates"]
    mature = int(outcomes["crime_count"].sum())
    charged = int(
        outcomes.loc[
            outcomes["outcome_group"] == "charged_or_court", "crime_count"
        ].sum()
    )
    reconciliation = tables["reconciliation"]
    quarantined = reconciliation.loc[
        (reconciliation["group"] == "reconciliation")
        & (reconciliation["item"] == "quarantined"),
        "rows",
    ]
    return {
        "records": records,
        "asb_share": asb / records if records else 0.0,
        "charge_rate": charged / mature if mature else 0.0,
        "quarantined": int(quarantined.iloc[0]) if len(quarantined) else 0,
    }


# --------------------------------------------------------------------- figures


def trend_figure(
    monthly: pd.DataFrame, forces: list[str], include_asb: bool
) -> go.Figure:
    """Monthly record counts by crime type for the selected forces."""
    data = monthly[monthly["force_name"].isin(forces)]
    if not include_asb:
        data = data[data["crime_type"] != ASB]
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


def hotspot_figure(ranking: pd.DataFrame, authorities: list[str]) -> go.Figure:
    """Map of London LSOAs sized and coloured by non-ASB crime count."""
    data = ranking[
        ranking["local_authority"].isin(authorities) & ranking["latitude"].notna()
    ]
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
        zoom=9,
        center={"lat": 51.51, "lon": -0.11},
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
    st.set_page_config(page_title="UK Crime Lakehouse", layout="wide")
    snapshot = st.cache_data(load_snapshot)()
    tables = st.cache_data(load_tables)()
    monthly = tables["mart_monthly_crimes"]
    outcome = snapshot["outcome"]
    window = snapshot["lsoa_window"]

    st.title("UK Crime Lakehouse")
    st.caption(
        "Metropolitan and City of London Police street-level records, "
        f"{snapshot['months']['first']} to {snapshot['months']['last']}. "
        "A snapshot of the gold layer; see the repository for how it is built."
    )

    figures = headline_figures(tables)
    columns = st.columns(4)
    columns[0].metric(
        "Records, 36 months",
        f"{figures['records']:,}",
        help="Street-level crime and anti-social behaviour records.",
    )
    columns[1].metric("Anti-social behaviour", f"{figures['asb_share']:.1%}")
    columns[2].metric(
        "Charged or at court",
        f"{figures['charge_rate']:.1%}",
        help=(
            "Share of non-ASB records in the outcome groups charged_or_court, using "
            f"mature months only ({month_label(outcome['first_month'])} to "
            f"{month_label(outcome['last_month'])})."
        ),
    )
    columns[3].metric(
        "Quarantined rows",
        f"{figures['quarantined']:,}",
        help="Rows set aside with a reason instead of being used or dropped.",
    )

    st.header("Records by month")
    forces = sorted(monthly["force_name"].unique())
    chosen_forces = st.multiselect("Force", forces, default=forces)
    include_asb = st.checkbox("Include anti-social behaviour", value=True)
    if chosen_forces:
        st.plotly_chart(
            trend_figure(monthly, chosen_forces, include_asb), width="stretch"
        )
    else:
        st.info("Select at least one force.")

    st.header("Outcomes by crime type")
    excluded = ", ".join(outcome["excluded_months"]) or "none"
    st.plotly_chart(outcome_figure(tables["mart_outcome_rates"]), width="stretch")
    st.caption(
        f"Period: {month_label(outcome['first_month'])} to {month_label(outcome['last_month'])} "
        f"({outcome['months_used']} months), excluding {excluded} (outlier). The newest "
        f"{outcome['maturity_months']} months are left out because their outcomes are not "
        "mature yet. Anti-social behaviour has no outcomes."
    )

    st.header("London hotspots, last 12 months")
    ranking = tables["mart_lsoa_ranking"]
    authorities = sorted(ranking["local_authority"].unique())
    chosen = st.multiselect("Borough", authorities, default=authorities)
    if chosen:
        st.plotly_chart(hotspot_figure(ranking, chosen), width="stretch")
    st.caption(
        f"Crimes per LSOA, {month_label(window['first'])} to {month_label(window['last'])}, "
        "anti-social behaviour excluded. These are counts, not rates: there is no "
        "population or footfall denominator, so busy central areas stand out. They show "
        "where crimes were recorded, not where residents are at higher risk."
    )
    top = chosen and ranking[ranking["local_authority"].isin(chosen)].nlargest(
        10, "crime_count"
    )
    if top is not None and len(top):
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
