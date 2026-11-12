"""
src/dashboard/ui/components.py
==============================
Reusable Streamlit UI visual components, scorecards, and analysis wrappers.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from config.settings import (
    DASHBOARD_SUBTITLE,
    DASHBOARD_TITLE,
    PREVIEW_ROWS_DEFAULT,
    PREVIEW_ROWS_MAX,
)
from src.dashboard.catalog import ANALYSIS_UI_MAP
from src.dashboard.insights import Insight
from src.dashboard.services import ActiveDatasetStore, AnalysisFrame

logger = logging.getLogger("dashboard.ui.components")


def render_header(store: ActiveDatasetStore):
    """Render top header banner, dataset metrics strip, and data quality banners."""
    st.title("🌦️ Weather & Climate Pattern Discovery (Spark SQL)")
    st.caption(f"{DASHBOARD_SUBTITLE} · Active session analytics")

    if store.prepared is None:
        return

    rep = store.prepared.cleaning_report
    raw_in = rep.summary.get("raw_rows_in", 0)
    clean_out = rep.summary.get("clean_rows_out", 0)
    interp_cnt = rep.summary.get("values_changed", {}).get("total_interpolated", 0)

    col1, col2, col3, col4, col5 = st.columns([3, 1.5, 1.5, 1.5, 2])
    with col1:
        st.markdown(f"**Dataset:** `{store.label}`")
    with col2:
        st.markdown(f"**Source:** `{store.source.upper()}`")
    with col3:
        st.markdown(f"**Raw In:** `{raw_in:,}`")
    with col4:
        st.markdown(f"**Clean Out:** `{clean_out:,}`")
    with col5:
        if interp_cnt > 0:
            st.markdown(f"**Interpolated:** `{interp_cnt:,}` values")
        else:
            st.markdown("**Interpolated:** `0`")

    # Incomplete years or interpolation caution banners
    if interp_cnt > 0:
        st.warning(
            f"ℹ️ **Data Quality Notice**: {interp_cnt:,} values were interpolated across short gaps "
            f"(<= 3 calendar days) during cleaning. All filled values are tracked by boolean flags."
        )

    st.divider()


def render_kpi_scorecard(kpi: dict[str, Any]):
    """Render the 10 executive headline KPI metric cards."""
    total_rec = kpi.get("total_records", 0)
    if total_rec == 0:
        st.warning("No records matched the selected filter criteria.")
        return

    # Row 1 (5 cards)
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.metric("Total Records", f"{kpi['total_records']:,}")
    with c2:
        loc_val = f"{kpi['locations']:,}" if kpi.get("locations") is not None else "n/a"
        st.metric("Locations", loc_val)
    with c3:
        avg_t = f"{kpi['avg_temp']:.1f} °C" if kpi.get("avg_temp") is not None else "n/a"
        st.metric("Average Temperature", avg_t)
    with c4:
        max_t = f"{kpi['max_temp']:.1f} °C" if kpi.get("max_temp") is not None else "n/a"
        cap_hot = (
            f"{kpi['max_temp_date']} at {kpi['max_temp_location']}"
            if kpi.get("max_temp_date")
            else None
        )
        st.metric("Maximum Temperature", max_t, help=cap_hot)
        if cap_hot:
            st.caption(cap_hot)
    with c5:
        min_t = f"{kpi['min_temp']:.1f} °C" if kpi.get("min_temp") is not None else "n/a"
        cap_cold = (
            f"{kpi['min_temp_date']} at {kpi['min_temp_location']}"
            if kpi.get("min_temp_date")
            else None
        )
        st.metric("Minimum Temperature", min_t, help=cap_cold)
        if cap_cold:
            st.caption(cap_cold)

    # Row 2 (5 cards)
    r1, r2, r3, r4, r5 = st.columns(5)
    with r1:
        rain_val = (
            f"{kpi['total_rainfall_mm']:,.1f} mm"
            if kpi.get("total_rainfall_mm") is not None
            else "n/a"
        )
        annual_val = (
            f"{kpi['avg_annual_rainfall_per_location']:,.1f}"
            if kpi.get("avg_annual_rainfall_per_location") is not None
            else "n/a"
        )
        st.metric("Total Rainfall", rain_val)
        st.caption(f"sum over selected city-days; avg per location/yr: {annual_val} mm")
    with r2:
        hum_val = f"{kpi['avg_humidity']:.1f} %" if kpi.get("avg_humidity") is not None else "n/a"
        st.metric("Average Humidity", hum_val)
    with r3:
        wind_val = (
            f"{kpi['avg_wind_speed']:.1f} km/h"
            if kpi.get("avg_wind_speed") is not None
            else "n/a"
        )
        st.metric("Average Wind Speed", wind_val)
    with r4:
        pres_val = (
            f"{kpi['avg_pressure']:.1f} hPa"
            if kpi.get("avg_pressure") is not None
            else "n/a"
        )
        st.metric("Average Pressure", pres_val)
    with r5:
        interp_cnt = kpi.get("interpolated_value_count", 0)
        st.metric("Interpolated Values", f"{interp_cnt:,}")
        st.caption("linear estimates <= 3 days")


def render_insights(insights: list[Insight]):
    """Render automated natural language climate insights with Evidence expanders."""
    if not insights:
        return

    st.subheader("💡 Key Findings & Climate Insights")
    st.caption("Deterministic analytical findings synthesized directly from Spark SQL query results.")

    for ins in insights:
        if ins.level == "caution":
            st.warning(f"⚠️ {ins.text}")
        else:
            st.info(f"🔹 {ins.text}")

        with st.expander(f"Evidence & Source Queries ({', '.join(ins.source_keys)})"):
            st.markdown(f"**Category:** `{ins.category.title()}` | **Sources:** `{', '.join(ins.source_keys)}`")
            st.json(ins.evidence)


def render_analysis(
    key: str,
    frame: AnalysisFrame,
    fig: go.Figure | None = None,
    extra_table: pd.DataFrame | None = None,
    help_note: str | None = None,
):
    """
    Standard visualization renderer for any of the 22 Spark SQL analyses:
    - Title & one-sentence description
    - Friendly warning if AnalysisFrame returned an error
    - Plotly chart and/or extra data table
    - 'Spark SQL behind this chart' expander
    - 'Data' expander with interactive DataFrame and CSV download button
    """
    ui_info = ANALYSIS_UI_MAP.get(key, {})
    title = ui_info.get("title", frame.title or key)
    desc = ui_info.get("description", "")

    st.markdown(f"#### {title}")
    if desc:
        st.caption(desc)
    if help_note:
        st.info(help_note)

    if frame.error:
        st.warning(f"Analysis unavailable: {frame.error}")
        return

    if fig is not None:
        st.plotly_chart(fig, use_container_width=True)

    if extra_table is not None and not extra_table.empty:
        st.markdown("**Detailed Results:**")
        st.dataframe(extra_table, use_container_width=True)
    elif fig is None and not frame.df.empty:
        st.dataframe(frame.df, use_container_width=True)

    # 1. Spark SQL Query Expander
    with st.expander("Spark SQL behind this chart"):
        st.markdown("**Spark SQL Query:**")
        st.code(frame.sql.strip(), language="sql")
        if frame.parameters:
            st.markdown("**Parameters:**")
            st.json(frame.parameters)

    # 2. Raw Result Data Expander with CSV Download
    if not frame.df.empty:
        with st.expander("Data"):
            st.dataframe(frame.df, use_container_width=True)
            csv_data = frame.df.to_csv(index=False)
            st.download_button(
                label=f"📥 Download {key}.csv",
                data=csv_data,
                file_name=f"{key}.csv",
                mime="text/csv",
                key=f"dl_csv_{key}",
            )
