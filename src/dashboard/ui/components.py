"""
src/dashboard/ui/components.py
==============================
Reusable Streamlit UI visual components, warm-theme scorecards, and analysis wrappers.
Implements the Phase 11 warm light design reference.
"""

from __future__ import annotations

import html
import logging
from typing import Any

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from config.settings import (
    COLORS,
    DASHBOARD_SUBTITLE,
    DASHBOARD_TITLE,
    FONT_BODY,
    FONT_HEADING,
    FONT_MONO,
)
from src.dashboard.catalog import ANALYSIS_UI_MAP
from src.dashboard.insights import Insight
from src.dashboard.services import ActiveDatasetStore, AnalysisFrame

logger = logging.getLogger("dashboard.ui.components")

# ── 10 Inline SVGs (24x24 viewBox, stroke-based, no external fonts/links) ──────
ICONS: dict[str, str] = {
    "dataset": (
        '<svg viewBox="0 0 24 24" width="18" height="18" stroke="currentColor" fill="none" '
        'stroke-width="2" stroke-linecap="round" stroke-linejoin="round">'
        '<ellipse cx="12" cy="5" rx="9" ry="3"></ellipse>'
        '<path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"></path>'
        '<path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"></path>'
        '</svg>'
    ),
    "location": (
        '<svg viewBox="0 0 24 24" width="18" height="18" stroke="currentColor" fill="none" '
        'stroke-width="2" stroke-linecap="round" stroke-linejoin="round">'
        '<path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0 1 18 0z"></path>'
        '<circle cx="12" cy="10" r="3"></circle>'
        '</svg>'
    ),
    "thermometer": (
        '<svg viewBox="0 0 24 24" width="18" height="18" stroke="currentColor" fill="none" '
        'stroke-width="2" stroke-linecap="round" stroke-linejoin="round">'
        '<path d="M14 14.76V3.5a2.5 2.5 0 0 0-5 0v11.26a4.5 4.5 0 1 0 5 0z"></path>'
        '</svg>'
    ),
    "flame": (
        '<svg viewBox="0 0 24 24" width="18" height="18" stroke="currentColor" fill="none" '
        'stroke-width="2" stroke-linecap="round" stroke-linejoin="round">'
        '<path d="M8.5 14.5A2.5 2.5 0 0 0 11 12c0-1.38-.5-2-1-3-1.072-2.143-.224-4.054 2-6 '
        '.5 2.5 2 4.9 4 6.5 2 1.6 3 3.5 3 5.5a7 7 0 1 1-14 0c0-1.153.433-2.294 1-3a2.5 2.5 0 0 0 2.5 2.5z"></path>'
        '</svg>'
    ),
    "snowflake": (
        '<svg viewBox="0 0 24 24" width="18" height="18" stroke="currentColor" fill="none" '
        'stroke-width="2" stroke-linecap="round" stroke-linejoin="round">'
        '<line x1="12" y1="2" x2="12" y2="22"></line>'
        '<line x1="12" y1="9" x2="8" y2="6"></line>'
        '<line x1="12" y1="9" x2="16" y2="6"></line>'
        '<line x1="12" y1="15" x2="8" y2="18"></line>'
        '<line x1="12" y1="15" x2="16" y2="18"></line>'
        '<line x1="3.3" y1="7" x2="20.7" y2="17"></line>'
        '<line x1="3.3" y1="17" x2="20.7" y2="7"></line>'
        '</svg>'
    ),
    "rain": (
        '<svg viewBox="0 0 24 24" width="18" height="18" stroke="currentColor" fill="none" '
        'stroke-width="2" stroke-linecap="round" stroke-linejoin="round">'
        '<path d="M12 2.69l5.66 5.66a8 8 0 1 1-11.31 0z"></path>'
        '</svg>'
    ),
    "humidity": (
        '<svg viewBox="0 0 24 24" width="18" height="18" stroke="currentColor" fill="none" '
        'stroke-width="2" stroke-linecap="round" stroke-linejoin="round">'
        '<path d="M12 2.69l5.66 5.66a8 8 0 1 1-11.31 0z"></path>'
        '<path d="M8 14a4 4 0 0 0 8 0" stroke-width="1.5"></path>'
        '</svg>'
    ),
    "wind": (
        '<svg viewBox="0 0 24 24" width="18" height="18" stroke="currentColor" fill="none" '
        'stroke-width="2" stroke-linecap="round" stroke-linejoin="round">'
        '<path d="M9.59 4.59A2 2 0 1 1 11 8H2m10.59 11.41A2 2 0 1 0 14 16H2m15.73-8.27A2.5 2.5 0 1 1 19.5 12H2"></path>'
        '</svg>'
    ),
    "gauge": (
        '<svg viewBox="0 0 24 24" width="18" height="18" stroke="currentColor" fill="none" '
        'stroke-width="2" stroke-linecap="round" stroke-linejoin="round">'
        '<circle cx="12" cy="12" r="10"></circle>'
        '<path d="M12 12l3-3"></path>'
        '<path d="M8 12a4 4 0 0 1 8 0"></path>'
        '</svg>'
    ),
    "check": (
        '<svg viewBox="0 0 24 24" width="18" height="18" stroke="currentColor" fill="none" '
        'stroke-width="2" stroke-linecap="round" stroke-linejoin="round">'
        '<path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"></path>'
        '<polyline points="22 4 12 14.01 9 11.01"></polyline>'
        '</svg>'
    ),
}

ACCENT_STYLES: dict[str, dict[str, str]] = {
    "neutral": {
        "icon_bg": "#F2ECE1",
        "icon_color": "#7A6E65",
        "unit_color": "#7A6E65",
        "dot_color": "#827661",
        "tint_class": "",
    },
    "green": {
        "icon_bg": "#E2F0EB",
        "icon_color": "#2D7A60",
        "unit_color": "#2D7A60",
        "dot_color": "#2D7A60",
        "tint_class": "",
    },
    "terracotta": {
        "icon_bg": "#FDE8E1",
        "icon_color": "#E05326",
        "unit_color": "#E05326",
        "dot_color": "#E05326",
        "tint_class": "tint-heat",
    },
    "slate-blue": {
        "icon_bg": "#E9EFF9",
        "icon_color": "#3B5998",
        "unit_color": "#3B5998",
        "dot_color": "#3B5998",
        "tint_class": "tint-cold",
    },
    "slate": {
        "icon_bg": "#EAEFF5",
        "icon_color": "#5A6B82",
        "unit_color": "#5A6B82",
        "dot_color": "#5A6B82",
        "tint_class": "",
    },
    "amber": {
        "icon_bg": "#FEF9EE",
        "icon_color": "#D97706",
        "unit_color": "#D97706",
        "dot_color": "#D97706",
        "tint_class": "",
    },
}


def build_kpi_card_html(
    label: str,
    value_text: str | None,
    unit: str = "",
    caption: str | None = None,
    accent: str = "neutral",
    icon: str = "dataset",
) -> str:
    """
    Generate clean, safe HTML for one KPI card adhering to the warm design reference.
    Dynamic values are sanitized with html.escape.
    """
    safe_label = html.escape(label)
    safe_val = html.escape(str(value_text)) if value_text is not None else "n/a"
    safe_unit = html.escape(unit)
    safe_caption = html.escape(caption) if caption else ""

    style = ACCENT_STYLES.get(accent, ACCENT_STYLES["neutral"])
    svg_icon = ICONS.get(icon, ICONS["dataset"])
    tint_class = style["tint_class"]

    unit_span = (
        f'<span class="wcp-kpi-unit" style="color: {style["unit_color"]};">{safe_unit}</span>'
        if safe_unit
        else ""
    )
    caption_div = (
        f'<div class="wcp-kpi-caption">'
        f'<span class="wcp-caption-dot" style="background-color: {style["dot_color"]};"></span>'
        f'<span>{safe_caption}</span>'
        f'</div>'
        if safe_caption
        else '<div class="wcp-kpi-caption">&nbsp;</div>'
    )

    return f"""
<div class="wcp-kpi-card {tint_class} wcp-accent-{accent}">
    <div class="wcp-kpi-top">
        <div class="wcp-kpi-label">{safe_label}</div>
        <div class="wcp-kpi-icon-tile" style="background-color: {style['icon_bg']}; color: {style['icon_color']};">
            {svg_icon}
        </div>
    </div>
    <div class="wcp-kpi-val-row">
        <span class="wcp-kpi-val">{safe_val}</span>
        {unit_span}
    </div>
    {caption_div}
</div>
"""


def kpi_card(
    label: str,
    value_text: str | None,
    unit: str = "",
    caption: str | None = None,
    accent: str = "neutral",
    icon: str = "dataset",
):
    """Render a single styled KPI card to the Streamlit page."""
    card_html = build_kpi_card_html(
        label=label,
        value_text=value_text,
        unit=unit,
        caption=caption,
        accent=accent,
        icon=icon,
    )
    st.markdown(card_html, unsafe_allow_html=True)


def render_header(store: ActiveDatasetStore):
    """Render styled header banner, badge, and dataset status strip."""
    st.markdown(
        f"""
        <div class="wcp-header-block">
            <div class="wcp-header-title-row">
                <span class="wcp-header-title">{html.escape(DASHBOARD_TITLE)}</span>
                <span class="wcp-badge">{html.escape(DASHBOARD_SUBTITLE)}</span>
            </div>
            <p class="wcp-header-subtitle">Daily weather analysis built on Spark SQL</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if store.prepared is None:
        return

    rep = store.prepared.cleaning_report
    raw_in = rep.summary.get("raw_rows_in", 0)
    clean_out = rep.summary.get("clean_rows_out", 0)
    interp_cnt = rep.summary.get("values_changed", {}).get("total_interpolated", 0)

    interp_style = 'style="color: #D97706; font-weight: 700;"' if interp_cnt > 0 else ""

    st.markdown(
        f"""
        <div class="wcp-status-strip">
            <div class="wcp-status-item">
                <span class="wcp-status-dot"></span>
                <span>Dataset:</span>
                <span class="wcp-status-val">{html.escape(store.label)}</span>
            </div>
            <div class="wcp-status-item">
                <span>Source:</span>
                <span class="wcp-status-val">{html.escape(store.source.upper())}</span>
            </div>
            <div class="wcp-status-item">
                <span>Raw In:</span>
                <span class="wcp-status-val">{raw_in:,}</span>
            </div>
            <div class="wcp-status-item">
                <span>Clean Out:</span>
                <span class="wcp-status-val">{clean_out:,}</span>
            </div>
            <div class="wcp-status-item" {interp_style}>
                <span>Interpolated:</span>
                <span class="wcp-status-val">{interp_cnt:,} values</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if interp_cnt > 0:
        st.warning(
            f"ℹ️ **Data Quality Notice**: {interp_cnt:,} values were interpolated across short gaps "
            f"(<= 3 calendar days) during cleaning. All filled values are tracked by boolean flags."
        )


def render_kpi_scorecard(kpi: dict[str, Any]):
    """Render the 10 executive headline KPI metric cards."""
    total_rec = kpi.get("total_records", 0)
    if total_rec == 0:
        st.warning("No records matched the selected filter criteria.")
        return

    # Row 1 (5 cards)
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        kpi_card(
            label="Total Records",
            value_text=f"{kpi['total_records']:,}",
            caption="quality-checked rows",
            accent="neutral",
            icon="dataset",
        )
    with c2:
        loc_val = f"{kpi['locations']:,}" if kpi.get("locations") is not None else "n/a"
        kpi_card(
            label="Locations",
            value_text=loc_val,
            caption="monitoring stations",
            accent="green",
            icon="location",
        )
    with c3:
        avg_t = f"{kpi['avg_temp']:.1f}" if kpi.get("avg_temp") is not None else "n/a"
        kpi_card(
            label="Average Temperature",
            value_text=avg_t,
            unit="°C",
            caption="all-station climatological mean",
            accent="terracotta",
            icon="thermometer",
        )
    with c4:
        max_t = f"{kpi['max_temp']:.1f}" if kpi.get("max_temp") is not None else "n/a"
        cap_hot = (
            f"{kpi['max_temp_date']} at {kpi['max_temp_location']}"
            if kpi.get("max_temp_date")
            else "peak record"
        )
        kpi_card(
            label="Maximum Temperature",
            value_text=max_t,
            unit="°C",
            caption=cap_hot,
            accent="terracotta",
            icon="flame",
        )
    with c5:
        min_t = f"{kpi['min_temp']:.1f}" if kpi.get("min_temp") is not None else "n/a"
        cap_cold = (
            f"{kpi['min_temp_date']} at {kpi['min_temp_location']}"
            if kpi.get("min_temp_date")
            else "trough record"
        )
        kpi_card(
            label="Minimum Temperature",
            value_text=min_t,
            unit="°C",
            caption=cap_cold,
            accent="slate-blue",
            icon="snowflake",
        )

    # Row 2 (5 cards)
    r1, r2, r3, r4, r5 = st.columns(5)
    with r1:
        rain_val = (
            f"{kpi['total_rainfall_mm']:,.1f}"
            if kpi.get("total_rainfall_mm") is not None
            else "n/a"
        )
        annual_val = (
            f"{kpi['avg_annual_rainfall_per_location']:,.1f}"
            if kpi.get("avg_annual_rainfall_per_location") is not None
            else "n/a"
        )
        kpi_card(
            label="Total Rainfall",
            value_text=rain_val,
            unit="mm",
            caption=f"sum over selected city-days; avg per location/yr: {annual_val} mm",
            accent="green",
            icon="rain",
        )
    with r2:
        hum_val = f"{kpi['avg_humidity']:.1f}" if kpi.get("avg_humidity") is not None else "n/a"
        kpi_card(
            label="Average Humidity",
            value_text=hum_val,
            unit="%",
            caption="mean relative humidity",
            accent="green",
            icon="humidity",
        )
    with r3:
        wind_val = (
            f"{kpi['avg_wind_speed']:.1f}"
            if kpi.get("avg_wind_speed") is not None
            else "n/a"
        )
        kpi_card(
            label="Average Wind Speed",
            value_text=wind_val,
            unit="km/h",
            caption="mean near-surface speed",
            accent="slate",
            icon="wind",
        )
    with r4:
        pres_val = (
            f"{kpi['avg_pressure']:.1f}"
            if kpi.get("avg_pressure") is not None
            else "n/a"
        )
        kpi_card(
            label="Average Pressure",
            value_text=pres_val,
            unit="hPa",
            caption="barometric sea-level pressure",
            accent="neutral",
            icon="gauge",
        )
    with r5:
        interp_cnt = kpi.get("interpolated_value_count", 0)
        interp_accent = "green" if interp_cnt == 0 else "amber"
        kpi_card(
            label="Interpolated Values",
            value_text=f"{interp_cnt:,}",
            caption="linear estimates <= 3 days",
            accent=interp_accent,
            icon="check",
        )


def render_insights(insights: list[Insight]):
    """Render automated natural language climate insights with category tags and Evidence expanders."""
    if not insights:
        return

    st.markdown(
        f"""
        <div class="wcp-card" style="margin-top: 14px; margin-bottom: 12px; padding: 14px 18px;">
            <div style="display: flex; align-items: center; justify-content: space-between;">
                <div style="display: flex; align-items: center; gap: 10px;">
                    <h3 style="margin: 0; font-size: 1.15rem;">Key findings</h3>
                    <span class="wcp-badge" style="background-color: #1F1815; color: #FFFFFF;">{len(insights)} insights</span>
                </div>
                <span style="font-size: 0.8rem; color: #7A6E65;">Derived from Spark SQL results</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    for ins in insights:
        is_caution = ins.level == "caution"
        caution_class = "wcp-caution" if is_caution else ""
        icon_prefix = "⚠️ " if is_caution else ""
        cat_key = ins.category.lower().replace(" ", "_")
        escaped_text = html.escape(ins.text)

        st.markdown(
            f"""
            <div class="wcp-finding-row {caution_class}">
                <div style="display: flex; align-items: flex-start; gap: 8px;">
                    <span class="wcp-category-tag wcp-tag-{cat_key}">{html.escape(ins.category.upper())}</span>
                    <span style="font-size: 0.92rem; line-height: 1.4; color: #1F1815;">{icon_prefix}{escaped_text}</span>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

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
    - Card container
    - Title & one-sentence description
    - Plotly chart and/or extra data table
    - 'Spark SQL behind this chart' expander (espresso code block + real execution time)
    - 'Data' expander with interactive DataFrame and CSV download button
    """
    ui_info = ANALYSIS_UI_MAP.get(key, {})
    title = ui_info.get("title", frame.title or key)
    desc = ui_info.get("description", "")

    st.markdown(f"#### {html.escape(title)}")
    if desc:
        st.markdown(
            f"<p style='color: #7A6E65; font-size: 0.88rem; margin-top: -6px;'>{html.escape(desc)}</p>",
            unsafe_allow_html=True,
        )
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

    col_sql, col_data = st.columns(2)
    with col_sql:
        time_s = getattr(frame, "execution_time_s", None)
        time_str = f" ({time_s:.3f} s)" if (isinstance(time_s, (int, float)) and time_s > 0) else ""
        with st.expander(f"Spark SQL behind this chart{time_str}"):
            if isinstance(time_s, (int, float)) and time_s > 0:
                st.caption(f"⏱️ Spark SQL execution time: `{time_s:.3f} s`")
            st.code(frame.sql.strip(), language="sql")
            if frame.parameters:
                st.markdown("**Parameters:**")
                st.json(frame.parameters)

    with col_data:
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
