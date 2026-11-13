"""
src/dashboard/ui/sections.py
============================
Renderers for each of the 8 dashboard sections.
Streamlit UI layer only.
"""

from __future__ import annotations

import io
import logging
from typing import Any

import pandas as pd
import streamlit as st
from pyspark.sql import SparkSession

from config.settings import (
    PREVIEW_ROWS_DEFAULT,
    PREVIEW_ROWS_MAX,
    TOP_N_DEFAULT,
)
from src.dashboard.catalog import ANALYSIS_UI_MAP
from src.dashboard.charts import (
    build_climate_city_profile,
    build_climate_year_comparison,
    build_correlations,
    build_empty_figure,
    build_extreme_events_yearly,
    build_heatwave_spells,
    build_rain_heavy_days,
    build_rain_location_ranking,
    build_rain_monthly,
    build_rain_monthly_climatology,
    build_rain_seasonal,
    build_rain_unusual_months,
    build_rain_yearly,
    build_temp_location_ranking,
    build_temp_monthly_climatology,
    build_temp_monthly_series,
    build_temp_seasonal,
    build_temp_trend,
    build_temp_yearly,
)
from src.dashboard.services import (
    ActiveDatasetStore,
    AnalysisFrame,
    get_analysis_frame,
    get_insights,
    get_kpis,
    get_preview,
    summarize_cleaning,
    summarize_validation,
)
from src.dashboard.ui.components import (
    render_analysis,
    render_insights,
    render_kpi_scorecard,
)
from src.queries.base import Filters
from src.queries.registry import ANALYSES, get_analysis

logger = logging.getLogger("dashboard.ui.sections")


# =============================================================================
# Helper: Cached Analysis Query Fetching
# =============================================================================

def _filters_repr(f: Filters | None) -> str:
    """Deterministic string representation of filters for caching keys."""
    if f is None:
        return "none"
    locs = sorted(f.locations) if f.locations is not None else "all"
    yrs = sorted(f.years) if f.years is not None else "all"
    sns = sorted(f.seasons) if f.seasons is not None else "all"
    return f"{locs}|{f.start_date}|{f.end_date}|{yrs}|{sns}"


def _cached_analysis_frame(
    version: int,
    filters_str: str,
    key: str,
    params_tuple: tuple | None,
    _spark: SparkSession,
    view: str,
    filters_obj: Filters | None,
) -> AnalysisFrame:
    """Retrieve AnalysisFrame backed by thread-safe ResultCache singleton."""
    params_dict = dict(params_tuple) if params_tuple else None
    return get_analysis_frame(_spark, key=key, filters=filters_obj, params=params_dict, view=view)


def _cached_insights(
    version: int,
    filters_str: str,
    _spark: SparkSession,
    view: str,
    filters_obj: Filters | None,
) -> list:
    """Retrieve natural language climate insights backed by ResultCache."""
    return get_insights(_spark, filters=filters_obj, view=view)


# =============================================================================
# 1. Section: Overview
# =============================================================================

def render_overview_section(spark: SparkSession, store: ActiveDatasetStore, filters: Filters):
    """Render executive KPI cards, automated insights, headline charts, and SQL expander."""
    clean_view = store.prepared.clean_view  # type: ignore
    f_str = _filters_repr(filters)

    with st.spinner("Computing headline KPIs via Spark SQL..."):
        kpi = get_kpis(spark, filters=filters, view=clean_view)

    # 1. 10 KPI metric cards
    render_kpi_scorecard(kpi)

    # 2. Key findings from get_insights
    with st.spinner("Synthesizing climate insights from Spark SQL results..."):
        insights = _cached_insights(store.version, f_str, spark, clean_view, filters)
    render_insights(insights)

    st.markdown("---")
    st.subheader("📊 Headline Climate Visualizations")
    st.caption("Key multi-decadal trends, precipitation patterns, and station comparisons.")

    # 4 headline charts
    # (a) temp_yearly
    f_temp_yr = _cached_analysis_frame(store.version, f_str, "temp_yearly", None, spark, clean_view, filters)
    fig_temp_yr = build_temp_yearly(f_temp_yr.df)
    render_analysis("temp_yearly", f_temp_yr, fig=fig_temp_yr)

    st.markdown("---")
    # (b) rain_yearly
    f_rain_yr = _cached_analysis_frame(store.version, f_str, "rain_yearly", None, spark, clean_view, filters)
    fig_rain_yr = build_rain_yearly(f_rain_yr.df)
    render_analysis("rain_yearly", f_rain_yr, fig=fig_rain_yr)

    st.markdown("---")
    # (c) climate_city_profile
    f_cprof = _cached_analysis_frame(store.version, f_str, "climate_city_profile", None, spark, clean_view, filters)
    fig_cprof = build_climate_city_profile(f_cprof.df, metric="avg_temp")
    render_analysis("climate_city_profile", f_cprof, fig=fig_cprof)

    st.markdown("---")
    # (d) temp_trend
    f_trend = _cached_analysis_frame(store.version, f_str, "temp_trend", None, spark, clean_view, filters)
    fig_trend, stats_table = build_temp_trend(f_trend.df, yearly_points_df=f_temp_yr.df)
    render_analysis(
        "temp_trend",
        f_trend,
        fig=fig_trend,
        extra_table=stats_table,
        help_note="Trend lines represent least-squares regression slopes (regr_slope) computed by Spark SQL across complete years.",
    )

    st.markdown("---")
    # 3. KPI SQL Expander
    with st.expander("Spark SQL behind headline KPIs"):
        st.code(kpi.get("sql", "").strip(), language="sql")


# =============================================================================
# 2. Section: Temperature
# =============================================================================

def render_temperature_section(spark: SparkSession, store: ActiveDatasetStore, filters: Filters):
    """Render all 6 temperature analyses and interactive figures."""
    st.subheader("🌡️ Temperature Dynamics & Multi-Decadal Trends")
    st.caption("Thermal patterns, annual anomalies, climatological regimes, and warming rates computed via Spark SQL.")

    clean_view = store.prepared.clean_view  # type: ignore
    f_str = _filters_repr(filters)
    active_locs = filters.locations or []

    # 1. temp_yearly
    f_yearly = _cached_analysis_frame(store.version, f_str, "temp_yearly", None, spark, clean_view, filters)
    fig_yearly = build_temp_yearly(f_yearly.df)
    render_analysis("temp_yearly", f_yearly, fig=fig_yearly)

    st.markdown("---")

    # 2. temp_monthly_climatology
    f_clim = _cached_analysis_frame(store.version, f_str, "temp_monthly_climatology", None, spark, clean_view, filters)
    fig_clim = build_temp_monthly_climatology(f_clim.df)
    render_analysis("temp_monthly_climatology", f_clim, fig=fig_clim)

    st.markdown("---")

    # 3. temp_monthly_series (Single station drilldown)
    col_sel, _ = st.columns([2, 4])
    with col_sel:
        target_loc = st.selectbox(
            "Select Station for Chronological Series",
            options=active_locs,
            key="sel_temp_series_loc",
        )
    f_series = _cached_analysis_frame(store.version, f_str, "temp_monthly_series", None, spark, clean_view, filters)
    fig_series = build_temp_monthly_series(f_series.df, location=target_loc)
    render_analysis("temp_monthly_series", f_series, fig=fig_series)

    st.markdown("---")

    # 4. temp_location_ranking
    f_rank = _cached_analysis_frame(store.version, f_str, "temp_location_ranking", None, spark, clean_view, filters)
    fig_rank = build_temp_location_ranking(f_rank.df)
    render_analysis("temp_location_ranking", f_rank, fig=fig_rank)

    st.markdown("---")

    # 5. temp_trend
    f_trend = _cached_analysis_frame(store.version, f_str, "temp_trend", None, spark, clean_view, filters)
    # Pass yearly points if available for scatter overlay
    fig_trend, stats_table = build_temp_trend(f_trend.df, yearly_points_df=f_yearly.df)
    render_analysis(
        "temp_trend",
        f_trend,
        fig=fig_trend,
        extra_table=stats_table,
        help_note="Trend lines represent least-squares regression slopes (regr_slope) computed by Spark SQL across complete years.",
    )

    st.markdown("---")

    # 6. temp_seasonal
    f_season = _cached_analysis_frame(store.version, f_str, "temp_seasonal", None, spark, clean_view, filters)
    fig_season = build_temp_seasonal(f_season.df)
    render_analysis("temp_seasonal", f_season, fig=fig_season)


# =============================================================================
# 3. Section: Rainfall
# =============================================================================

def render_rainfall_section(spark: SparkSession, store: ActiveDatasetStore, filters: Filters):
    """Render all 7 precipitation analyses, seasonality breakdowns, and anomaly detection."""
    st.subheader("🌧️ Rainfall Patterns, Monsoon Regimes & Extreme Deluges")
    st.caption("Precipitation volume, seasonal partitioning, statistical anomalies (Z-Score), and IMD deluge events.")

    clean_view = store.prepared.clean_view  # type: ignore
    f_str = _filters_repr(filters)
    active_locs = filters.locations or []

    # 1. rain_yearly
    f_yearly = _cached_analysis_frame(store.version, f_str, "rain_yearly", None, spark, clean_view, filters)
    fig_yearly = build_rain_yearly(f_yearly.df)
    render_analysis("rain_yearly", f_yearly, fig=fig_yearly)

    st.markdown("---")

    # 2. rain_monthly (Station Heatmap)
    col_sel, _ = st.columns([2, 4])
    with col_sel:
        target_rain_loc = st.selectbox(
            "Select Station for Monthly Rainfall Heatmap",
            options=active_locs,
            key="sel_rain_monthly_loc",
        )
    f_monthly = _cached_analysis_frame(store.version, f_str, "rain_monthly", None, spark, clean_view, filters)
    fig_monthly = build_rain_monthly(f_monthly.df, location=target_rain_loc)
    render_analysis("rain_monthly", f_monthly, fig=fig_monthly)

    st.markdown("---")

    # 3. rain_monthly_climatology
    f_clim = _cached_analysis_frame(store.version, f_str, "rain_monthly_climatology", None, spark, clean_view, filters)
    fig_clim = build_rain_monthly_climatology(f_clim.df)
    render_analysis("rain_monthly_climatology", f_clim, fig=fig_clim)

    st.markdown("---")

    # 4. rain_location_ranking
    f_rank = _cached_analysis_frame(store.version, f_str, "rain_location_ranking", None, spark, clean_view, filters)
    fig_rank = build_rain_location_ranking(f_rank.df)
    render_analysis("rain_location_ranking", f_rank, fig=fig_rank)

    st.markdown("---")

    # 5. rain_seasonal
    f_season = _cached_analysis_frame(store.version, f_str, "rain_seasonal", None, spark, clean_view, filters)
    fig_season = build_rain_seasonal(f_season.df)
    render_analysis("rain_seasonal", f_season, fig=fig_season)

    st.markdown("---")

    # 6. rain_unusual_months
    col_z, _ = st.columns([2, 4])
    with col_z:
        target_z_loc = st.selectbox(
            "Select Station for Standardized Anomaly (Z-Score)",
            options=active_locs,
            key="sel_rain_z_loc",
        )
    f_unusual = _cached_analysis_frame(store.version, f_str, "rain_unusual_months", None, spark, clean_view, filters)
    fig_unusual, flagged_table = build_rain_unusual_months(f_unusual.df, location=target_z_loc)
    render_analysis(
        "rain_unusual_months",
        f_unusual,
        fig=fig_unusual,
        extra_table=flagged_table,
        help_note="Statistically unusual rainfall is flagged when monthly precipitation exceeds 2.0 standard deviations (Z >= 2.0) above the station-month mean.",
    )

    st.markdown("---")

    # 7. rain_heavy_days
    col_h, _ = st.columns([2, 4])
    with col_h:
        target_h_loc = st.selectbox(
            "Select Station for Heavy Rainfall Frequency",
            options=active_locs,
            key="sel_rain_heavy_loc",
        )
    f_heavy = _cached_analysis_frame(store.version, f_str, "rain_heavy_days", None, spark, clean_view, filters)
    fig_heavy = build_rain_heavy_days(f_heavy.df, location=target_h_loc)
    render_analysis("rain_heavy_days", f_heavy, fig=fig_heavy)


# =============================================================================
# 4. Section: Climate Patterns
# =============================================================================

def render_climate_section(spark: SparkSession, store: ActiveDatasetStore, filters: Filters):
    """Render climate profile comparisons, extreme events, heatwaves, and correlations."""
    st.subheader("⚡ Climate Regimes, Weather Extremes & Atmospheric Correlations")
    st.caption("Station comparisons, extreme thresholds (95th percentile), heatwaves (gaps-and-islands), and Pearson correlations.")

    clean_view = store.prepared.clean_view  # type: ignore
    f_str = _filters_repr(filters)
    active_locs = filters.locations or []

    # 1. climate_year_profile
    f_yprof = _cached_analysis_frame(store.version, f_str, "climate_year_profile", None, spark, clean_view, filters)
    render_analysis("climate_year_profile", f_yprof)

    st.markdown("---")

    # 2. climate_year_comparison (Year A vs Year B selectors)
    avail_years = list(range(2010, 2026))
    cA, cB, _ = st.columns([2, 2, 4])
    with cA:
        yr_a = st.selectbox("Baseline Year (A)", options=avail_years, index=5, key="sel_cmp_yr_a")  # 2015
    with cB:
        yr_b = st.selectbox("Comparison Year (B)", options=avail_years, index=10, key="sel_cmp_yr_b")  # 2020

    cmp_params = (("year_a", yr_a), ("year_b", yr_b))
    f_cmp = _cached_analysis_frame(store.version, f_str, "climate_year_comparison", cmp_params, spark, clean_view, filters)
    fig_cmp = build_climate_year_comparison(f_cmp.df, year_a=yr_a, year_b=yr_b)
    render_analysis("climate_year_comparison", f_cmp, fig=fig_cmp, extra_table=f_cmp.df)

    st.markdown("---")

    # 3. climate_city_profile (Metric selector)
    metric_options = {
        "avg_temp": "Average Temperature (°C)",
        "annual_rainfall_equiv": "Annual Rainfall Equivalent (mm)",
        "avg_humidity": "Average Humidity (%)",
        "avg_wind_speed": "Average Wind Speed (km/h)",
        "avg_pressure": "Average Pressure (hPa)",
        "rainy_days_per_year": "Rainy Days Per Year",
    }
    c_m, _ = st.columns([3, 4])
    with c_m:
        chosen_metric = st.selectbox(
            "Select Metric to Rank Stations",
            options=list(metric_options.keys()),
            format_func=lambda x: metric_options[x],
            key="sel_city_prof_metric",
        )
    f_cprof = _cached_analysis_frame(store.version, f_str, "climate_city_profile", None, spark, clean_view, filters)
    fig_cprof = build_climate_city_profile(f_cprof.df, metric=chosen_metric)
    render_analysis("climate_city_profile", f_cprof, fig=fig_cprof, extra_table=f_cprof.df)

    st.markdown("---")

    # 4. extreme_thresholds
    f_thresh = _cached_analysis_frame(store.version, f_str, "extreme_thresholds", None, spark, clean_view, filters)
    render_analysis(
        "extreme_thresholds",
        f_thresh,
        help_note="Notice: Statistical 95th/5th percentile thresholds are established across the entire multi-year historical dataset to ensure stable climatological baselines.",
    )

    st.markdown("---")

    # 5. extreme_events_yearly (Metric selector)
    ev_options = {
        "hot_days": "Hot Days (temp_max >= 95th percentile)",
        "cold_days": "Cold Days (temp_min <= 5th percentile)",
        "extreme_rain_days": "Extreme Rain Days (rain >= 95th percentile of wet days)",
        "heavy_rain_days": "Heavy Rain Days (rain >= 64.5 mm)",
    }
    c_ev, _ = st.columns([3, 4])
    with c_ev:
        chosen_ev_metric = st.selectbox(
            "Select Extreme Event Metric",
            options=list(ev_options.keys()),
            format_func=lambda x: ev_options[x],
            key="sel_ev_metric",
        )
    f_ev = _cached_analysis_frame(store.version, f_str, "extreme_events_yearly", None, spark, clean_view, filters)
    fig_ev = build_extreme_events_yearly(f_ev.df, metric=chosen_ev_metric)
    render_analysis("extreme_events_yearly", f_ev, fig=fig_ev)

    st.markdown("---")

    # 6. extreme_top_events (Top-N slider)
    c_n, _ = st.columns([2, 5])
    with c_n:
        top_n = st.slider("Number of Top Events (N)", min_value=5, max_value=30, value=TOP_N_DEFAULT, key="sld_top_n")
    top_params = (("n", top_n),)
    f_top = _cached_analysis_frame(store.version, f_str, "extreme_top_events", top_params, spark, clean_view, filters)
    render_analysis("extreme_top_events", f_top)

    st.markdown("---")

    # 7. heatwave_spells
    f_hw = _cached_analysis_frame(store.version, f_str, "heatwave_spells", None, spark, clean_view, filters)
    fig_hw = build_heatwave_spells(f_hw.df)
    render_analysis(
        "heatwave_spells",
        f_hw,
        fig=fig_hw,
        extra_table=f_hw.df,
        help_note="Discovered via Spark SQL gaps-and-islands window pattern (DATE_SUB on ROW_NUMBER). A heatwave spell requires >= 3 consecutive days breaching the station hot threshold.",
    )

    st.markdown("---")

    # 8. correlations (Mode & Scope selectors)
    c_mode, c_scope = st.columns([2, 2])
    with c_mode:
        corr_mode = st.radio(
            "Correlation Mode",
            options=["raw", "anomaly"],
            format_func=lambda x: "Raw Observations" if x == "raw" else "Deseasonalized Anomalies",
            key="rad_corr_mode",
            horizontal=True,
        )
    with c_scope:
        scope_options = ["all"] + active_locs
        corr_scope = st.selectbox(
            "Correlation Scope",
            options=scope_options,
            format_func=lambda x: "All Stations" if x == "all" else f"Station: {x}",
            key="sel_corr_scope",
        )

    corr_params = (("mode", corr_mode), ("scope", corr_scope))
    f_corr = _cached_analysis_frame(store.version, f_str, "correlations", corr_params, spark, clean_view, filters)
    fig_corr = build_correlations(f_corr.df, mode=corr_mode, scope=corr_scope)
    render_analysis(
        "correlations",
        f_corr,
        fig=fig_corr,
        help_note="Notice: Anomaly mode removes the strong 12-month astronomical seasonal cycle by subtracting each station's monthly mean baseline before computing Pearson r. Correlation does not imply causation.",
    )


# =============================================================================
# 5. Section: Data Quality
# =============================================================================

def render_quality_section(store: ActiveDatasetStore):
    """Render validation findings, column statistics, and coverage tables."""
    st.subheader("📋 Dataset Quality & Validation Audit")
    st.caption("Schema conformance, sentinel codes, numerical range validity, and structural integrity.")

    load_res = store.prepared.load_result  # type: ignore
    summary = summarize_validation(load_res.validation)

    if summary["is_valid"]:
        st.success("Validation PASSED: Dataset structure satisfies all standard schema constraints.")
    else:
        st.error(f"Validation FAILED: {summary['error_count']} critical error(s) detected.")

    if summary["errors"]:
        st.markdown("**Critical Schema Errors:**")
        for err in summary["errors"]:
            st.error(f"• {err}")

    if summary["warnings"]:
        st.markdown("**Data Quality Warnings:**")
        for w in summary["warnings"]:
            st.warning(f"• {w}")

    # Defect summary metrics
    d1, d2, d3, d4 = st.columns(4)
    with d1:
        st.metric("Sentinel Codes", summary["total_sentinels"])
    with d2:
        st.metric("Out-of-Range", summary["total_out_of_range"])
    with d3:
        st.metric("Parse Failures", summary["total_parse_failures"])
    with d4:
        st.metric("Duplicate Keys", summary["total_duplicate_keys"])

    st.markdown("---")
    st.markdown("**Column Schema & Completeness Table:**")
    st.dataframe(summary["column_table"], use_container_width=True)


# =============================================================================
# 6. Section: Cleaning Report
# =============================================================================

def render_cleaning_section(store: ActiveDatasetStore):
    """Render cleaning pipeline audit: row count deltas, step SQL, and interpolation stats."""
    st.subheader("🧹 Cleaning Pipeline & Transformation Stages")
    st.caption("Deterministic Spark SQL cleaning stages: unusable drops, sentinel nullification, deduplication, and gap interpolation.")

    rep = store.prepared.cleaning_report  # type: ignore
    c_summary = summarize_cleaning(rep)

    st.markdown("**Cleaning Pipeline Execution Summary:**")
    s1, s2, s3, s4 = st.columns(4)
    with s1:
        st.metric("Raw Rows In", f"{rep.summary.get('raw_rows_in', 0):,}")
    with s2:
        st.metric("Clean Rows Out", f"{rep.summary.get('clean_rows_out', 0):,}")
    with s3:
        st.metric("Rows Removed", f"{rep.summary.get('rows_removed', 0):,}")
    with s4:
        tot_interp = rep.summary.get("values_changed", {}).get("total_interpolated", 0)
        st.metric("Total Interpolated", f"{tot_interp:,}")

    st.markdown("---")
    st.markdown("**Pipeline Steps Breakdown:**")
    st.dataframe(c_summary["steps_table"], use_container_width=True)

    st.markdown("---")
    st.markdown("**Step-by-Step Spark SQL Queries:**")
    for s in rep.steps:
        with st.expander(f"Step {s['step']}: {s['description']}"):
            st.markdown(f"Rows before: `{s['rows_before']:,}` → Rows after: `{s['rows_after']:,}` (Removed: `{s['rows_removed']:,}`)")
            if s.get("values_changed"):
                st.json(s["values_changed"])
            st.code(s.get("sql", "").strip(), language="sql")


# =============================================================================
# 7. Section: Data Preview
# =============================================================================

def render_preview_section(spark: SparkSession, store: ActiveDatasetStore, filters: Filters):
    """Render paginated data preview table of clean dataset records."""
    st.subheader("🔍 Clean Dataset Preview")
    st.caption("Interactive inspection of records in the active clean view ordered by station and date.")

    clean_view = store.prepared.clean_view  # type: ignore

    r_col, _ = st.columns([2, 5])
    with r_col:
        num_rows = st.number_input(
            "Rows to preview",
            min_value=10,
            max_value=PREVIEW_ROWS_MAX,
            value=PREVIEW_ROWS_DEFAULT,
            step=50,
            key="input_preview_rows",
        )

    with st.spinner("Fetching preview records from Spark..."):
        preview_df, preview_sql = get_preview(
            spark, filters=filters, n=int(num_rows), view=clean_view
        )

    st.dataframe(preview_df, use_container_width=True)

    with st.expander("Spark SQL used for preview"):
        st.code(preview_sql, language="sql")


# =============================================================================
# 8. Section: Spark SQL
# =============================================================================

def render_spark_sql_section(spark: SparkSession, store: ActiveDatasetStore, filters: Filters):
    """Render comprehensive Spark SQL query log and physical execution plan explain viewer."""
    st.subheader("⚙️ Spark SQL Query Inspector & Catalyst Execution Plans")
    st.caption("Inspect exact declarative SQL queries and physical Catalyst execution plans (explain) across all 22 analysis queries.")

    clean_view = store.prepared.clean_view  # type: ignore

    # Selectbox of all 22 analysis keys
    analysis_keys = list(ANALYSIS_UI_MAP.keys())
    chosen_key = st.selectbox(
        "Select Analysis Query to Inspect",
        options=analysis_keys,
        format_func=lambda k: f"{ANALYSIS_UI_MAP[k]['title']} ({k})",
        key="sel_explain_key",
    )

    frame = get_analysis_frame(spark, key=chosen_key, filters=filters, view=clean_view)

    st.markdown(f"**Query Title:** {ANALYSIS_UI_MAP[chosen_key]['title']}")
    st.markdown(f"**Section:** `{ANALYSIS_UI_MAP[chosen_key]['section']}` | **Kind:** `{ANALYSIS_UI_MAP[chosen_key]['kind']}`")
    st.caption(ANALYSIS_UI_MAP[chosen_key]["description"])

    st.markdown("**Spark SQL Query Statement:**")
    st.code(frame.sql.strip(), language="sql")

    if st.button("Show Spark execution plan", key="btn_show_explain"):
        with st.spinner("Generating Spark physical Catalyst plan via explain()..."):
            try:
                # Capture explain output from PySpark DataFrame
                fn = get_analysis(chosen_key)["function"]
                res = fn(spark, filters=filters, view=clean_view)
                
                # Capture plan output via _jdf.explain() string capture
                plan_str = res.df._jdf.queryExecution().explainString(
                    res.df._sc._gateway.jvm.org.apache.spark.sql.execution.ExtendedMode()
                )
                st.markdown("**Catalyst Physical Execution Plan (`explain(extended=True)`):**")
                st.code(plan_str, language="text")
            except Exception as exc:
                st.error(f"Could not generate physical plan: {exc}")
