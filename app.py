"""
app.py
======
Weather and Climate Pattern Discovery - Streamlit Dashboard Foundation.

Powered by Apache Spark SQL & PySpark.
Phase 6: Ingestion, schema mapping, validation/cleaning reporting, filters, and KPI cards.
Charts, Plotly visualizations, and auto-generated insights follow in Phase 7.
"""

from __future__ import annotations

import contextlib
from datetime import date, datetime
import io
import logging
from pathlib import Path
import sys
from typing import Any

import pandas as pd
import streamlit as st
from pyspark.sql import SparkSession

from config.schema import (
    OPTIONAL_COLUMNS,
    REQUIRED_COLUMNS,
    STANDARD_COLUMNS,
)
from config.settings import (
    BUILTIN_DATASET_LABEL,
    CLEAN_VIEW_NAME,
    DASHBOARD_SUBTITLE,
    DASHBOARD_TITLE,
    NO_COLUMN_LABEL,
    PREVIEW_ROWS_DEFAULT,
    PREVIEW_ROWS_MAX,
)
from src.dashboard.services import (
    ActiveDatasetStore,
    UploadInspection,
    build_filters,
    get_filter_options,
    get_kpis,
    get_preview,
    inspect_upload,
    load_builtin,
    load_upload,
    summarize_cleaning,
    summarize_validation,
)
from src.data_loader import DatasetLoadError
from src.pipeline import PipelineError
from src.queries.base import Filters
from src.queries.registry import ANALYSES
from src.spark_session import get_spark_session
from src.upload import UploadError

logger = logging.getLogger(__name__)

# =============================================================================
# 1. Page Config & Custom Styling
# =============================================================================

st.set_page_config(
    page_title=DASHBOARD_TITLE,
    page_icon="🌦️",
    layout="wide",
)

# Light metric card styling: rounded border and padding only (no external fonts or scripts)
st.markdown(
    """
    <style>
    div[data-testid="stMetric"] {
        background-color: #1E293B;
        border: 1px solid #334155;
        padding: 12px 18px;
        border-radius: 8px;
    }
    div[data-testid="stMetricLabel"] p {
        font-size: 0.85rem;
        color: #94A3B8;
        margin-bottom: 4px;
    }
    div[data-testid="stMetricValue"] div {
        font-size: 1.6rem;
        font-weight: 600;
        color: #F8FAFC;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# =============================================================================
# 2. Spark Session Resource & Cached Data Accessors
# =============================================================================

@st.cache_resource
def get_spark() -> SparkSession:
    """Initialize or return the cached process-wide SparkSession."""
    return get_spark_session("WeatherDashboard")


def _filters_to_tuple(f: Filters | None) -> tuple:
    """Convert Filters object into a deterministic, hashable tuple for caching."""
    if f is None:
        return ()
    locs = tuple(sorted(f.locations)) if f.locations is not None else None
    yrs = tuple(sorted(f.years)) if f.years is not None else None
    sns = tuple(sorted(f.seasons)) if f.seasons is not None else None
    return (locs, f.start_date, f.end_date, yrs, sns)


def _filters_from_tuple(t: tuple) -> Filters | None:
    """Reconstruct Filters object from its hashable tuple representation."""
    if not t:
        return None
    locs, start, end, yrs, sns = t
    return Filters(
        locations=list(locs) if locs is not None else None,
        start_date=start,
        end_date=end,
        years=list(yrs) if yrs is not None else None,
        seasons=list(sns) if sns is not None else None,
    )


@st.cache_data
def _cached_filter_options(version: int, _spark: SparkSession, view: str) -> dict[str, Any]:
    return get_filter_options(_spark, view=view)


@st.cache_data
def _cached_kpis(version: int, f_tuple: tuple, _spark: SparkSession, view: str) -> dict[str, Any]:
    filters = _filters_from_tuple(f_tuple)
    return get_kpis(_spark, filters=filters, view=view)


@st.cache_data
def _cached_preview(version: int, f_tuple: tuple, n: int, _spark: SparkSession, view: str) -> tuple[pd.DataFrame, str]:
    filters = _filters_from_tuple(f_tuple)
    return get_preview(_spark, filters=filters, n=n, view=view)


# =============================================================================
# 3. Sidebar UI Components
# =============================================================================

def render_sidebar(spark: SparkSession, store: ActiveDatasetStore) -> Filters | None:
    """Render the sidebar controls: data source selection, upload, and filters."""
    st.sidebar.title("Data Controls")

    # ── Section: Data source ───────────────────────────────────────────────────
    st.sidebar.subheader("Data Source")
    source_choice = st.sidebar.radio(
        "Select dataset origin",
        ["Built-in dataset", "Upload CSV"],
        index=0 if store.source != "upload" else 1,
        key="sb_data_source",
    )

    if source_choice == "Built-in dataset":
        if st.sidebar.button("Reload built-in dataset", key="btn_reload_builtin"):
            with st.spinner("Reloading built-in dataset into Spark..."):
                try:
                    load_builtin(spark)
                    st.sidebar.success("Built-in dataset reloaded successfully.")
                    st.rerun()
                except Exception as exc:
                    logger.exception("Failed to load built-in dataset")
                    st.sidebar.error(f"Could not load built-in dataset: {exc}")

    else:
        # Upload flow
        uploaded_file = st.sidebar.file_uploader(
            "Upload weather CSV file",
            type=["csv"],
            key="file_uploader_csv",
            help="Upload a daily weather CSV (max 200 MB).",
        )

        if uploaded_file is not None:
            file_bytes = uploaded_file.getvalue()
            try:
                inspection = inspect_upload(file_bytes, uploaded_file.name)
                st.sidebar.caption(
                    f"Saved: `{inspection.saved_name}` | "
                    f"Delimiter: **{inspection.delimiter}** | "
                    f"Columns: **{len(inspection.original_columns)}**"
                )

                st.sidebar.markdown("##### Column Mapping")
                st.sidebar.caption("Map file columns to standard system columns (* = required):")

                file_options = [NO_COLUMN_LABEL] + inspection.original_columns
                chosen_mapping: dict[str, str] = {}
                mapped_targets: list[str] = []

                all_standard_cols = [
                    "date", "location", "temperature", "rainfall", "humidity",
                    "wind_speed", "pressure", "temp_max", "temp_min", "latitude", "longitude"
                ]

                for std_col in all_standard_cols:
                    is_req = std_col in REQUIRED_COLUMNS
                    label_text = f"{'* ' if is_req else ''}{std_col}"
                    suggested_orig = inspection.suggested_mapping.get(std_col)

                    default_idx = 0
                    if suggested_orig and suggested_orig in file_options:
                        default_idx = file_options.index(suggested_orig)

                    selected = st.sidebar.selectbox(
                        label_text,
                        file_options,
                        index=default_idx,
                        key=f"map_sel_{std_col}",
                    )
                    chosen_mapping[std_col] = selected
                    if selected != NO_COLUMN_LABEL:
                        mapped_targets.append(selected)

                # Check for duplicate column mapping collisions
                duplicates = [col for col in set(mapped_targets) if mapped_targets.count(col) > 1]
                has_duplicate = len(duplicates) > 0
                if has_duplicate:
                    st.sidebar.warning(f"Column collision: {', '.join(duplicates)} mapped to multiple targets.")

                # Fallback location input if location column is unmapped
                default_loc = None
                if chosen_mapping.get("location") == NO_COLUMN_LABEL:
                    default_loc = st.sidebar.text_input(
                        "Location name for this dataset",
                        value="Uploaded location",
                        key="input_default_loc",
                        help="Required when the CSV has no location column.",
                    )

                fill_gaps = st.sidebar.checkbox(
                    "Fill gaps of up to 3 days by interpolation (flagged)",
                    value=True,
                    key="chk_fill_gaps",
                )

                disable_btn = has_duplicate or (chosen_mapping.get("location") == NO_COLUMN_LABEL and not default_loc)

                if st.sidebar.button("Process dataset", disabled=disable_btn, key="btn_process_upload"):
                    with st.spinner("Processing, validating, and cleaning uploaded dataset..."):
                        try:
                            prep = load_upload(
                                spark=spark,
                                saved_name=inspection.saved_name,
                                mapping=chosen_mapping,
                                default_location=default_loc,
                                fill_gaps=fill_gaps,
                            )
                            raw_in = prep.cleaning_report.summary.get("raw_rows_in", 0)
                            clean_out = prep.cleaning_report.summary.get("clean_rows_out", 0)
                            st.sidebar.success(
                                f"Dataset ingested: {raw_in:,} raw rows in -> {clean_out:,} clean rows out."
                            )
                            st.rerun()
                        except PipelineError as p_err:
                            st.sidebar.error("Dataset validation failed:")
                            for err in p_err.errors:
                                st.sidebar.error(f"• {err}")
                        except (UploadError, DatasetLoadError, ValueError) as d_err:
                            st.sidebar.error(f"Upload error: {d_err}")
                        except Exception as exc:
                            logger.exception("Unexpected error processing upload")
                            st.sidebar.error("An unexpected error occurred while processing the file.")

            except (UploadError, DatasetLoadError) as err:
                st.sidebar.error(f"Upload failed: {err}")
            except Exception as exc:
                logger.exception("Failed to inspect uploaded file")
                st.sidebar.error("Could not read uploaded file headers.")

    # ── Section: Filters ───────────────────────────────────────────────────────
    if not store.is_ready(spark) or store.prepared is None:
        return None

    st.sidebar.divider()
    st.sidebar.subheader("Filters")

    clean_view = store.prepared.clean_view
    options = _cached_filter_options(store.version, spark, clean_view)

    all_locations = options["locations"]
    min_date_val = datetime.strptime(options["min_date"], "%Y-%m-%d").date() if options["min_date"] else date(2010, 1, 1)
    max_date_val = datetime.strptime(options["max_date"], "%Y-%m-%d").date() if options["max_date"] else date(2025, 12, 31)

    # Reset button
    if st.sidebar.button("Reset filters", key="btn_reset_filters"):
        st.session_state["sel_locations"] = all_locations
        st.session_state["sel_dates"] = (min_date_val, max_date_val)
        st.session_state["sel_years"] = []
        st.session_state["sel_seasons"] = []
        st.rerun()

    # Location selection
    selected_locations = st.sidebar.multiselect(
        "Locations",
        options=all_locations,
        default=st.session_state.get("sel_locations", all_locations),
        key="sel_locations",
    )

    if not selected_locations:
        st.warning("Select at least one location.")
        return None

    # Date range selection
    date_val = st.sidebar.date_input(
        "Date range",
        value=st.session_state.get("sel_dates", (min_date_val, max_date_val)),
        min_value=min_date_val,
        max_value=max_date_val,
        key="sel_dates",
    )

    start_date = None
    end_date = None
    if isinstance(date_val, (tuple, list)):
        if len(date_val) == 1:
            st.info("Please select the end date.")
            return None
        elif len(date_val) >= 2:
            start_date, end_date = date_val[0], date_val[1]
    elif isinstance(date_val, date):
        start_date = date_val
        end_date = date_val

    # Optional years
    avail_years = options["years"]
    selected_years = st.sidebar.multiselect(
        "Limit to specific years",
        options=avail_years,
        default=st.session_state.get("sel_years", []),
        key="sel_years",
    )

    # Optional seasons
    avail_seasons = options["seasons"]
    selected_seasons = st.sidebar.multiselect(
        "Limit to seasons",
        options=avail_seasons,
        default=st.session_state.get("sel_seasons", []),
        key="sel_seasons",
    )

    try:
        filters = build_filters(
            locations=selected_locations,
            start=start_date,
            end=end_date,
            years=selected_years,
            seasons=selected_seasons,
        )
        return filters
    except ValueError as ve:
        st.error(str(ve))
        return None


# =============================================================================
# 4. Header Strip Component
# =============================================================================

def render_header(store: ActiveDatasetStore):
    """Render top header banner and dataset metrics strip."""
    st.title(f"🌦️ {DASHBOARD_TITLE}")
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

    st.divider()


# =============================================================================
# 5. Tab Renderers
# =============================================================================

def render_overview_tab(spark: SparkSession, store: ActiveDatasetStore, filters: Filters):
    """Render KPI metric cards and SQL query expander."""
    clean_view = store.prepared.clean_view  # type: ignore
    f_tuple = _filters_to_tuple(filters)

    with st.spinner("Computing headline KPIs via Spark SQL..."):
        kpi = _cached_kpis(store.version, f_tuple, spark, clean_view)

    total_rec = kpi.get("total_records", 0)
    if total_rec == 0:
        st.warning("No records matched the selected filter criteria.")
        return

    # Row 1 of KPI cards
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.metric("Total Records", f"{kpi['total_records']:,}")
    with c2:
        loc_val = f"{kpi['locations']:,}" if kpi["locations"] is not None else "n/a"
        st.metric("Locations", loc_val)
    with c3:
        avg_t = f"{kpi['avg_temp']:.1f} °C" if kpi["avg_temp"] is not None else "n/a"
        st.metric("Average Temperature", avg_t)
    with c4:
        max_t = f"{kpi['max_temp']:.1f} °C" if kpi["max_temp"] is not None else "n/a"
        cap_hot = f"{kpi['max_temp_date']} at {kpi['max_temp_location']}" if kpi["max_temp_date"] else None
        st.metric("Maximum Temperature", max_t, help=cap_hot)
        if cap_hot:
            st.caption(cap_hot)
    with c5:
        min_t = f"{kpi['min_temp']:.1f} °C" if kpi["min_temp"] is not None else "n/a"
        cap_cold = f"{kpi['min_temp_date']} at {kpi['min_temp_location']}" if kpi["min_temp_date"] else None
        st.metric("Minimum Temperature", min_t, help=cap_cold)
        if cap_cold:
            st.caption(cap_cold)

    # Row 2 of KPI cards
    r1, r2, r3, r4, r5 = st.columns(5)
    with r1:
        rain_val = f"{kpi['total_rainfall_mm']:,.1f} mm" if kpi["total_rainfall_mm"] is not None else "n/a"
        annual_val = f"{kpi['avg_annual_rainfall_per_location']:,.1f}" if kpi["avg_annual_rainfall_per_location"] is not None else "n/a"
        st.metric("Total Rainfall", rain_val)
        st.caption(f"sum over selected city-days; avg per location/yr: {annual_val} mm")
    with r2:
        hum_val = f"{kpi['avg_humidity']:.1f} %" if kpi["avg_humidity"] is not None else "n/a"
        st.metric("Average Humidity", hum_val)
    with r3:
        wind_val = f"{kpi['avg_wind_speed']:.1f} km/h" if kpi["avg_wind_speed"] is not None else "n/a"
        st.metric("Average Wind Speed", wind_val)
    with r4:
        pres_val = f"{kpi['avg_pressure']:.1f} hPa" if kpi["avg_pressure"] is not None else "n/a"
        st.metric("Average Pressure", pres_val)
    with r5:
        interp_cnt = kpi.get("interpolated_value_count", 0)
        st.metric("Interpolated Values", f"{interp_cnt:,}")
        st.caption("linear estimates <= 3 days")

    # SQL inspection expander
    with st.expander("Show the Spark SQL behind these numbers"):
        st.markdown("**Spark SQL Query:**")
        st.code(kpi.get("sql", "").strip(), language="sql")
        st.markdown("**Parameters / Filters:**")
        st.json(kpi.get("parameters", {}))


def render_quality_tab(store: ActiveDatasetStore):
    """Render validation findings, column statistics, and coverage tables."""
    load_res = store.prepared.load_result  # type: ignore
    summary = summarize_validation(load_res.validation)

    st.subheader("Dataset Validation Status")
    if summary["is_valid"]:
        st.success("Validation PASSED: Dataset structure satisfies all standard schema constraints.")
    else:
        st.error("Validation FAILED: Dataset contains fatal structural errors.")

    if summary["errors"]:
        st.markdown("##### Validation Errors")
        for err in summary["errors"]:
            st.error(f"• {err}")

    if summary["warnings"]:
        st.markdown("##### Validation Warnings")
        for w in summary["warnings"]:
            st.warning(f"• {w}")

    st.divider()
    st.markdown("##### Column Profile & Quality Statistics")
    st.dataframe(summary["column_stats"], use_container_width=True)

    st.markdown("##### Location Record Coverage")
    st.dataframe(summary["location_coverage"], use_container_width=True)


def render_cleaning_tab(store: ActiveDatasetStore):
    """Render data cleaning audit trail and immutability notice."""
    report = store.prepared.cleaning_report  # type: ignore
    summary = summarize_cleaning(report)

    st.info(
        "**Data Architecture Note:** Raw dataset files are never modified in place. "
        "The cleaning pipeline executes deterministically via Spark SQL and registers a clean temporary view."
    )

    st.subheader("Cleansing Steps Audit")
    st.dataframe(summary["steps_table"], use_container_width=True)

    if summary["warnings"]:
        st.markdown("##### Cleansing Pipeline Warnings")
        for w in summary["warnings"]:
            st.warning(f"• {w}")

    st.markdown("##### Cleansing Summary Metrics")
    st.json(summary["summary"])


def render_preview_tab(spark: SparkSession, store: ActiveDatasetStore, filters: Filters):
    """Render interactive data preview table and query expander."""
    clean_view = store.prepared.clean_view  # type: ignore
    f_tuple = _filters_to_tuple(filters)

    n_rows = st.number_input(
        "Number of rows to preview",
        min_value=1,
        max_value=PREVIEW_ROWS_MAX,
        value=PREVIEW_ROWS_DEFAULT,
        step=50,
        key="num_preview_rows",
    )

    with st.spinner("Fetching preview records from Spark..."):
        pdf, preview_sql = _cached_preview(store.version, f_tuple, int(n_rows), spark, clean_view)

    st.caption(f"Displaying top {len(pdf):,} records ordered by `location`, `date`:")
    st.dataframe(pdf, use_container_width=True)

    with st.expander("Show Preview Spark SQL Query"):
        st.code(preview_sql, language="sql")


def render_spark_sql_tab(spark: SparkSession, store: ActiveDatasetStore, filters: Filters):
    """Render full recorded SQL statements and physical execution plan explain button."""
    clean_view = store.prepared.clean_view  # type: ignore
    load_res = store.prepared.load_result  # type: ignore
    clean_rep = store.prepared.cleaning_report  # type: ignore

    st.subheader("Spark SQL Execution Log")
    st.markdown("Inspect every SQL statement executed across loading, standardization, and cleaning:")

    # 1. Ingestion SQL
    with st.expander("Ingestion & Standardization SQL"):
        for entry in load_res.sql_log:
            st.markdown(f"**{entry.get('title', 'SQL Statement')}**")
            st.code(entry.get("sql", "").strip(), language="sql")

    # 2. Cleaning SQL
    for s in clean_rep.steps:
        step_num = s.get("step")
        step_desc = s.get("description")
        step_sql = s.get("sql", "")
        if step_sql:
            with st.expander(f"Cleaning Step {step_num}: {step_desc}"):
                st.code(step_sql.strip(), language="sql")

    # 3. KPI Query & Execution Plan
    f_tuple = _filters_to_tuple(filters)
    kpi_data = _cached_kpis(store.version, f_tuple, spark, clean_view)
    kpi_sql = kpi_data.get("sql", "")

    with st.expander("Headline KPI Query"):
        st.code(kpi_sql.strip(), language="sql")

    st.divider()
    st.subheader("Spark Execution Plan")
    st.caption("Inspect Catalyst optimizer physical execution plan for the active KPI query:")

    if st.button("Show Spark execution plan for KPI query", key="btn_explain_kpi"):
        with st.spinner("Compiling physical execution plan..."):
            try:
                kpi_df = spark.sql(kpi_sql)
                buf = io.StringIO()
                with contextlib.redirect_stdout(buf):
                    kpi_df.explain(True)
                plan_text = buf.getvalue()
                st.code(plan_text, language="text")
            except Exception as exc:
                logger.exception("Failed to generate explain plan")
                st.error(f"Could not generate Spark execution plan: {exc}")


def render_analyses_tab():
    """Render analysis engine registry catalogue (visualizations arrive in Phase 7)."""
    st.info(
        "📊 **Phase 7 Preview:** Full interactive Plotly time series, anomaly charts, heatmaps, "
        "and automated meteorological insights are arriving next. "
        "Below is the complete catalogue of 22 analytical queries currently wired into the Spark SQL engine."
    )

    reg_rows = []
    for idx, item in enumerate(ANALYSES, start=1):
        reg_rows.append({
            "#": idx,
            "Key": item["key"],
            "Category": item["category"].upper(),
            "Title": item["title"],
        })

    reg_df = pd.DataFrame(reg_rows)
    st.dataframe(reg_df, use_container_width=True, hide_index=True)


# =============================================================================
# 6. Main Application Controller
# =============================================================================

def main():
    """Application entrypoint."""
    spark = get_spark()
    store = ActiveDatasetStore.get_instance()

    # Initial dataset load if store is empty
    if not store.is_ready(spark):
        with st.spinner("Starting Spark and preparing the dataset (first load takes about 20-30 seconds)..."):
            try:
                load_builtin(spark)
            except Exception as exc:
                logger.exception("First load of built-in dataset failed")
                st.error(f"Failed to initialize dataset: {exc}")
                if st.button("Retry initialization"):
                    st.rerun()
                return

    # Render sidebar controls and retrieve active filters
    filters = render_sidebar(spark, store)

    # Render header metrics
    render_header(store)

    if filters is None:
        return

    # Tab navigation
    tabs = st.tabs([
        "Overview",
        "Data quality",
        "Cleaning report",
        "Data preview",
        "Spark SQL",
        "Analyses (coming in Phase 7)",
    ])

    with tabs[0]:
        render_overview_tab(spark, store, filters)

    with tabs[1]:
        render_quality_tab(store)

    with tabs[2]:
        render_cleaning_tab(store)

    with tabs[3]:
        render_preview_tab(spark, store, filters)

    with tabs[4]:
        render_spark_sql_tab(spark, store, filters)

    with tabs[5]:
        render_analyses_tab()


if __name__ == "__main__":
    main()
