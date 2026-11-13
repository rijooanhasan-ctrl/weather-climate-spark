"""
app.py
======
Weather and Climate Pattern Discovery - Streamlit Dashboard Foundation.

Powered by Apache Spark SQL & PySpark.
Interactive multi-view dashboard with real-time Spark SQL analytical queries,
Plotly climatological charts, and deterministic automated insights.
"""

from __future__ import annotations

from datetime import date, datetime
import logging
from pathlib import Path
from typing import Any

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
)
from src.dashboard.catalog import SECTIONS
from src.dashboard.services import (
    ActiveDatasetStore,
    UploadInspection,
    build_filters,
    get_filter_options,
    inspect_upload,
    is_prefetching,
    load_builtin,
    load_upload,
    prefetch_status,
)
from src.dashboard.ui.components import render_header
from src.dashboard.ui.sections import (
    render_cleaning_section,
    render_climate_section,
    render_overview_section,
    render_preview_section,
    render_quality_section,
    render_rainfall_section,
    render_spark_sql_section,
    render_temperature_section,
)
from src.data_loader import DatasetLoadError
from src.pipeline import PipelineError
from src.queries.base import Filters
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

# Metric card styling: rounded border and padding only (no external fonts or scripts)
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


@st.cache_data
def _cached_filter_options(version: int, _spark: SparkSession, view: str) -> dict[str, Any]:
    return get_filter_options(_spark, view=view)


# =============================================================================
# 3. Sidebar UI Controls
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
            with st.sidebar.status("Reloading built-in dataset...", expanded=True) as status_box:
                def _prog(step_name: str, idx: int, total: int):
                    status_box.write(f"[{idx}/{total}] {step_name}...")
                try:
                    load_builtin(spark, progress=_prog)
                    status_box.update(label="Built-in dataset reloaded successfully!", state="complete", expanded=False)
                    st.rerun()
                except Exception as exc:
                    logger.exception("Failed to load built-in dataset")
                    status_box.update(label="Failed to reload dataset", state="error")
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
                    with st.sidebar.status("Processing uploaded dataset...", expanded=True) as status_box:
                        def _prog(step_name: str, idx: int, total: int):
                            status_box.write(f"[{idx}/{total}] {step_name}...")
                        try:
                            prep = load_upload(
                                spark=spark,
                                saved_name=inspection.saved_name,
                                mapping=chosen_mapping,
                                default_location=default_loc,
                                fill_gaps=fill_gaps,
                                progress=_prog,
                            )
                            raw_in = prep.cleaning_report.summary.get("raw_rows_in", 0)
                            clean_out = prep.cleaning_report.summary.get("clean_rows_out", 0)
                            status_box.update(
                                label=f"Dataset ingested: {raw_in:,} raw in -> {clean_out:,} clean out.",
                                state="complete",
                                expanded=False,
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
        st.sidebar.divider()
        done_p, total_p = prefetch_status()
        if is_prefetching():
            st.sidebar.caption(f"⚡ Prefetching queries in background: ({done_p}/{total_p})...")
        elif done_p > 0:
            st.sidebar.caption(f"⚡ Cache warm ({done_p}/{total_p} queries ready)")
        return filters
    except ValueError as ve:
        st.error(str(ve))
        return None


# =============================================================================
# 4. Main Application Controller
# =============================================================================

def main():
    """Application entrypoint."""
    spark = get_spark()
    store = ActiveDatasetStore.get_instance()

    # Initial dataset load if store is empty
    if not store.is_ready(spark):
        with st.status("Initializing and preparing built-in dataset...", expanded=True) as status_box:
            def _prog(step_name: str, idx: int, total: int):
                status_box.write(f"[{idx}/{total}] {step_name}...")
            try:
                load_builtin(spark, progress=_prog)
                status_box.update(label="Dataset preparation complete!", state="complete", expanded=False)
            except Exception as exc:
                logger.exception("First load of built-in dataset failed")
                status_box.update(label=f"Failed to initialize dataset: {exc}", state="error")
                st.error(f"Failed to initialize dataset: {exc}")
                if st.button("Retry initialization"):
                    st.rerun()
                return

    # Render sidebar controls and retrieve active filters
    filters = render_sidebar(spark, store)

    # Render top header strip
    render_header(store)

    if filters is None:
        return

    # Section selection using horizontal st.radio:
    # Streamlit evaluates every tab in st.tabs on every rerun, which triggers
    # Spark SQL evaluations across all views simultaneously. Using a horizontal st.radio
    # ensures only the currently active section executes, keeping reruns snappy and responsive.
    active_section = st.radio(
        "Select View",
        options=SECTIONS,
        key="section",
        horizontal=True,
    )

    if active_section == "Overview":
        render_overview_section(spark, store, filters)
    elif active_section == "Temperature":
        render_temperature_section(spark, store, filters)
    elif active_section == "Rainfall":
        render_rainfall_section(spark, store, filters)
    elif active_section == "Climate patterns":
        render_climate_section(spark, store, filters)
    elif active_section == "Data quality":
        render_quality_section(store)
    elif active_section == "Cleaning report":
        render_cleaning_section(store)
    elif active_section == "Data preview":
        render_preview_section(spark, store, filters)
    elif active_section == "Spark SQL":
        render_spark_sql_section(spark, store, filters)


if __name__ == "__main__":
    main()
