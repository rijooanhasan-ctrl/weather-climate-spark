"""
src/dashboard/services.py
=========================
Core data services, active dataset management, and analysis wrappers for the
Streamlit dashboard foundation.

Architecture & Isolation Rules:
--------------------------------
1. This module MUST NOT import `streamlit`. It is pure Python + PySpark so it can
   be tested directly without Streamlit runtime dependencies.
2. Spark temporary views are session-wide. ActiveDatasetStore manages ONE active
   dataset view at a time for this session. Switching datasets uncaches the old
   view first to ensure no stale data or cached partitions are ever served.
3. All data analysis queries run purely in Spark SQL via `src.queries`.
"""

from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from pathlib import Path
import threading
import time
from typing import Any

import pandas as pd
from pyspark.sql import DataFrame, SparkSession

from config.schema import (
    OPTIONAL_COLUMNS,
    REQUIRED_COLUMNS,
    STANDARD_COLUMNS,
    suggest_column_mapping,
)
from config.settings import (
    BUILTIN_DATASET_LABEL,
    CLEAN_VIEW_NAME,
    NO_COLUMN_LABEL,
    PREVIEW_ROWS_DEFAULT,
    PREVIEW_ROWS_MAX,
    PRIMARY_DATASET_PATH,
    UPLOAD_DIR,
)
from src.cleaner import CleaningReport
from src.data_loader import DatasetLoadError, LoadResult, load_dataset, read_csv_header
from src.pipeline import PipelineError, PreparedData, prepare
from src.queries.base import Filters, QueryResult, build_where
from src.queries.kpis import kpi_summary
from src.queries.registry import ANALYSES_BY_KEY, get_analysis
from src.dashboard.insights import Insight, generate_insights
from src.dashboard.result_cache import ResultCache
from src.sql_utils import quote_identifier
from src.upload import UploadError, save_uploaded_file
from src.validator import ValidationReport


# =============================================================================
# 1. ActiveDatasetStore Singleton
# =============================================================================

class ActiveDatasetStore:
    """
    Process-wide singleton holding the currently active prepared dataset.

    Spark temporary views are session-wide, so the application runtime maintains
    ONE active dataset view at a time (acceptable and intended for a single-user
    local analytical tool).

    When a new dataset is loaded, `set_active` explicitly uncaches the previous
    view in the Spark catalog via `spark.catalog.uncacheTable` so subsequent
    queries never serve stale data or mixed partition states.
    """

    _instance: ActiveDatasetStore | None = None
    _lock = threading.Lock()

    def __new__(cls) -> ActiveDatasetStore:
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._prepared = None
                cls._instance._label = ""
                cls._instance._source = ""
                cls._instance._version = 1
                cls._instance._load_seconds = 0.0
                cls._instance._mutation_lock = threading.Lock()
            return cls._instance

    @classmethod
    def get_instance(cls) -> ActiveDatasetStore:
        """Access the singleton instance."""
        return cls()

    @property
    def prepared(self) -> PreparedData | None:
        """The currently active PreparedData instance, or None."""
        return self._prepared

    @property
    def label(self) -> str:
        """Human-readable label for the dataset."""
        return self._label

    @property
    def source(self) -> str:
        """Data source: 'builtin' or 'upload'."""
        return self._source

    @property
    def version(self) -> int:
        """Integer cache-busting version counter incremented on every successful load."""
        return self._version

    @property
    def load_seconds(self) -> float:
        """Duration in seconds taken to prepare and cache the dataset."""
        return self._load_seconds

    def get(self) -> PreparedData | None:
        """Return the active PreparedData, or None."""
        return self._prepared

    def reset(self) -> None:
        """Reset the active dataset state (primarily used in test teardown)."""
        with self._mutation_lock:
            self._prepared = None
            self._label = ""
            self._source = ""
            self._load_seconds = 0.0

    def is_ready(self, spark: SparkSession | None = None) -> bool:
        """
        Verify that an active dataset is loaded and its clean view exists in Spark.
        Costs 0 Spark jobs.
        """
        if self._prepared is None or not self._prepared.clean_view:
            return False
        if spark is None:
            return True
        try:
            return spark.catalog.tableExists(self._prepared.clean_view)
        except Exception:
            return False

    def set_active(
        self,
        spark: SparkSession,
        prepared: PreparedData,
        label: str,
        source: str,
        load_seconds: float,
    ) -> None:
        """
        Replace the active dataset. Uncaches previous view, increments version,
        and invalidates the query result cache.
        """
        with self._mutation_lock:
            # Uncache previous view if it existed to prevent stale memory caching
            if self._prepared is not None and self._prepared.clean_view:
                try:
                    if spark.catalog.tableExists(self._prepared.clean_view):
                        spark.catalog.uncacheTable(self._prepared.clean_view)
                except Exception:
                    pass

            self._prepared = prepared
            self._label = label
            self._source = source
            self._load_seconds = float(load_seconds)
            self._version += 1
            ResultCache.get_instance().clear()


# =============================================================================
# 2. Dataset Loading Services
# =============================================================================

def load_builtin(
    spark: SparkSession,
    progress: Callable[[str, int, int], None] | None = None,
) -> PreparedData:
    """
    Prepare and cache the primary built-in 8-city historical weather dataset.

    Registers result in ActiveDatasetStore with label BUILTIN_DATASET_LABEL.
    """
    t0 = time.perf_counter()
    prepared = prepare(
        spark=spark,
        path=PRIMARY_DATASET_PATH,
        view_name=CLEAN_VIEW_NAME,
        progress=progress,
    )
    duration = time.perf_counter() - t0
    ActiveDatasetStore.get_instance().set_active(
        spark=spark,
        prepared=prepared,
        label=BUILTIN_DATASET_LABEL,
        source="builtin",
        load_seconds=duration,
    )
    start_prefetch(spark)
    return prepared


@dataclass
class UploadInspection:
    """Inspection metadata extracted from an uploaded CSV before ingestion."""
    saved_name: str
    delimiter: str
    original_columns: list[str]
    suggested_mapping: dict[str, str | None]
    unmapped_required: list[str]
    location_unmapped: bool


def inspect_upload(file_bytes: bytes, original_name: str) -> UploadInspection:
    """
    Safely persist uploaded file bytes, sniff delimiter and headers, and suggest mappings.

    Exceptions:
    -----------
    UploadError and DatasetLoadError are NOT swallowed here; they propagate
    directly so the UI can render friendly, descriptive messages.
    """
    saved_path = save_uploaded_file(file_bytes=file_bytes, original_name=original_name)
    columns, raw_delim = read_csv_header(saved_path)

    delimiter_names = {
        ",": "comma",
        ";": "semicolon",
        "\t": "tab",
        "|": "pipe",
    }
    delim_display = delimiter_names.get(raw_delim, raw_delim)
    suggested = suggest_column_mapping(columns)
    unmapped_req = [col for col in REQUIRED_COLUMNS if suggested.get(col) is None]
    loc_unmapped = (suggested.get("location") is None)

    return UploadInspection(
        saved_name=saved_path.name,
        delimiter=delim_display,
        original_columns=columns,
        suggested_mapping=suggested,
        unmapped_required=unmapped_req,
        location_unmapped=loc_unmapped,
    )


def load_upload(
    spark: SparkSession,
    saved_name: str,
    mapping: dict[str, str | None],
    default_location: str | None = None,
    date_format: str | None = None,
    fill_gaps: bool = True,
    progress: Callable[[str, int, int], None] | None = None,
) -> PreparedData:
    """
    Validate and ingest an uploaded CSV into the active dataset store.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    saved_name : str
        Base filename previously saved inside UPLOAD_DIR.
    mapping : dict[str, str | None]
        User-confirmed mapping from standard column names to CSV header names.
    default_location : str, optional
        Fallback city name if location column is unmapped.
    date_format : str, optional
        Date format override.
    fill_gaps : bool, default True
        Whether to linearly interpolate short gaps (<= 3 days).
    progress : Callable[[str, int, int], None], optional
        Optional callback reporting pipeline preparation progress.
    """
    # Reject directory traversal / path separators
    if "/" in saved_name or "\\" in saved_name or ".." in saved_name or Path(saved_name).name != saved_name:
        raise ValueError(f"Invalid file name '{saved_name}'. Path separators and traversal are not allowed.")

    file_path = (UPLOAD_DIR / saved_name).resolve()
    if not file_path.is_file() or file_path.parent != UPLOAD_DIR.resolve():
        raise ValueError(f"Uploaded file not found inside upload directory: '{saved_name}'")

    # Clean mapping (remove None or NO_COLUMN_LABEL)
    clean_map: dict[str, str | None] = {}
    for std_col, orig_col in mapping.items():
        if orig_col and orig_col != NO_COLUMN_LABEL:
            clean_map[std_col] = orig_col
        else:
            clean_map[std_col] = None

    t0 = time.perf_counter()
    clean_options = {"fill_gaps": fill_gaps}

    # prepare() raises PipelineError if validation fails, keeping previous dataset intact
    prepared = prepare(
        spark=spark,
        path=file_path,
        mapping=clean_map,
        date_format=date_format,
        default_location=default_location,
        clean_options=clean_options,
        view_name=CLEAN_VIEW_NAME,
        progress=progress,
    )
    duration = time.perf_counter() - t0

    ActiveDatasetStore.get_instance().set_active(
        spark=spark,
        prepared=prepared,
        label=f"Uploaded: {saved_name}",
        source="upload",
        load_seconds=duration,
    )
    start_prefetch(spark)
    return prepared


# =============================================================================
# 3. Filter Options and Filter Builder
# =============================================================================

def get_filter_options(spark: SparkSession, view: str = CLEAN_VIEW_NAME) -> dict[str, Any]:
    """
    Query available filter choices directly from the clean dataset view using Spark SQL.

    Returns
    -------
    dict with:
    - locations: sorted list of distinct location names
    - min_date: earliest date string (YYYY-MM-DD)
    - max_date: latest date string (YYYY-MM-DD)
    - years: sorted list of integer years
    - seasons: distinct seasons in canonical meteorological order
    """
    v = quote_identifier(view)

    # 1. Locations
    loc_rows = spark.sql(f"SELECT DISTINCT location FROM {v} WHERE location IS NOT NULL ORDER BY location").collect()
    locations = [r["location"] for r in loc_rows]

    # 2. Date extremes
    date_row = spark.sql(f"SELECT CAST(MIN(date) AS STRING) AS min_d, CAST(MAX(date) AS STRING) AS max_d FROM {v}").collect()[0]
    min_date = date_row["min_d"]
    max_date = date_row["max_d"]

    # 3. Years
    year_rows = spark.sql(f"SELECT DISTINCT year FROM {v} WHERE year IS NOT NULL ORDER BY year").collect()
    years = [int(r["year"]) for r in year_rows]

    # 4. Seasons (fixed meteorological sequence)
    season_order = ["Winter", "Summer", "Pre-monsoon", "Monsoon", "Post-monsoon"]
    season_rows = spark.sql(f"SELECT DISTINCT season FROM {v} WHERE season IS NOT NULL").collect()
    found_seasons = {r["season"] for r in season_rows if r["season"]}
    seasons = [s for s in season_order if s in found_seasons]

    return {
        "locations": locations,
        "min_date": min_date,
        "max_date": max_date,
        "years": years,
        "seasons": seasons,
    }


def build_filters(
    locations: list[str] | None = None,
    start: date | datetime | str | None = None,
    end: date | datetime | str | None = None,
    years: list[int] | None = None,
    seasons: list[str] | None = None,
    options: dict[str, Any] | None = None,
) -> Filters:
    """
    Construct a validated Filters object for analytical queries.

    Validation Rules:
    - An empty locations list `[]` stays `[]` (evaluates to zero rows).
    - If `locations` is None or contains all available locations, it represents no restriction.
    - Start date after end date raises ValueError with a descriptive message.
    - Empty years or seasons lists mean no restriction (normalized to None).
    """
    start_str = start.isoformat() if isinstance(start, (date, datetime)) else (str(start) if start else None)
    end_str = end.isoformat() if isinstance(end, (date, datetime)) else (str(end) if end else None)

    if start_str and end_str and start_str > end_str:
        raise ValueError(f"Start date ({start_str}) cannot be after end date ({end_str}).")

    norm_years = years if years else None
    norm_seasons = seasons if seasons else None

    return Filters(
        locations=locations,
        start_date=start_str,
        end_date=end_str,
        years=norm_years,
        seasons=norm_seasons,
    )


# =============================================================================
# 4. KPIs and Data Preview
# =============================================================================

def get_kpis(
    spark: SparkSession,
    filters: Filters | None = None,
    view: str = CLEAN_VIEW_NAME,
) -> dict[str, Any]:
    """
    Compute headline KPI metrics using Spark SQL via `kpi_summary`.

    Returns plain Python values (None for NULL), plus the SQL text and parameters.
    When the selection contains zero rows, returns total_records: 0 and None
    for all other metrics without raising an exception.
    """
    store = ActiveDatasetStore.get_instance()
    cache = ResultCache.get_instance()
    cache_key = ResultCache.build_key(
        version=store.version,
        filters=filters,
        analysis_key="kpis_dict",
        params=None,
    )

    def _compute() -> dict[str, Any]:
        result = kpi_summary(spark, filters=filters, view=view)
        rows = result.df.collect()

        if not rows or rows[0]["total_records"] == 0 or rows[0]["total_records"] is None:
            return {
                "total_records": 0,
                "locations": None,
                "first_date": None,
                "last_date": None,
                "avg_temp": None,
                "max_temp": None,
                "max_temp_date": None,
                "max_temp_location": None,
                "min_temp": None,
                "min_temp_date": None,
                "min_temp_location": None,
                "total_rainfall_mm": None,
                "avg_annual_rainfall_per_location": None,
                "avg_humidity": None,
                "avg_wind_speed": None,
                "avg_pressure": None,
                "wettest_year": None,
                "hottest_year": None,
                "interpolated_value_count": 0,
                "sql": result.sql,
                "parameters": result.parameters,
            }

        row = rows[0].asDict()
        return {
            "total_records": int(row.get("total_records") or 0),
            "locations": int(row["locations"]) if row.get("locations") is not None else None,
            "first_date": row.get("first_date"),
            "last_date": row.get("last_date"),
            "avg_temp": float(row["avg_temp"]) if row.get("avg_temp") is not None else None,
            "max_temp": float(row["max_temp"]) if row.get("max_temp") is not None else None,
            "max_temp_date": row.get("max_temp_date"),
            "max_temp_location": row.get("max_temp_location"),
            "min_temp": float(row["min_temp"]) if row.get("min_temp") is not None else None,
            "min_temp_date": row.get("min_temp_date"),
            "min_temp_location": row.get("min_temp_location"),
            "total_rainfall_mm": float(row["total_rainfall_mm"]) if row.get("total_rainfall_mm") is not None else None,
            "avg_annual_rainfall_per_location": float(row["avg_annual_rainfall_per_location"]) if row.get("avg_annual_rainfall_per_location") is not None else None,
            "avg_humidity": float(row["avg_humidity"]) if row.get("avg_humidity") is not None else None,
            "avg_wind_speed": float(row["avg_wind_speed"]) if row.get("avg_wind_speed") is not None else None,
            "avg_pressure": float(row["avg_pressure"]) if row.get("avg_pressure") is not None else None,
            "wettest_year": int(row["wettest_year"]) if row.get("wettest_year") is not None else None,
            "hottest_year": int(row["hottest_year"]) if row.get("hottest_year") is not None else None,
            "interpolated_value_count": int(row.get("interpolated_value_count") or 0),
            "sql": result.sql,
            "parameters": result.parameters,
        }

    return cache.get_or_compute(cache_key, _compute)


def get_preview(
    spark: SparkSession,
    filters: Filters | None = None,
    n: int = PREVIEW_ROWS_DEFAULT,
    view: str = CLEAN_VIEW_NAME,
) -> tuple[pd.DataFrame, str]:
    """
    Retrieve preview rows of standard columns from the clean view.

    Parameters
    ----------
    n : int
        Number of preview rows (clamped to PREVIEW_ROWS_MAX).

    Returns
    -------
    tuple[pd.DataFrame, str]
        (Pandas DataFrame of preview records, generated Spark SQL query).
    """
    clamped_n = max(1, min(int(n), PREVIEW_ROWS_MAX))
    where_clause = build_where(filters)
    cols = "date, location, latitude, longitude, temperature, temp_max, temp_min, rainfall, humidity, wind_speed, pressure"
    sql = f"""
    SELECT
        {cols}
    FROM {quote_identifier(view)}
    WHERE {where_clause}
    ORDER BY location, date
    LIMIT {clamped_n}
    """
    df = spark.sql(sql)
    pdf = df.toPandas()
    return pdf, sql.strip()


# =============================================================================
# 5. Report Summarizers (for Quality and Cleaning UI tabs)
# =============================================================================

def summarize_validation(report: ValidationReport) -> dict[str, Any]:
    """Format ValidationReport into display structures and tables."""
    # Column statistics table
    col_rows = []
    tot_rows = report.info.get("total_rows", 0)
    null_counts = report.info.get("null_counts", {})
    ranges = report.info.get("ranges", {})

    units_map = {
        "temperature": "°C",
        "temp_max": "°C",
        "temp_min": "°C",
        "rainfall": "mm",
        "humidity": "%",
        "wind_speed": "km/h",
        "pressure": "hPa",
        "latitude": "°",
        "longitude": "°",
    }

    all_cols = REQUIRED_COLUMNS + OPTIONAL_COLUMNS
    for col in all_cols:
        cnt = null_counts.get(col, 0)
        pct = (cnt / tot_rows * 100.0) if tot_rows > 0 else 0.0
        r_info = ranges.get(col, {})
        col_rows.append({
            "Column": col,
            "Null Count": cnt,
            "Null Rate (%)": f"{pct:.1f}%",
            "Observed Min": r_info.get("min", "N/A"),
            "Observed Max": r_info.get("max", "N/A"),
            "Unit": units_map.get(col, "-"),
        })

    col_df = pd.DataFrame(col_rows)

    # Location coverage table
    loc_cov = report.info.get("location_coverage", {})
    loc_rows = []
    for loc, info in sorted(loc_cov.items()):
        loc_rows.append({
            "Location": loc,
            "Row Count": info.get("count", 0),
            "Earliest Date": info.get("min_date", "-"),
            "Latest Date": info.get("max_date", "-"),
        })
    loc_df = pd.DataFrame(loc_rows) if loc_rows else pd.DataFrame(columns=["Location", "Row Count", "Earliest Date", "Latest Date"])

    sentinel_cnt = sum(report.info.get("sentinel_counts", {}).values()) if isinstance(report.info.get("sentinel_counts"), dict) else 0
    out_of_range_cnt = sum(report.info.get("out_of_range_counts", {}).values()) if isinstance(report.info.get("out_of_range_counts"), dict) else 0
    parse_fail_cnt = sum(report.info.get("numeric_parse_failures", {}).values()) if isinstance(report.info.get("numeric_parse_failures"), dict) else 0
    dup_keys_cnt = report.info.get("duplicate_keys_count", 0)

    return {
        "is_valid": report.is_valid,
        "error_count": len(report.errors),
        "errors": report.errors,
        "warnings": report.warnings,
        "total_sentinels": sentinel_cnt,
        "total_out_of_range": out_of_range_cnt,
        "total_parse_failures": parse_fail_cnt,
        "total_duplicate_keys": dup_keys_cnt,
        "column_stats": col_df,
        "column_table": col_df,
        "location_coverage": loc_df,
        "info": report.info,
    }


def summarize_cleaning(report: CleaningReport) -> dict[str, Any]:
    """Format CleaningReport into display structures and tables."""
    step_rows = []
    for s in report.steps:
        changed = s.get("values_changed", {})
        changed_str = ", ".join(f"{k}: {v}" for k, v in changed.items()) if changed else "none"
        step_rows.append({
            "Step": s.get("step"),
            "Description": s.get("description"),
            "Rows Before": s.get("rows_before"),
            "Rows After": s.get("rows_after"),
            "Rows Removed": s.get("rows_removed"),
            "Values Changed": changed_str,
        })
    steps_df = pd.DataFrame(step_rows) if step_rows else pd.DataFrame()

    return {
        "steps_table": steps_df,
        "summary": report.summary,
        "warnings": report.warnings,
        "steps": report.steps,
    }


# =============================================================================
# 7. Analysis Query Service & Natural Language Insights
# =============================================================================

@dataclass
class AnalysisFrame:
    """Structure encapsulating executed analysis query data, SQL, and error state."""
    key: str
    title: str
    df: pd.DataFrame
    sql: str
    parameters: dict[str, Any]
    error: str | None = None


def get_analysis_frame(
    spark: SparkSession,
    key: str,
    filters: Filters | None = None,
    params: dict[str, Any] | None = None,
    view: str = CLEAN_VIEW_NAME,
) -> AnalysisFrame:
    """
    Execute a registered analysis query, convert result to a pandas DataFrame,
    and return an AnalysisFrame structure.

    Leverages ResultCache with single-flight execution so duplicate calls
    share identical results across concurrent threads without duplicate Spark jobs.
    Catches row-limit safety exceptions and filter validation errors, returning
    an empty DataFrame and a human-friendly error message.
    """
    store = ActiveDatasetStore.get_instance()
    cache = ResultCache.get_instance()
    cache_key = ResultCache.build_key(
        version=store.version,
        filters=filters,
        analysis_key=key,
        params=params,
    )

    def _compute() -> AnalysisFrame:
        try:
            item = get_analysis(key)
        except ValueError as ve:
            return AnalysisFrame(
                key=key,
                title=key,
                df=pd.DataFrame(),
                sql="",
                parameters={},
                error=str(ve),
            )

        title = item.get("title", key)
        fn = item["function"]
        kw = dict(params or {})

        try:
            res: QueryResult = fn(spark, filters=filters, view=view, **kw)
            df = res.to_pandas()
            return AnalysisFrame(
                key=key,
                title=title,
                df=df,
                sql=res.sql,
                parameters=res.parameters,
                error=None,
            )
        except ValueError as ve:
            # Row limit exceeded or filter range invalid
            return AnalysisFrame(
                key=key,
                title=title,
                df=pd.DataFrame(),
                sql="",
                parameters=kw,
                error=str(ve),
            )
        except Exception as exc:
            return AnalysisFrame(
                key=key,
                title=title,
                df=pd.DataFrame(),
                sql="",
                parameters=kw,
                error=f"Error executing analysis '{key}': {exc}",
            )

    return cache.get_or_compute(cache_key, _compute)


def fetch_many(
    spark: SparkSession,
    keys: list[str],
    filters: Filters | None = None,
    params: dict[str, Any] | None = None,
    view: str = CLEAN_VIEW_NAME,
    max_workers: int = 4,
) -> dict[str, AnalysisFrame]:
    """
    Execute multiple registered analysis queries concurrently using a thread pool.

    Each query is isolated so errors in one query do not affect others.
    Leverages ResultCache for instant cache hits and single-flight deduplication.
    """
    results: dict[str, AnalysisFrame] = {}

    def _fetch_one(k: str) -> tuple[str, AnalysisFrame]:
        frame = get_analysis_frame(spark, k, filters=filters, params=params, view=view)
        return k, frame

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_fetch_one, k) for k in keys]
        for f in as_completed(futures):
            try:
                k, frame = f.result()
                results[k] = frame
            except Exception:
                pass

    return results


def get_insights(
    spark: SparkSession,
    filters: Filters | None = None,
    view: str = CLEAN_VIEW_NAME,
) -> list[Insight]:
    """
    Synthesize natural language climate insights across multiple analysis queries.
    Uses fetch_many to run prerequisite analytical queries concurrently.
    """
    kpis = get_kpis(spark, filters=filters, view=view)
    if kpis.get("total_records", 0) == 0:
        return []

    needed_keys = [
        "temp_location_ranking",
        "temp_trend",
        "rain_location_ranking",
        "rain_seasonal",
        "rain_unusual_months",
        "extreme_events_yearly",
        "heatwave_spells",
        "correlations",
        "rain_yearly",
        "temp_yearly",
    ]

    fetched = fetch_many(spark, needed_keys, filters=filters, view=view, max_workers=4)
    frames: dict[str, pd.DataFrame] = {}
    for key in needed_keys:
        frame = fetched.get(key)
        if frame and frame.error is None and not frame.df.empty:
            frames[key] = frame.df

    # Also fetch anomaly correlations to check seasonal cycle divergence
    anom_corr_frame = get_analysis_frame(
        spark, "correlations", filters=filters, params={"mode": "anomaly"}, view=view
    )
    corr_anomaly_df = (
        anom_corr_frame.df if anom_corr_frame.error is None and not anom_corr_frame.df.empty else None
    )

    return generate_insights(frames, kpis, corr_anomaly_df=corr_anomaly_df)


# =============================================================================
# 8. Background Cache Prefetch Service
# =============================================================================

_prefetch_done: int = 0
_prefetch_total: int = 22
_prefetch_lock = threading.Lock()
_prefetch_active: bool = False


def prefetch_status() -> tuple[int, int]:
    """Return (done_count, total_count) for background cache prefetching."""
    with _prefetch_lock:
        return (_prefetch_done, _prefetch_total)


def is_prefetching() -> bool:
    """Return True if background cache prefetching is actively running."""
    with _prefetch_lock:
        return _prefetch_active


def start_prefetch(
    spark: SparkSession,
    filters: Filters | None = None,
    view: str = CLEAN_VIEW_NAME,
) -> None:
    """
    Launch a background daemon thread that eagerly warms ResultCache.
    Does not block caller. Computes KPIs, insight frames, then sections.
    """
    global _prefetch_done, _prefetch_total, _prefetch_active
    with _prefetch_lock:
        _prefetch_done = 0
        _prefetch_total = 22
        _prefetch_active = True

    def _worker():
        global _prefetch_done, _prefetch_active
        try:
            # 1. Warm KPIs
            get_kpis(spark, filters=filters, view=view)
            with _prefetch_lock:
                _prefetch_done += 1

            # 2. Warm Insight queries
            insight_keys = [
                "temp_location_ranking",
                "temp_trend",
                "rain_location_ranking",
                "rain_seasonal",
                "rain_unusual_months",
                "extreme_events_yearly",
                "heatwave_spells",
                "correlations",
                "rain_yearly",
                "temp_yearly",
            ]
            for ik in insight_keys:
                get_analysis_frame(spark, ik, filters=filters, view=view)
                with _prefetch_lock:
                    _prefetch_done += 1

            # Anomaly correlations
            get_analysis_frame(
                spark, "correlations", filters=filters, params={"mode": "anomaly"}, view=view
            )

            # 3. Warm remaining registry queries
            remaining_keys = [
                k for k in ANALYSES_BY_KEY
                if k not in insight_keys and k != "kpi_summary"
            ]
            for rk in remaining_keys:
                get_analysis_frame(spark, rk, filters=filters, view=view)
                with _prefetch_lock:
                    _prefetch_done += 1
        except Exception:
            pass
        finally:
            with _prefetch_lock:
                _prefetch_active = False

    t = threading.Thread(target=_worker, daemon=True, name="PrefetchDaemon")
    t.start()

