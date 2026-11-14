"""
config/settings.py
==================
Project-wide constants.

All paths are built with pathlib so the code works identically on
Windows, macOS, and Linux – no hardcoded slashes or drive letters.
"""

from pathlib import Path

# ── Directory layout ──────────────────────────────────────────────────────────
# BASE_DIR is the project root (the folder that contains this config/ package).
BASE_DIR: Path = Path(__file__).resolve().parent.parent

# Raw CSV files dropped by the user go here (not committed to git).
RAW_DATA_DIR: Path = BASE_DIR / "data" / "raw"

# Spark-processed output files land here (not committed to git).
PROCESSED_DATA_DIR: Path = BASE_DIR / "data" / "processed"

# ── Spark application name ────────────────────────────────────────────────────
APP_NAME: str = "WeatherClimatePatternDiscovery"

# ── Standard column names ─────────────────────────────────────────────────────
# Using a dictionary as a single source of truth so every module that
# references column names imports from here rather than using raw strings.
STANDARD_COLUMNS: dict[str, str] = {
    "date":        "date",
    "location":    "location",
    "temperature": "temperature",
    "rainfall":    "rainfall",
    "humidity":    "humidity",
    "wind_speed":  "wind_speed",
    "pressure":    "pressure",
}

# ── Default Cities for Dataset Acquisition ────────────────────────────────────
# Exactly 8 diverse meteorological regions across India
DEFAULT_CITIES: list[dict[str, str | float]] = [
    {"name": "Bengaluru", "latitude": 12.9716, "longitude": 77.5946},
    {"name": "Mumbai",    "latitude": 19.0760, "longitude": 72.8777},
    {"name": "Delhi",     "latitude": 28.6139, "longitude": 77.2090},
    {"name": "Chennai",   "latitude": 13.0827, "longitude": 80.2707},
    {"name": "Kolkata",   "latitude": 22.5726, "longitude": 88.3639},
    {"name": "Hyderabad", "latitude": 17.3850, "longitude": 78.4867},
    {"name": "Jaipur",    "latitude": 26.9124, "longitude": 75.7873},
    {"name": "Shillong",  "latitude": 25.5788, "longitude": 91.8933},
]

# ── Primary Dataset Date Range & Storage ──────────────────────────────────────
DATASET_START_DATE: str = "2010-01-01"
DATASET_END_DATE: str = "2025-12-31"
PRIMARY_DATASET_PATH: Path = RAW_DATA_DIR / "india_weather_daily.csv"
RAW_VIEW_NAME: str = "weather_raw"

# ── Upload and Ingestion Configuration ────────────────────────────────────────
UPLOAD_DIR: Path = BASE_DIR / "data" / "uploads"
MAX_UPLOAD_MB: int = 200
ALLOWED_UPLOAD_EXTENSIONS: list[str] = [".csv"]
STANDARDIZED_VIEW_NAME: str = "weather_standardized"
SUPPORTED_DELIMITERS: list[str] = [",", ";", "\t", "|"]
DATE_FORMAT_CANDIDATES: list[str] = [
    "yyyy-MM-dd",
    "dd-MM-yyyy",
    "MM-dd-yyyy",
    "yyyy/MM/dd",
    "dd/MM/yyyy",
    "MM/dd/yyyy",
    "dd.MM.yyyy",
    "yyyyMMdd",
]

# ── Validation Thresholds ─────────────────────────────────────────────────────
MIN_ROWS: int = 30
DATE_PARSE_ERROR_BELOW: float = 0.50
NUMERIC_PARSE_WARN_BELOW: float = 0.95
NULL_WARN_ABOVE: float = 0.20
OUT_OF_RANGE_UNIT_WARN_ABOVE: float = 0.50

# ── Cleaning Pipeline Configuration ───────────────────────────────────────────
CLEAN_VIEW_NAME: str = "weather_clean"
PROCESSED_CLEAN_PATH: Path = PROCESSED_DATA_DIR / "weather_clean.csv"
CLEANING_REPORT_PATH: Path = PROCESSED_DATA_DIR / "cleaning_report.json"
MAX_FILL_GAP_DAYS: int = 3
FILL_COLUMNS: list[str] = [
    "temperature",
    "temp_max",
    "temp_min",
    "humidity",
    "wind_speed",
    "pressure",
]
LOCATION_MAX_NULL_RATE: float = 0.30

# ── Phase 5 Analysis Engine Constants ─────────────────────────────────────────
# India Meteorological Department (IMD) daily rainfall category thresholds (mm)
RAINY_DAY_MM: float = 2.5
HEAVY_RAIN_MM: float = 64.5
VERY_HEAVY_RAIN_MM: float = 115.6
EXTREMELY_HEAVY_RAIN_MM: float = 204.5

# Extreme event percentile definitions and Spark SQL percentile_approx accuracy
HOT_PERCENTILE: float = 0.95
COLD_PERCENTILE: float = 0.05
WET_DAY_EXTREME_PERCENTILE: float = 0.95
PERCENTILE_ACCURACY: int = 10000

# Climate pattern thresholds
HEATWAVE_MIN_DAYS: int = 3
RAINFALL_ZSCORE_THRESHOLD: float = 2.0
MIN_YEARS_FOR_ZSCORE: int = 8
COMPLETE_YEAR_MIN_COVERAGE: float = 0.95
TREND_MIN_YEARS: int = 5
TOP_N_DEFAULT: int = 10

# Multi-variate correlation variables
CORRELATION_VARIABLES: list[str] = [
    "temperature",
    "rainfall",
    "humidity",
    "wind_speed",
    "pressure",
]

# Query execution and safety limits
ANALYSIS_RESULT_MAX_ROWS: int = 5000

# ── Phase 6 Dashboard Constants ───────────────────────────────────────────────
DASHBOARD_TITLE: str = "Weather and Climate Pattern Discovery"
DASHBOARD_SUBTITLE: str = "Powered by Apache Spark SQL"
PREVIEW_ROWS_DEFAULT: int = 100
PREVIEW_ROWS_MAX: int = 1000
BUILTIN_DATASET_LABEL: str = "Built-in dataset: 8 Indian cities, 2010-2025"
NO_COLUMN_LABEL: str = "(not in file)"

# ── Spark Optimization Settings ───────────────────────────────────────────────
# Configurations are only adopted if they achieve a 3-run median speedup >= 10%
# and verified identical results across all 22 analysis queries.
# Phase 9 3x retest: baseline median 24.840s vs codegen_off 22.843s (+8.04% gain < 10% threshold).
# Reverted to default Spark codegen configuration per Phase 9 decision rule.
SPARK_CONF: dict[str, str] = {}

# ── Phase 7 Dashboard & Chart Constants ───────────────────────────────────────
PLOTLY_TEMPLATE: str = "plotly_dark"
CHART_HEIGHT: int = 420
CHART_HEIGHT_TALL: int = 560
MAX_LOCATIONS_IN_CHART: int = 12
DIVERGING_COLORSCALE: str = "RdBu_r"
SEQUENTIAL_COLORSCALE: str = "Blues"
SEASON_ORDER: list[str] = ["Winter", "Summer", "Monsoon", "Post-monsoon"]
MONTH_NAMES: list[str] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
]
INSIGHT_MIN_ABS_CORR: float = 0.5
INSIGHT_ANOMALY_DIFF: float = 0.3
INSIGHT_MAX_ITEMS: int = 12






