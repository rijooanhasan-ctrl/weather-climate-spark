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


