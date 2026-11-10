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

