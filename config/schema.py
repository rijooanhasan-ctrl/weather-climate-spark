"""
config/schema.py
================
Standard schema definitions, physical plausibility ranges, and column mapping
utilities for the Weather and Climate Pattern Discovery system.

NOTE: This module imports only from pyspark.sql.types and config.settings.
It does NOT create or depend on a running SparkSession.
"""

from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from config.settings import STANDARD_COLUMNS

# ── Required and Optional Column Subsets ───────────────────────────────────────
REQUIRED_COLUMNS: list[str] = [
    STANDARD_COLUMNS["date"],
    STANDARD_COLUMNS["location"],
    STANDARD_COLUMNS["temperature"],
    STANDARD_COLUMNS["rainfall"],
    STANDARD_COLUMNS["humidity"],
    STANDARD_COLUMNS["wind_speed"],
    STANDARD_COLUMNS["pressure"],
]

OPTIONAL_COLUMNS: list[str] = [
    "latitude",
    "longitude",
    "temp_max",
    "temp_min",
]

# ── Explicit Standard Schema ──────────────────────────────────────────────────
# Column order is canonical across the whole project.
# Every field is nullable to accommodate real-world meteorological missing values.
STANDARD_SCHEMA: StructType = StructType([
    StructField("date",        DateType(),   nullable=True),
    StructField("location",    StringType(), nullable=True),
    StructField("latitude",    DoubleType(), nullable=True),
    StructField("longitude",   DoubleType(), nullable=True),
    StructField("temperature", DoubleType(), nullable=True),
    StructField("temp_max",    DoubleType(), nullable=True),
    StructField("temp_min",    DoubleType(), nullable=True),
    StructField("rainfall",    DoubleType(), nullable=True),
    StructField("humidity",    DoubleType(), nullable=True),
    StructField("wind_speed",  DoubleType(), nullable=True),
    StructField("pressure",    DoubleType(), nullable=True),
])

# ── Standard Physical Units ───────────────────────────────────────────────────
UNITS: dict[str, str] = {
    "temperature": "°C",
    "temp_max":    "°C",
    "temp_min":    "°C",
    "rainfall":    "mm",
    "humidity":    "%",
    "wind_speed":  "km/h",
    "pressure":    "hPa",
}

# ── Physical Plausibility Ranges ──────────────────────────────────────────────
# Global meteorological limits used for data quality checks and filtering.
PLAUSIBLE_RANGES: dict[str, tuple[float, float]] = {
    "temperature": (-60.0, 60.0),
    "temp_max":    (-60.0, 60.0),
    "temp_min":    (-60.0, 60.0),
    "rainfall":    (0.0, 2000.0),
    "humidity":    (0.0, 100.0),
    "wind_speed":  (0.0, 400.0),
    "pressure":    (300.0, 1100.0),
}

# ── Missing Value Sentinel Values ─────────────────────────────────────────────
# Common sentinel codes present in legacy weather observation files.
SENTINEL_VALUES: list[int] = [-999, -9999, 9999]

# ── Common Header Aliases for Ingestion Mapping ───────────────────────────────
COLUMN_ALIASES: dict[str, list[str]] = {
    "date": [
        "datetime", "time", "day", "dt", "date_time", "timestamp", "observation_date"
    ],
    "location": [
        "city", "station", "place", "name", "city_name", "location_name", "station_name"
    ],
    "latitude": [
        "lat", "latitude_deg", "lat_deg", "y"
    ],
    "longitude": [
        "lon", "long", "lng", "longitude_deg", "lon_deg", "x"
    ],
    "temperature": [
        "temp", "temperature_c", "tavg", "meantemp", "avg_temp",
        "temperature_2m_mean", "mean_temp", "temp_mean", "avg_temperature"
    ],
    "temp_max": [
        "max_temp", "temperature_max", "tmax", "temp_max_c",
        "temperature_2m_max", "max_temperature", "maximum_temperature", "high_temp"
    ],
    "temp_min": [
        "min_temp", "temperature_min", "tmin", "temp_min_c",
        "temperature_2m_min", "min_temperature", "minimum_temperature", "low_temp"
    ],
    "rainfall": [
        "rain", "precip", "precipitation", "prcp", "precipitation_sum",
        "rainfall_mm", "rain_mm", "precipitation_mm", "daily_rainfall"
    ],
    "humidity": [
        "rh", "relative_humidity", "relative_humidity_2m_mean",
        "rel_humidity", "humidity_pct", "humidity_percent", "avg_humidity"
    ],
    "wind_speed": [
        "wind", "windspeed", "wspd", "wind_speed_10m_mean",
        "wind_speed_kmh", "mean_wind_speed", "wind_spd", "avg_wind_speed"
    ],
    "pressure": [
        "pres", "atm_pressure", "meanpressure", "pressure_msl_mean",
        "sea_level_pressure", "pressure_hpa", "slp", "barometric_pressure"
    ],
}


def suggest_column_mapping(columns: list[str]) -> dict[str, str | None]:
    """
    Map original CSV header names to standard project schema columns.

    Parameters
    ----------
    columns : list[str]
        Raw column header names from the source dataset.

    Returns
    -------
    dict[str, str | None]
        Mapping from standard_column to the matched original column name,
        or None if no match is found.

    Rules
    -----
    1. Names are normalized: stripped, lowercased, spaces/hyphens converted to '_'.
    2. Exact matches against standard column names take priority.
    3. Alias matches from COLUMN_ALIASES are checked next.
    4. An original column is never mapped to more than one standard column.
    """
    def _normalize(name: str) -> str:
        return name.strip().lower().replace(" ", "_").replace("-", "_")

    standard_order = [field.name for field in STANDARD_SCHEMA.fields]
    mapping: dict[str, str | None] = {col: None for col in standard_order}
    matched_orig: set[str] = set()

    # Pass 1: Exact matches against standard column names
    for std_col in standard_order:
        for orig in columns:
            if orig not in matched_orig and _normalize(orig) == std_col:
                mapping[std_col] = orig
                matched_orig.add(orig)
                break

    # Pass 2: Alias matches for unmapped standard columns
    for std_col in standard_order:
        if mapping[std_col] is not None:
            continue
        aliases = COLUMN_ALIASES.get(std_col, [])
        norm_aliases = {_normalize(a) for a in aliases}
        for orig in columns:
            if orig not in matched_orig and _normalize(orig) in norm_aliases:
                mapping[std_col] = orig
                matched_orig.add(orig)
                break

    return mapping

