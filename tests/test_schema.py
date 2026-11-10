"""
tests/test_schema.py
====================
Unit tests for standard schema definitions and column mapping suggestions.

Does NOT require Apache Spark.
"""

import sys
from pathlib import Path

# Ensure project root is on path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.schema import (  # noqa: E402
    OPTIONAL_COLUMNS,
    PLAUSIBLE_RANGES,
    REQUIRED_COLUMNS,
    STANDARD_SCHEMA,
    UNITS,
    suggest_column_mapping,
)


def test_standard_schema_field_order():
    """Verify field names in STANDARD_SCHEMA match the canonical project order."""
    expected_order = [
        "date",
        "location",
        "latitude",
        "longitude",
        "temperature",
        "temp_max",
        "temp_min",
        "rainfall",
        "humidity",
        "wind_speed",
        "pressure",
    ]
    actual_order = [field.name for field in STANDARD_SCHEMA.fields]
    assert actual_order == expected_order, f"Schema order mismatch: {actual_order} != {expected_order}"


def test_required_columns_present_in_schema():
    """Verify that every REQUIRED_COLUMNS entry exists in STANDARD_SCHEMA."""
    schema_fields = {field.name for field in STANDARD_SCHEMA.fields}
    for col in REQUIRED_COLUMNS:
        assert col in schema_fields, f"Required column '{col}' is missing from STANDARD_SCHEMA"


def test_optional_columns_present_in_schema():
    """Verify that every OPTIONAL_COLUMNS entry exists in STANDARD_SCHEMA."""
    schema_fields = {field.name for field in STANDARD_SCHEMA.fields}
    for col in OPTIONAL_COLUMNS:
        assert col in schema_fields, f"Optional column '{col}' is missing from STANDARD_SCHEMA"


def test_units_and_plausible_ranges_coverage():
    """Verify physical units and plausible ranges cover all expected numeric variables."""
    numeric_vars = ["temperature", "temp_max", "temp_min", "rainfall", "humidity", "wind_speed", "pressure"]
    for var in numeric_vars:
        assert var in UNITS, f"Missing unit for '{var}'"
        assert var in PLAUSIBLE_RANGES, f"Missing plausible range for '{var}'"
        low, high = PLAUSIBLE_RANGES[var]
        assert low < high, f"Invalid range bounds for '{var}': {low} >= {high}"


def test_suggest_column_mapping_with_standard_names():
    """Header list that already uses exact standard names should map 1-to-1."""
    standard_headers = [
        "date", "location", "latitude", "longitude", "temperature",
        "temp_max", "temp_min", "rainfall", "humidity", "wind_speed", "pressure"
    ]
    mapping = suggest_column_mapping(standard_headers)
    for col in standard_headers:
        assert mapping[col] == col, f"Expected '{col}' to map to itself, got '{mapping[col]}'"


def test_suggest_column_mapping_with_common_title_case_aliases():
    """Verify alias mapping with typical mixed-case shorthand headers."""
    aliases = ["Date", "City", "Temp", "Rain", "RH", "Wind", "Pres"]
    mapping = suggest_column_mapping(aliases)

    assert mapping["date"] == "Date"
    assert mapping["location"] == "City"
    assert mapping["temperature"] == "Temp"
    assert mapping["rainfall"] == "Rain"
    assert mapping["humidity"] == "RH"
    assert mapping["wind_speed"] == "Wind"
    assert mapping["pressure"] == "Pres"

    # Unprovided columns must be None
    assert mapping["latitude"] is None
    assert mapping["longitude"] is None
    assert mapping["temp_max"] is None
    assert mapping["temp_min"] is None


def test_suggest_column_mapping_with_noaa_style_aliases():
    """Verify alias mapping with lowercase abbreviated headers; unmapped become None."""
    headers = ["datetime", "station", "tavg", "prcp", "wspd"]
    mapping = suggest_column_mapping(headers)

    assert mapping["date"] == "datetime"
    assert mapping["location"] == "station"
    assert mapping["temperature"] == "tavg"
    assert mapping["rainfall"] == "prcp"
    assert mapping["wind_speed"] == "wspd"
    assert mapping["humidity"] is None
    assert mapping["pressure"] is None


def test_suggest_column_mapping_no_duplicate_assignments():
    """Ensure that no single original column is ever matched to multiple standard columns."""
    headers = ["date", "temp", "rain"]
    mapping = suggest_column_mapping(headers)

    # 'temp' should only map to 'temperature', not also to 'temp_max' or 'temp_min'
    assert mapping["temperature"] == "temp"
    assert mapping["temp_max"] is None
    assert mapping["temp_min"] is None

    # Verify all non-None mapped values in the dict are strictly unique
    matched_values = [v for v in mapping.values() if v is not None]
    assert len(matched_values) == len(set(matched_values)), (
        f"Duplicate original column matches found: {matched_values}"
    )

