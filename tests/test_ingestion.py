"""
tests/test_ingestion.py
=======================
Integration tests for CSV ingestion, delimiter detection, header mapping,
date format detection, schema standardization, and Spark SQL data quality validation.

Uses real historical weather observations sliced from data/raw/india_weather_daily.csv.
No synthetic or fake weather data is invented.
"""

import codecs
import csv
from datetime import datetime
from pathlib import Path
import sys

import pytest

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import PRIMARY_DATASET_PATH  # noqa: E402
from src.data_loader import DatasetLoadError, load_dataset  # noqa: E402


@pytest.mark.skipif(not PRIMARY_DATASET_PATH.exists(), reason="Primary dataset not found")
def test_ingest_primary_real_dataset(spark):
    """
    Test 1: Ingest primary real dataset (46,752 rows, 8 locations).
    Assert exit 0, validation valid, expected row count and location count.
    """
    result = load_dataset(spark, PRIMARY_DATASET_PATH)

    assert result.validation.is_valid is True
    assert len(result.validation.errors) == 0
    assert result.validation.info["total_rows"] == 46752
    assert len(result.validation.info["locations"]) == 8
    assert result.date_format_info["format"] == "yyyy-MM-dd"
    assert result.delimiter == ","


def test_ingest_renamed_headers_and_custom_date_format(spark, tmp_path, raw_dataset_slices):
    """
    Test 2: Ingest CSV with alias header names and dd/MM/yyyy date format.
    Assert auto-mapping resolves all columns, date format detected, validation passes.
    """
    slice_rows = raw_dataset_slices["bengaluru_rows"][:100]
    file_path = tmp_path / "renamed_weather.csv"

    alias_headers = [
        "Observation_Date", "City_Name", "Lat", "Lon", "Meantemp",
        "Max_Temp", "Min_Temp", "Rainfall_mm", "Relative_Humidity",
        "Mean_Wind_Speed", "Sea_Level_Pressure"
    ]

    with open(file_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(alias_headers)
        for row in slice_rows:
            # Reformat yyyy-MM-dd to dd/MM/yyyy
            d = datetime.strptime(row[0], "%Y-%m-%d").strftime("%d/%m/%Y")
            new_row = [d] + row[1:]
            writer.writerow(new_row)

    result = load_dataset(spark, file_path)

    assert result.validation.is_valid is True
    assert result.mapping["date"] == "Observation_Date"
    assert result.mapping["location"] == "City_Name"
    assert result.mapping["temperature"] == "Meantemp"
    assert result.mapping["temp_max"] == "Max_Temp"
    assert result.mapping["temp_min"] == "Min_Temp"
    assert result.mapping["rainfall"] == "Rainfall_mm"
    assert result.mapping["humidity"] == "Relative_Humidity"
    assert result.mapping["wind_speed"] == "Mean_Wind_Speed"
    assert result.mapping["pressure"] == "Sea_Level_Pressure"
    assert result.date_format_info["format"] in ["dd/MM/yyyy", "dd-MM-yyyy"]


def test_ingest_semicolon_delimited_with_bom(spark, tmp_path, raw_dataset_slices):
    """
    Test 3: Ingest semicolon-delimited CSV with UTF-8 BOM.
    Assert delimiter detected as ';', BOM handled transparently, validation passes.
    """
    slice_rows = raw_dataset_slices["mumbai_rows"][:60]
    file_path = tmp_path / "semicolon_bom.csv"

    with open(file_path, "wb") as f:
        f.write(codecs.BOM_UTF8)
        header_line = ";".join(raw_dataset_slices["header"]) + "\n"
        f.write(header_line.encode("utf-8"))
        for row in slice_rows:
            line = ";".join(row) + "\n"
            f.write(line.encode("utf-8"))

    result = load_dataset(spark, file_path)

    assert result.delimiter == ";"
    assert result.validation.is_valid is True
    assert result.validation.info["total_rows"] == 60


def test_ingest_missing_required_column(spark, tmp_path, raw_dataset_slices):
    """
    Test 4: Ingest CSV missing a required column (temperature omitted).
    Assert validation reports an error and is_valid is False.
    """
    header = [h for h in raw_dataset_slices["header"] if h != "temperature"]
    temp_idx = raw_dataset_slices["header"].index("temperature")
    rows = [[val for i, val in enumerate(r) if i != temp_idx] for r in raw_dataset_slices["bengaluru_rows"][:50]]

    file_path = tmp_path / "missing_temp.csv"
    with open(file_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)

    result = load_dataset(spark, file_path)

    assert result.validation.is_valid is False
    assert any("temperature" in err and "required" in err.lower() for err in result.validation.errors)


def test_ingest_with_injected_defects(spark, tmp_path, raw_dataset_slices):
    """
    Test 5: Ingest real weather data slice with injected defects:
    - Sentinel -999 in temperature
    - Temperature 95.0 (outside physical plausibility range [-60, 60])
    - Unparseable date string
    - Non-numeric string 'abc' in rainfall
    - Duplicate (date, location) record
    Assert validator catches and warns for every defect.
    """
    header = list(raw_dataset_slices["header"])
    rows = [list(r) for r in raw_dataset_slices["bengaluru_rows"][:60]]

    temp_idx = header.index("temperature")
    rain_idx = header.index("rainfall")

    # Injected defect 1: Sentinel value
    rows[5][temp_idx] = "-999"

    # Injected defect 2: Plausibility range outlier
    rows[10][temp_idx] = "95.0"

    # Injected defect 3: Invalid unparseable date
    rows[15][0] = "not-a-valid-date"

    # Injected defect 4: Non-numeric string in rainfall
    rows[20][rain_idx] = "abc"

    # Injected defect 5: Duplicate primary key
    rows.append(list(rows[0]))

    file_path = tmp_path / "defects_weather.csv"
    with open(file_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)

    result = load_dataset(spark, file_path)
    warnings = result.validation.warnings

    # Assert all 5 defects generated specific warnings
    assert any("sentinel" in w.lower() and "temperature" in w for w in warnings), "Missing sentinel warning"
    assert any("outside plausible range" in w.lower() and "temperature" in w for w in warnings), "Missing range warning"
    assert any("invalid or unparseable dates" in w.lower() for w in warnings), "Missing date parse warning"
    assert any("failed numeric parsing" in w.lower() and "rainfall" in w for w in warnings), "Missing numeric parse warning"
    assert any("duplicate" in w.lower() for w in warnings), "Missing duplicate warning"


def test_ingest_date_ambiguity_detection(spark, tmp_path, raw_dataset_slices):
    """
    Test 6: Ingest dates where all days <= 12 formatted as 01/02/2010.
    Assert is_ambiguous is True and note explains tie-break.
    """
    header = list(raw_dataset_slices["header"])
    # Take first 10 days of January 2010 (day numbers 1 to 10)
    rows = [list(r) for r in raw_dataset_slices["bengaluru_rows"][:10]]

    file_path = tmp_path / "ambiguous_dates.csv"
    with open(file_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for row in rows:
            dt = datetime.strptime(row[0], "%Y-%m-%d")
            # Format as 05/01/2010 (could be May 1 or Jan 5)
            row[0] = dt.strftime("%d/%m/%Y")
            writer.writerow(row)

    result = load_dataset(spark, file_path)

    assert result.date_format_info["is_ambiguous"] is True
    assert "Multiple" in result.date_format_info["note"]


def test_ingest_empty_and_header_only_files(spark, tmp_path):
    """
    Test 7: Empty CSV file raises DatasetLoadError; header-only CSV reports 0 rows error.
    """
    # 7a: 0-byte file
    empty_file = tmp_path / "empty.csv"
    empty_file.write_bytes(b"")

    with pytest.raises(DatasetLoadError, match="no header row"):
        load_dataset(spark, empty_file)

    # 7b: Header-only file
    header_file = tmp_path / "header_only.csv"
    header_file.write_text("date,location,temperature,rainfall,humidity,wind_speed,pressure\n", encoding="utf-8")

    result = load_dataset(spark, header_file)
    assert result.validation.is_valid is False
    assert any("0 rows" in err for err in result.validation.errors)


def test_ingest_single_location_with_default_fallback(spark, tmp_path, raw_dataset_slices):
    """
    Test 8: Ingest single-location CSV without location column, supplying default_location.
    Assert location is populated with the default and validation passes.
    """
    header = [h for h in raw_dataset_slices["header"] if h != "location"]
    loc_idx = raw_dataset_slices["header"].index("location")
    rows = [[val for i, val in enumerate(r) if i != loc_idx] for r in raw_dataset_slices["bengaluru_rows"][:50]]

    file_path = tmp_path / "single_city_no_loc_col.csv"
    with open(file_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)

    result = load_dataset(spark, file_path, default_location="Bengaluru")

    assert result.validation.is_valid is True
    locs = spark.sql(f"SELECT DISTINCT location FROM {result.standardized_view}").collect()
    assert len(locs) == 1
    assert locs[0]["location"] == "Bengaluru"
