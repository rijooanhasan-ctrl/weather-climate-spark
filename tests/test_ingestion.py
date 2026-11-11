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
    - 3 rows with -999 in rainfall
    - 2 unparseable temperature cells ("abc")
    - 2 duplicated (date, location) keys
    - 4 blank humidity cells
    - 1 out-of-range temperature (150)
    Assert validator catches and warns for every defect, counts match exactly,
    and standardized row count equals raw row count.
    """
    header = list(raw_dataset_slices["header"])
    rows = [list(r) for r in raw_dataset_slices["bengaluru_rows"]] + [list(r) for r in raw_dataset_slices["mumbai_rows"]]

    temp_idx = header.index("temperature")
    rain_idx = header.index("rainfall")
    hum_idx = header.index("humidity")

    # Injected defect 1: 3 rows with -999 in rainfall
    rows[5][rain_idx] = "-999"
    rows[6][rain_idx] = "-999"
    rows[7][rain_idx] = "-999"

    # Injected defect 2: 2 unparseable temperature cells ("abc")
    rows[10][temp_idx] = "abc"
    rows[11][temp_idx] = "abc"

    # Injected defect 3: 1 out-of-range temperature (150)
    rows[15][temp_idx] = "150"

    # Injected defect 4: 4 blank humidity cells
    rows[20][hum_idx] = ""
    rows[21][hum_idx] = ""
    rows[22][hum_idx] = ""
    rows[23][hum_idx] = ""

    # Injected defect 5: 2 duplicated (date, location) keys
    rows.append(list(rows[0]))
    rows.append(list(rows[1]))

    file_path = tmp_path / "defects_weather.csv"
    with open(file_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)

    result = load_dataset(spark, file_path)
    report = result.validation
    warnings = report.warnings

    raw_view = report.info.get("raw_view", "weather_raw")
    std_view = result.standardized_view
    raw_row_count = int(spark.sql(f"SELECT COUNT(*) AS n FROM {raw_view}").collect()[0]["n"])
    std_row_count = int(spark.sql(f"SELECT COUNT(*) AS n FROM {std_view}").collect()[0]["n"])

    # Output detailed audit reporting to stdout for audit verification
    print("\n" + "=" * 65)
    print("VALIDATOR DEFECT AUDIT RESULTS:")
    print(f"Raw row count: {raw_row_count}, Standardized row count: {std_row_count}")
    print(f"Report errors ({len(report.errors)}): {report.errors}")
    print(f"Report warnings ({len(report.warnings)}):")
    for w in warnings:
        print(f"  - {w}")
    print(f"Rows with -999 in rainfall: {report.info['sentinel_counts'].get('rainfall', 0)}")
    print(f"Unparseable temperature cells ('abc'): {report.info['numeric_parse_failures'].get('temperature', 0)}")
    print(f"Duplicated (date, location) keys: {report.info.get('duplicate_keys_count', 0)}")
    print(f"Blank humidity cells: {report.info['null_counts'].get('humidity', 0)}")
    print(f"Out-of-range temperature (150): {report.info['out_of_range_counts'].get('temperature', 0)}")
    print("=" * 65 + "\n")

    # Assert report errors is empty and report counts match exact expected defect counts
    assert len(report.errors) == 0, f"Expected 0 errors, got: {report.errors}"
    assert report.info["sentinel_counts"].get("rainfall") == 3, "Expected 3 sentinel values in rainfall"
    assert report.info["numeric_parse_failures"].get("temperature") == 2, "Expected 2 numeric parse failures in temperature"
    assert report.info.get("duplicate_keys_count") == 2, "Expected 2 duplicate keys"
    assert report.info["null_counts"].get("humidity") == 4, "Expected 4 blank/null humidity cells"
    assert report.info["out_of_range_counts"].get("temperature") == 1, "Expected 1 out-of-range temperature"
    assert raw_row_count == 402, f"Expected 402 raw rows, got {raw_row_count}"
    assert raw_row_count == std_row_count, f"Raw ({raw_row_count}) != Standardized ({std_row_count})"

    # Assert warning text matches exact numbers
    assert any("Column 'rainfall' contains 3 sentinel value(s)" in w for w in warnings), "Missing rainfall sentinel warning"
    assert any("Column 'temperature' has 1 value(s) outside plausible range" in w for w in warnings), "Missing temperature range warning"
    assert any("Column 'temperature' had 2 value(s) that failed numeric parsing" in w for w in warnings), "Missing temperature numeric parse warning"
    assert any("Found 2 duplicate (date, location) key(s)" in w for w in warnings), "Missing duplicate warning"
    assert any("Column 'humidity' has 4 null value(s)" in w for w in warnings), "Missing humidity null warning"
    assert any("duplicate" in w.lower() for w in warnings), "Missing duplicate warning"
    assert any("null value" in w.lower() and "humidity" in w for w in warnings), "Missing humidity null warning"


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

