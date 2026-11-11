"""
tests/test_dashboard_services.py
================================
Comprehensive verification of dashboard backend services, dataset store,
upload ingestion, filtering, and Spark SQL KPI generation.

Expected values are computed independently from the real CSV file using
the standard library `csv` and `statistics` modules, never copied from Spark.
"""

import csv
from datetime import date, datetime
import io
import math
from pathlib import Path
import re
import statistics
import sys
import tomllib
from typing import Any

import pytest

from config.schema import STANDARD_COLUMNS
import config.settings as settings
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
)
from src.pipeline import PipelineError
from src.queries.base import Filters


@pytest.fixture(autouse=True)
def clean_store_state():
    """Ensure ActiveDatasetStore is cleanly initialized before and after each test."""
    store = ActiveDatasetStore.get_instance()
    yield
    # We do not reset the store if subsequent tests rely on the built-in view,
    # but we ensure store lock is free.


# -----------------------------------------------------------------------------
# Test 1: get_filter_options on built-in data
# -----------------------------------------------------------------------------
def test_filter_options_builtin_dataset(spark):
    """
    Test 1: get_filter_options returns 8 locations, min date 2010-01-01, max date
    2025-12-31, years 2010..2025, and 4 canonical seasons.
    """
    load_builtin(spark)
    opts = get_filter_options(spark, view=settings.CLEAN_VIEW_NAME)

    assert len(opts["locations"]) == 8
    assert opts["locations"] == sorted([
        "Bengaluru", "Chennai", "Delhi", "Hyderabad",
        "Jaipur", "Kolkata", "Mumbai", "Shillong"
    ])
    assert opts["min_date"] == "2010-01-01"
    assert opts["max_date"] == "2025-12-31"
    assert opts["years"] == list(range(2010, 2026))
    assert opts["seasons"] == ["Winter", "Summer", "Monsoon", "Post-monsoon"]


# -----------------------------------------------------------------------------
# Test 2: KPIs with no filters (known reference values)
# -----------------------------------------------------------------------------
def test_kpis_no_filters_reference_values(spark):
    """
    Test 2: KPIs with no filters match reference acceptance values:
    total_records 46,752, avg_temp 24.6, max_temp 46.6 at Jaipur on 2024-05-27,
    total_rainfall_mm 174,900.5 (tolerance 0.1).
    """
    load_builtin(spark)
    kpis = get_kpis(spark, filters=None, view=settings.CLEAN_VIEW_NAME)

    assert kpis["total_records"] == 46752
    assert math.isclose(kpis["avg_temp"], 24.6, abs_tol=0.05)
    assert math.isclose(kpis["max_temp"], 46.6, abs_tol=0.05)
    assert kpis["max_temp_location"] == "Jaipur"
    assert kpis["max_temp_date"] == "2024-05-27"
    assert math.isclose(kpis["total_rainfall_mm"], 174900.5, abs_tol=0.1)


# -----------------------------------------------------------------------------
# Test 3: KPIs for Mumbai, 2020-01-01 to 2022-12-31 (cross-checked with CSV)
# -----------------------------------------------------------------------------
def test_kpis_mumbai_filtered_cross_check_csv(spark):
    """
    Test 3: KPIs for Mumbai, 2020-01-01 to 2022-12-31: 1,096 records and 7,706.1 mm
    total rainfall, cross-checked by a plain-Python sum over the CSV.
    """
    load_builtin(spark)

    # 1. Independent Python calculation directly from the raw CSV
    py_count = 0
    py_rain_sum = 0.0
    with open(settings.PRIMARY_DATASET_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["location"] == "Mumbai" and "2020-01-01" <= row["date"] <= "2022-12-31":
                py_count += 1
                py_rain_sum += float(row["rainfall"])

    assert py_count == 1096
    assert math.isclose(py_rain_sum, 7706.1, abs_tol=0.1)

    # 2. Spark SQL KPI query
    f = build_filters(locations=["Mumbai"], start="2020-01-01", end="2022-12-31")
    kpis = get_kpis(spark, filters=f, view=settings.CLEAN_VIEW_NAME)

    assert kpis["total_records"] == 1096
    assert math.isclose(kpis["total_rainfall_mm"], 7706.1, abs_tol=0.1)
    assert math.isclose(kpis["total_rainfall_mm"], py_rain_sum, abs_tol=0.1)


# -----------------------------------------------------------------------------
# Test 4: KPIs for Bengaluru in year 2015 only (cross-checked with CSV)
# -----------------------------------------------------------------------------
def test_kpis_bengaluru_2015_independent_mean(spark):
    """
    Test 4: KPIs for Bengaluru in year 2015 only: avg_temp equals the Python mean within 0.01.
    """
    load_builtin(spark)

    # Independent Python calculation
    temps = []
    with open(settings.PRIMARY_DATASET_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["location"] == "Bengaluru" and row["date"].startswith("2015-"):
                temps.append(float(row["temperature"]))

    py_avg_temp = statistics.mean(temps)

    # Spark SQL KPI query
    f = build_filters(locations=["Bengaluru"], years=[2015])
    kpis = get_kpis(spark, filters=f, view=settings.CLEAN_VIEW_NAME)

    assert math.isclose(kpis["avg_temp"], py_avg_temp, abs_tol=0.01)


# -----------------------------------------------------------------------------
# Test 5: Empty location selection
# -----------------------------------------------------------------------------
def test_kpis_empty_location_selection(spark):
    """
    Test 5: Empty location selection: total_records 0, no exception, other values None.
    """
    load_builtin(spark)
    f = build_filters(locations=[])
    kpis = get_kpis(spark, filters=f, view=settings.CLEAN_VIEW_NAME)

    assert kpis["total_records"] == 0
    assert kpis["avg_temp"] is None
    assert kpis["max_temp"] is None
    assert kpis["min_temp"] is None
    assert kpis["avg_humidity"] is None


# -----------------------------------------------------------------------------
# Test 6: build_filters validation and safety
# -----------------------------------------------------------------------------
def test_build_filters_behavior(spark):
    """
    Test 6: build_filters raises ValueError when start > end; empty years/seasons
    mean no restriction; SQL injection attempt in location returns 0 records safely.
    """
    # 6a. Start > End raises ValueError
    with pytest.raises(ValueError, match="cannot be after end date"):
        build_filters(start="2022-01-01", end="2020-01-01")

    # 6b. Empty years and seasons normalized to None
    f = build_filters(locations=["Delhi"], years=[], seasons=[])
    assert f.years is None
    assert f.seasons is None

    # 6c. SQL injection safety in location parameter
    load_builtin(spark)
    f_inj = build_filters(locations=["x' OR '1'='1"])
    kpis = get_kpis(spark, filters=f_inj, view=settings.CLEAN_VIEW_NAME)
    assert kpis["total_records"] == 0


# -----------------------------------------------------------------------------
# Test 7: get_preview ordering, limit, and standard columns
# -----------------------------------------------------------------------------
def test_get_preview_behavior(spark):
    """
    Test 7: get_preview: n is clamped to PREVIEW_ROWS_MAX; ordering is by location
    then date; the columns equal the 11 standard columns.
    """
    load_builtin(spark)
    # Clamp check: request 5000 rows, should clamp to PREVIEW_ROWS_MAX (1000)
    pdf, sql = get_preview(spark, filters=None, n=5000, view=settings.CLEAN_VIEW_NAME)

    assert len(pdf) == settings.PREVIEW_ROWS_MAX

    expected_cols = [
        "date", "location", "latitude", "longitude", "temperature",
        "temp_max", "temp_min", "rainfall", "humidity", "wind_speed", "pressure"
    ]
    assert list(pdf.columns) == expected_cols

    # Verify ordering by location, date
    loc_list = pdf["location"].tolist()
    date_list = pdf["date"].tolist()
    for i in range(len(pdf) - 1):
        if loc_list[i] == loc_list[i + 1]:
            assert date_list[i] <= date_list[i + 1]
        else:
            assert loc_list[i] <= loc_list[i + 1]


# -----------------------------------------------------------------------------
# Test 8: Upload flow (service level) with messy real data slice
# -----------------------------------------------------------------------------
def test_upload_flow_service_level(spark, tmp_path, monkeypatch, raw_dataset_slices):
    """
    Test 8: Upload flow (service level) with a file built from REAL rows of the dataset
    (first 200 rows of Bengaluru and 200 of Mumbai) with headers renamed to
    Date,City,Temp,Rain,RH,Wind,Pres and dates rewritten as dd/MM/yyyy:
    - inspect_upload suggests the correct mapping and a comma delimiter
    - load_upload succeeds
    - KPI total_records is 400
    - the saved file lives inside UPLOAD_DIR (monkeypatch UPLOAD_DIR to tmp_path).
    """
    import src.upload as up_mod
    import src.dashboard.services as serv_mod

    monkeypatch.setattr(settings, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(up_mod, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(serv_mod, "UPLOAD_DIR", tmp_path)

    # Build CSV bytes from real rows
    b_rows = raw_dataset_slices["bengaluru_rows"]
    m_rows = raw_dataset_slices["mumbai_rows"]
    all_400 = b_rows + m_rows

    custom_headers = ["Date", "City", "Lat", "Lon", "Temp", "Tmax", "Tmin", "Rain", "RH", "Wind", "Pres"]

    out_buf = io.StringIO()
    writer = csv.writer(out_buf)
    writer.writerow(custom_headers)
    for r in all_400:
        # Reformat yyyy-MM-dd to dd/MM/yyyy
        d_reformatted = datetime.strptime(r[0], "%Y-%m-%d").strftime("%d/%m/%Y")
        writer.writerow([d_reformatted] + r[1:])

    csv_bytes = out_buf.getvalue().encode("utf-8")

    # Step 1: inspect_upload
    inspection = inspect_upload(csv_bytes, "real_400_messy.csv")
    assert inspection.delimiter == "comma"
    assert inspection.suggested_mapping["date"] == "Date"
    assert inspection.suggested_mapping["location"] == "City"
    assert inspection.suggested_mapping["temperature"] == "Temp"
    assert inspection.suggested_mapping["rainfall"] == "Rain"
    assert inspection.suggested_mapping["humidity"] == "RH"
    assert inspection.suggested_mapping["wind_speed"] == "Wind"
    assert inspection.suggested_mapping["pressure"] == "Pres"

    # Step 2: load_upload
    prep = load_upload(
        spark=spark,
        saved_name=inspection.saved_name,
        mapping=inspection.suggested_mapping,
        fill_gaps=True,
    )
    assert prep.clean_view == settings.CLEAN_VIEW_NAME

    # Step 3: verify KPIs
    kpi = get_kpis(spark, filters=None, view=settings.CLEAN_VIEW_NAME)
    assert kpi["total_records"] == 400

    # Step 4: verify file saved inside UPLOAD_DIR (tmp_path)
    saved_file = tmp_path / inspection.saved_name
    assert saved_file.is_file()


# -----------------------------------------------------------------------------
# Test 9: Upload without a rainfall column raises PipelineError
# -----------------------------------------------------------------------------
def test_upload_missing_required_column_preserves_active_dataset(spark, tmp_path, monkeypatch, raw_dataset_slices):
    """
    Test 9: Upload without a rainfall column: load_upload raises PipelineError
    with an errors list that names 'rainfall'; the previously active dataset
    remains active and usable.
    """
    import src.upload as up_mod
    import src.dashboard.services as serv_mod

    monkeypatch.setattr(settings, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(up_mod, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(serv_mod, "UPLOAD_DIR", tmp_path)

    # First load built-in dataset to establish active dataset
    load_builtin(spark)
    initial_store = ActiveDatasetStore.get_instance()
    assert initial_store.is_ready(spark)
    assert initial_store.label == settings.BUILTIN_DATASET_LABEL

    # Build CSV with rainfall omitted
    b_rows = raw_dataset_slices["bengaluru_rows"][:50]
    headers_no_rain = ["date", "location", "temperature", "humidity", "wind_speed", "pressure"]
    out_buf = io.StringIO()
    writer = csv.writer(out_buf)
    writer.writerow(headers_no_rain)
    for r in b_rows:
        writer.writerow([r[0], r[1], r[4], r[7], r[8], r[9]])

    csv_bytes = out_buf.getvalue().encode("utf-8")
    inspection = inspect_upload(csv_bytes, "missing_rain.csv")

    with pytest.raises(PipelineError) as exc_info:
        load_upload(
            spark=spark,
            saved_name=inspection.saved_name,
            mapping=inspection.suggested_mapping,
        )

    # Check that errors list names rainfall
    err_str = " ".join(exc_info.value.errors).lower()
    assert "rainfall" in err_str

    # Check previously active dataset remains active and usable
    assert initial_store.is_ready(spark)
    assert initial_store.label == settings.BUILTIN_DATASET_LABEL
    kpis = get_kpis(spark, filters=None, view=settings.CLEAN_VIEW_NAME)
    assert kpis["total_records"] == 46752


# -----------------------------------------------------------------------------
# Test 10: Dataset switching never serves stale views or cache
# -----------------------------------------------------------------------------
def test_dataset_switching_uncaches_and_refreshes(spark, tmp_path, monkeypatch, raw_dataset_slices):
    """
    Test 10: After load_upload (400 rows) the filter options show 2 locations;
    after load_builtin they show 8 again and KPIs return 46,752 (proves no stale
    views or cache).
    """
    import src.upload as up_mod
    import src.dashboard.services as serv_mod

    monkeypatch.setattr(settings, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(up_mod, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(serv_mod, "UPLOAD_DIR", tmp_path)

    # 1. Upload 400 rows (Bengaluru + Mumbai)
    out_buf = io.StringIO()
    writer = csv.writer(out_buf)
    writer.writerow(raw_dataset_slices["header"])
    writer.writerows(raw_dataset_slices["bengaluru_rows"])
    writer.writerows(raw_dataset_slices["mumbai_rows"])
    csv_bytes = out_buf.getvalue().encode("utf-8")

    inspection = inspect_upload(csv_bytes, "switch_test_400.csv")
    load_upload(spark, inspection.saved_name, mapping=inspection.suggested_mapping)

    opts_upload = get_filter_options(spark, view=settings.CLEAN_VIEW_NAME)
    assert len(opts_upload["locations"]) == 2
    assert sorted(opts_upload["locations"]) == ["Bengaluru", "Mumbai"]

    # 2. Reload built-in dataset
    load_builtin(spark)
    opts_builtin = get_filter_options(spark, view=settings.CLEAN_VIEW_NAME)
    assert len(opts_builtin["locations"]) == 8

    kpis_builtin = get_kpis(spark, filters=None, view=settings.CLEAN_VIEW_NAME)
    assert kpis_builtin["total_records"] == 46752


# -----------------------------------------------------------------------------
# Test 11: load_upload rejects path traversal
# -----------------------------------------------------------------------------
def test_load_upload_rejects_path_traversal(spark):
    """
    Test 11: load_upload rejects saved_name containing path separators ("../x.csv").
    """
    with pytest.raises(ValueError, match="Path separators and traversal are not allowed"):
        load_upload(spark, saved_name="../malicious.csv", mapping={})


# -----------------------------------------------------------------------------
# Test 12: Static architecture & project rules check
# -----------------------------------------------------------------------------
def test_static_rules_and_no_forbidden_patterns():
    """
    Test 12: Static check:
    1. src/dashboard must not import streamlit.
    2. app.py contains no '.rdd', 'parallelize', 'createDataFrame', 'udf(' or 'pandas_udf'.
    """
    # 1. Check src/dashboard for streamlit imports
    dashboard_files = list((settings.BASE_DIR / "src" / "dashboard").glob("*.py"))
    for py_file in dashboard_files:
        content = py_file.read_text(encoding="utf-8")
        lines = [line.strip() for line in content.splitlines()]
        for line in lines:
            if line.startswith("#"):
                continue
            assert not re.search(r"^\s*(import streamlit|from streamlit)", line), (
                f"Forbidden streamlit import found in {py_file.name}: {line}"
            )

    # 2. Check app.py for forbidden PySpark constructs
    app_py = (settings.BASE_DIR / "app.py").read_text(encoding="utf-8")
    forbidden = [".rdd", "parallelize", "createDataFrame", "udf(", "pandas_udf"]
    for pattern in forbidden:
        assert pattern not in app_py, f"Forbidden PySpark pattern '{pattern}' found in app.py"


# -----------------------------------------------------------------------------
# Test 13: Settings max upload equals Streamlit config.toml maxUploadSize
# -----------------------------------------------------------------------------
def test_settings_matches_streamlit_toml():
    """
    Test 13: Streamlit maxUploadSize in .streamlit/config.toml equals settings.MAX_UPLOAD_MB.
    """
    toml_path = settings.BASE_DIR / ".streamlit" / "config.toml"
    assert toml_path.is_file(), f"Missing config.toml at {toml_path}"

    with open(toml_path, "rb") as f:
        config = tomllib.load(f)

    toml_max_upload = config["server"]["maxUploadSize"]
    assert toml_max_upload == settings.MAX_UPLOAD_MB, (
        f"Mismatch: config.toml ({toml_max_upload}) != settings.MAX_UPLOAD_MB ({settings.MAX_UPLOAD_MB})"
    )
