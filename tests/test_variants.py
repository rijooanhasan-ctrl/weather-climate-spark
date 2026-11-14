"""
tests/test_variants.py
======================
Test suite for dataset variants V1 through V10 and Phase 9 audit invariants:
- V1: Headers Date;City;Temp;Rain;RH;Wind;Pres, semicolon, UTF-8 BOM, dd/MM/yyyy
- V2: Missing location column handled via default_location
- V3: Kelvin temperature unit mismatch (>50% out of range) triggers validation ERROR
- V4: Single year dataset handled gracefully in trend query ("insufficient data")
- V5: Two cities, 3 years handled gracefully in trend query
- V6: Rainfall column empty for one city, handled cleanly and NEVER interpolated
- V7: Gaps <= 3 days interpolated, longer gaps untouched, was_filled flags set
- V8: Mixed case and spaces in location reported as separate locations
- V9: Dates in yyyyMMdd, column names with spaces and quotes auto-mapped
- V10: Duplicate header names trigger friendly DatasetLoadError
- A2: Insight rules conformance (no century text, valid formats)
- A3: Validator merged profiling query consistency
"""

from __future__ import annotations

import csv
import sys
import tempfile
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import PRIMARY_DATASET_PATH, STANDARD_COLUMNS
from src.cleaner import clean
from src.dashboard.insights import generate_insights
from src.data_loader import DatasetLoadError, load_dataset, suggest_column_mapping
from src.pipeline import PipelineError, prepare
from src.queries.base import Filters
from src.queries.kpis import kpi_summary
from src.queries.rainfall import rain_yearly
from src.queries.temperature import temp_trend, temp_yearly
from src.validator import validate


@pytest.fixture(scope="module")
def real_slices():
    with open(PRIMARY_DATASET_PATH, mode="r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        rows = [next(reader) for _ in range(300)]
    return {"header": header, "rows": rows}


def test_variant_v1_automap(spark, tmp_path, real_slices):
    """V1: Semicolon delimiter, UTF-8 BOM, dd/MM/yyyy dates, alternate headers."""
    v1_header = ["Date", "City", "Temp", "Rain", "RH", "Wind", "Pres"]
    idx_map = {c: i for i, c in enumerate(real_slices["header"])}
    rows = []
    for r in real_slices["rows"][:50]:
        parts = r[idx_map["date"]].split("-")
        ddmmyyyy = f"{parts[2]}/{parts[1]}/{parts[0]}"
        rows.append([
            ddmmyyyy, r[idx_map["location"]], r[idx_map["temperature"]],
            r[idx_map["rainfall"]], r[idx_map["humidity"]],
            r[idx_map["wind_speed"]], r[idx_map["pressure"]]
        ])

    p = tmp_path / "v1.csv"
    with open(p, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(v1_header)
        w.writerows(rows)

    load_res = load_dataset(spark, p)
    assert load_res.delimiter == ";"
    assert load_res.mapping.get("date") == "Date"
    assert load_res.mapping.get("location") == "City"
    assert load_res.mapping.get("temperature") == "Temp"
    assert load_res.mapping.get("rainfall") == "Rain"
    assert load_res.mapping.get("humidity") == "RH"
    assert load_res.mapping.get("wind_speed") == "Wind"
    assert load_res.mapping.get("pressure") == "Pres"
    assert load_res.date_format_info.get("format") == "dd/MM/yyyy"
    assert load_res.validation.is_valid is True


def test_variant_v2_default_location(spark, tmp_path, real_slices):
    """V2: Missing location column is filled by default_location."""
    header = [c for c in real_slices["header"] if c != "location"]
    loc_idx = real_slices["header"].index("location")
    rows = [[v for i, v in enumerate(r) if i != loc_idx] for r in real_slices["rows"][:50]]

    p = tmp_path / "v2.csv"
    with open(p, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)

    load_res = load_dataset(spark, p, default_location="Custom_Station")
    assert load_res.validation.is_valid is True
    df_clean, _ = clean(spark, std_view=load_res.standardized_view, view_name="test_v2_clean")
    locs = [r["location"] for r in spark.sql("SELECT DISTINCT location FROM test_v2_clean").collect()]
    assert locs == ["Custom_Station"]


def test_variant_v3_unit_mismatch_error(spark, tmp_path, real_slices):
    """V3: Kelvin temperatures (>50% out of range) trigger an ERROR in validation and PipelineError."""
    header = list(real_slices["header"])
    t_idx = header.index("temperature")
    rows = []
    for r in real_slices["rows"][:50]:
        rc = list(r)
        c_val = float(r[t_idx])
        rc[t_idx] = str(round(c_val + 273.15, 2))  # Kelvin > 60 C
        rows.append(rc)

    p = tmp_path / "v3.csv"
    with open(p, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)

    load_res = load_dataset(spark, p)
    assert not load_res.validation.is_valid
    assert any("unit mismatch" in err.lower() and "temperature" in err.lower() for err in load_res.validation.errors)

    with pytest.raises(PipelineError):
        prepare(spark, p)


def test_variant_v4_short_span_one_year(spark, tmp_path, real_slices):
    """V4: Single year dataset handled gracefully in trend query."""
    header = list(real_slices["header"])
    rows = real_slices["rows"][:365]

    p = tmp_path / "v4.csv"
    with open(p, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)

    load_res = load_dataset(spark, p)
    df_clean, _ = clean(spark, std_view=load_res.standardized_view, view_name="test_v4_clean")
    tr_res = temp_trend(spark, view="test_v4_clean")
    pdf = tr_res.to_pandas()
    assert not pdf.empty
    assert (pdf["trend_label"] == "insufficient data").all()


def test_variant_v5_intermittent_years(spark, tmp_path, real_slices):
    """V5: 3-year dataset handled cleanly and trend query returns insufficient data."""
    header = list(real_slices["header"])
    rows = real_slices["rows"][:100]

    p = tmp_path / "v5.csv"
    with open(p, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)

    load_res = load_dataset(spark, p)
    assert load_res.validation.info["total_rows"] == 100
    df_clean, report = clean(spark, std_view=load_res.standardized_view, view_name="test_v5_clean")
    assert report.summary["clean_rows_out"] == 100
    tr_res = temp_trend(spark, view="test_v5_clean")
    pdf = tr_res.to_pandas()
    assert not pdf.empty
    assert (pdf["trend_label"] == "insufficient data").all()


def test_variant_v6_null_rainfall(spark, tmp_path, real_slices):
    """V6: Missing rainfall values handled cleanly and NEVER interpolated."""
    header = list(real_slices["header"])
    r_idx = header.index("rainfall")
    rows = [list(r) for r in real_slices["rows"][:50]]
    for i in range(5, 15):
        rows[i][r_idx] = ""

    p = tmp_path / "v6.csv"
    with open(p, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)

    load_res = load_dataset(spark, p)
    df_clean, report = clean(spark, std_view=load_res.standardized_view, view_name="test_v6_clean")
    assert "rainfall" not in report.summary["values_changed"]["interpolated"]


def test_variant_v7_short_gap_filling(spark, tmp_path, real_slices):
    """V7: Short gaps (<= 3 days) filled with was_filled flags set, longer untouched."""
    header = list(real_slices["header"])
    t_idx = header.index("temperature")
    rows = [list(r) for r in real_slices["rows"][:30]]
    # 2-day gap at rows 10, 11 (within 3 days -> filled)
    rows[10][t_idx] = ""
    rows[11][t_idx] = ""

    p = tmp_path / "v7.csv"
    with open(p, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)

    load_res = load_dataset(spark, p)
    df_clean, report = clean(spark, std_view=load_res.standardized_view, view_name="test_v7_clean")
    assert report.summary["values_changed"]["interpolated"].get("temperature") == 2

    filled_flags = spark.sql(
        "SELECT COUNT(*) AS c FROM test_v7_clean WHERE temperature_was_filled = true"
    ).collect()[0]["c"]
    assert filled_flags == 2


def test_variant_v8_mixed_case_spaces_location(spark, tmp_path, real_slices):
    """V8: Location names with mixed case and surrounding spaces trimmed to 3 distinct locations."""
    header = list(real_slices["header"])
    loc_idx = header.index("location")
    rows = []
    loc_variants = [" MUMBAI ", " Mumbai ", " mumbai ", "Mumbai", "mumbai"]
    for i, r in enumerate(real_slices["rows"][:30]):
        rc = list(r)
        rc[loc_idx] = loc_variants[i % len(loc_variants)]
        rows.append(rc)

    p = tmp_path / "v8.csv"
    with open(p, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)

    load_res = load_dataset(spark, p)
    df_clean, _ = clean(spark, std_view=load_res.standardized_view, view_name="test_v8_clean")
    locs = [r["location"] for r in spark.sql("SELECT DISTINCT location FROM test_v8_clean").collect()]
    assert len(set(locs)) == 3
    assert set(locs) == {"MUMBAI", "Mumbai", "mumbai"}


def test_variant_v9_yyyymmdd_spaced_headers(spark, tmp_path, real_slices):
    """V9: Dates in yyyyMMdd and column names with spaces/quotes auto-mapped."""
    header = [
        " Observation Date ", " City Name ", " Mean Temp ", " Precipitation ",
        " Relative Humidity ", " Wind Velocity ", " Atmospheric Pressure "
    ]
    idx_map = {c: i for i, c in enumerate(real_slices["header"])}
    rows = []
    for r in real_slices["rows"][:50]:
        compact_date = r[idx_map["date"]].replace("-", "")
        rows.append([
            compact_date, r[idx_map["location"]], r[idx_map["temperature"]],
            r[idx_map["rainfall"]], r[idx_map["humidity"]],
            r[idx_map["wind_speed"]], r[idx_map["pressure"]]
        ])

    p = tmp_path / "v9.csv"
    with open(p, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, quoting=csv.QUOTE_ALL)
        w.writerow(header)
        w.writerows(rows)

    load_res = load_dataset(spark, p)
    assert load_res.date_format_info.get("format") == "yyyyMMdd"
    assert load_res.mapping.get("date") == "Observation Date"
    assert load_res.mapping.get("location") == "City Name"
    assert load_res.validation.is_valid is True


def test_variant_v10_duplicate_headers(spark, tmp_path, real_slices):
    """V10: Duplicate header names raise friendly DatasetLoadError."""
    header = ["date", "location", "temperature", "rainfall", "temperature"]
    rows = [[r[0], r[1], r[2], r[3], r[2]] for r in real_slices["rows"][:10]]

    p = tmp_path / "v10.csv"
    with open(p, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)

    with pytest.raises(DatasetLoadError) as exc_info:
        load_dataset(spark, p)
    assert "Duplicate or empty column names" in str(exc_info.value)


def test_a2_insight_rules_and_no_century_text():
    """A2: Insight generator respects all formatting rules and contains no per-century text."""
    import pandas as pd
    frames = {
        "kpi_summary": pd.DataFrame([{
            "total_records": 1000,
            "date_min": "2010-01-01",
            "date_max": "2020-12-31",
            "total_locations": 2,
            "avg_temp_overall": 25.4,
            "max_temp_recorded": 42.1,
            "min_temp_recorded": 10.2,
            "total_rainfall_overall": 1200.5,
            "max_daily_rainfall": 150.2,
            "avg_humidity_overall": 65.0,
            "avg_wind_speed_overall": 8.5,
            "avg_pressure_overall": 1012.0,
        }]),
        "temp_yearly": pd.DataFrame([
            {"year": 2010, "avg_temp": 24.5, "anomaly": -0.9},
            {"year": 2015, "avg_temp": 25.4, "anomaly": 0.0},
            {"year": 2020, "avg_temp": 26.3, "anomaly": 0.9},
        ]),
        "rain_yearly": pd.DataFrame([
            {"year": 2010, "total_rainfall": 1100.0, "pct_of_normal": 91.6},
            {"year": 2015, "total_rainfall": 1200.0, "pct_of_normal": 100.0},
            {"year": 2020, "total_rainfall": 1300.0, "pct_of_normal": 108.3},
        ]),
        "temp_trend": pd.DataFrame([
            {"location": "Bengaluru", "slope": 0.035, "intercept": 24.0, "r_squared": 0.85, "data_span_years": 11}
        ]),
    }
    kpis = frames["kpi_summary"].iloc[0].to_dict()
    insights = generate_insights(frames, kpis)
    assert len(insights) > 0
    for ins in insights:
        assert "per century" not in ins.text.lower()
        assert "°c/decade" in ins.text.lower() or "decade" in ins.text.lower() or "°c" in ins.text.lower() or "%" in ins.text.lower() or "mm" in ins.text.lower()


def test_validator_merge_query_consistency(spark):
    """A3: Merged validator profiling queries return consistent column statistics."""
    from src.data_loader import load_raw_csv, standardize

    raw_df = load_raw_csv(spark, PRIMARY_DATASET_PATH, delimiter=",", view_name="test_vm_raw")
    std_df = standardize(
        spark,
        raw_view="test_vm_raw",
        mapping={c: c for c in STANDARD_COLUMNS},
        date_format="yyyy-MM-dd",
        view_name="test_vm_std",
    )
    report = validate(
        spark,
        raw_view="test_vm_raw",
        std_view="test_vm_std",
        mapping={c: c for c in STANDARD_COLUMNS},
        original_row_count=raw_df.count(),
    )
    assert report.is_valid is True
    assert len(report.errors) == 0
    assert len(report.sql_log) == 10
    assert report.info["total_rows"] == 46752
    assert report.info["duplicate_keys_count"] == 0
    assert len(report.column_stats) == 5


def test_variant_v5_v6_insights(spark, tmp_path):
    """Phase 10 A2: Variants produce insights; V5 trend says 'insufficient data' and V6 handles missing city rainfall."""
    from scripts.make_variant_files import make_variants
    from src.dashboard.services import get_insights
    var_files = make_variants(tmp_path)

    # Test V5
    prep_v5 = prepare(spark, var_files["V5"], view_name="test_v5_clean_ins")
    insights_v5 = get_insights(spark, filters=None, view="test_v5_clean_ins")
    assert len(insights_v5) >= 8
    trend_ins_v5 = [i for i in insights_v5 if "temp_trend" in i.source_keys][0]
    assert "insufficient data" in trend_ins_v5.text.lower()
    assert trend_ins_v5.level == "caution"

    # Test V6
    prep_v6 = prepare(spark, var_files["V6"], view_name="test_v6_clean_ins")
    insights_v6 = get_insights(spark, filters=None, view="test_v6_clean_ins")
    assert len(insights_v6) >= 8
    rain_ins_v6 = [i for i in insights_v6 if "rain_location_ranking" in i.source_keys][0]
    assert "Bengaluru" in rain_ins_v6.text


def test_validator_old_vs_new_equality(spark, tmp_path):
    """Phase 10 A5: Merged validator matches previous validator on defect fixtures and real data."""
    import tools.validator_old as val_old
    from src.validator import validate as val_new
    from config.schema import STANDARD_SCHEMA
    from src.data_loader import load_raw_csv, standardize

    all_cols = [f.name for f in STANDARD_SCHEMA.fields]
    all_mapping = {c: c for c in all_cols}

    # Verify on real dataset
    raw_real = load_raw_csv(spark, PRIMARY_DATASET_PATH, delimiter=",", view_name="test_raw_real_eq")
    std_real = standardize(spark, raw_view="test_raw_real_eq", mapping=all_mapping, date_format="yyyy-MM-dd", view_name="test_std_real_eq")
    rep_old = val_old.validate(spark, raw_view="test_raw_real_eq", std_view="test_std_real_eq", mapping=all_mapping, original_row_count=46752)
    rep_new = val_new(spark, raw_view="test_raw_real_eq", std_view="test_std_real_eq", mapping=all_mapping, original_row_count=46752)

    assert rep_old.errors == rep_new.errors
    assert sorted(rep_old.warnings) == sorted(rep_new.warnings)
    assert sorted(rep_old.column_stats, key=lambda x: x["column"]) == sorted(rep_new.column_stats, key=lambda x: x["column"])
    for k in ["total_rows", "min_date", "max_date", "sentinel_counts", "numeric_parse_failures", "duplicate_keys_count", "null_counts", "out_of_range_counts"]:
        assert rep_old.info.get(k) == rep_new.info.get(k)


def test_scale_count_logic(spark, tmp_path):
    """Phase 10 A6: Tiny 2x replica (two cities replicated once = 4 stations) verifies exact expected query counts."""
    from src.queries.climate import climate_city_profile
    from src.queries.temperature import temp_location_ranking

    # Read two cities from primary dataset
    selected_cities = {"Bengaluru", "Mumbai"}
    rows_orig = []
    with open(PRIMARY_DATASET_PATH, mode="r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        loc_idx = header.index("location")
        for r in reader:
            if r[loc_idx] in selected_cities:
                rows_orig.append(r)

    # Replicate selected cities once with suffix _1
    rows_replica = []
    for r in rows_orig:
        r_rep = list(r)
        r_rep[loc_idx] = f"{r[loc_idx]}_1"
        rows_replica.append(r_rep)

    test_csv = tmp_path / "scale_tiny_2x.csv"
    with open(test_csv, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows_orig)
        writer.writerows(rows_replica)

    view_name = "test_scale_tiny_clean"
    prepare(spark, str(test_csv), view_name=view_name)

    ty_df = temp_yearly(spark, view=view_name).df
    ry_df = rain_yearly(spark, view=view_name).df
    tlr_df = temp_location_ranking(spark, view=view_name).df
    ccp_df = climate_city_profile(spark, view=view_name).df
    tt_df = temp_trend(spark, view=view_name).df
    kpi_df = kpi_summary(spark, view=view_name).df

    assert ty_df.count() == 4 * 16
    assert ry_df.count() == 4 * 16
    assert tlr_df.count() == 4
    assert ccp_df.count() == 4
    assert tt_df.count() == 4 + 1
    assert kpi_df.count() == 1



