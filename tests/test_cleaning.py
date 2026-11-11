"""
tests/test_cleaning.py
======================
Unit and integration tests for the Spark SQL data cleaning pipeline.

Uses real historical weather observations sliced from data/raw/india_weather_daily.csv.
No synthetic or fake weather observations are invented; only explicit defects are injected.
"""

import csv
import json
from pathlib import Path
import sys

import pytest

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.schema import STANDARD_SCHEMA  # noqa: E402
from config.settings import PRIMARY_DATASET_PATH  # noqa: E402
from src.cleaner import clean  # noqa: E402
from src.data_loader import load_dataset  # noqa: E402


def test_clean_sentinels_to_null(spark, tmp_path, raw_dataset_slices):
    """
    Test 1: 3 rainfall cells with -999 become NULL; row count is unchanged;
    report records 3 changed values in rainfall.
    """
    header = list(raw_dataset_slices["header"])
    rows = [list(r) for r in raw_dataset_slices["bengaluru_rows"][:25]]
    rain_idx = header.index("rainfall")

    # Inject 3 sentinels in rainfall
    rows[3][rain_idx] = "-999"
    rows[7][rain_idx] = "-999"
    rows[12][rain_idx] = "-999"

    file_path = tmp_path / "sentinels_test.csv"
    with open(file_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)

    load_res = load_dataset(spark, file_path)
    df_clean, report = clean(spark, std_view=load_res.standardized_view)

    assert report.summary["raw_rows_in"] == 25
    assert report.summary["clean_rows_out"] == 25
    assert report.summary["rows_removed"]["total_rows_removed"] == 0
    assert report.summary["values_changed"]["sentinels_to_null"].get("rainfall") == 3

    # Verify that the 3 values in rainfall became NULL
    null_count = spark.sql(f"SELECT COUNT(*) AS c FROM {load_res.standardized_view} WHERE rainfall IS NULL").collect()[0]["c"]
    clean_null_count = spark.sql("SELECT COUNT(*) AS c FROM weather_clean WHERE rainfall IS NULL").collect()[0]["c"]
    assert clean_null_count == null_count + 3


def test_clean_out_of_range_to_null(spark, tmp_path, raw_dataset_slices):
    """
    Test 2: One temperature cell with 150 becomes NULL; row is preserved; count is 1.
    """
    header = list(raw_dataset_slices["header"])
    rows = [list(r) for r in raw_dataset_slices["bengaluru_rows"][:20]]
    temp_idx = header.index("temperature")

    rows[5][temp_idx] = "150.0"

    file_path = tmp_path / "outlier_test.csv"
    with open(file_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)

    load_res = load_dataset(spark, file_path)
    df_clean, report = clean(spark, std_view=load_res.standardized_view, options={"fill_gaps": False})

    assert report.summary["clean_rows_out"] == 20
    assert report.summary["rows_removed"]["total_rows_removed"] == 0
    assert report.summary["values_changed"]["out_of_range_to_null"].get("temperature") == 1

    # Verify temperature in row 5 is NULL when gap filling is disabled
    target_date = rows[5][0]
    row = spark.sql(f"SELECT temperature FROM weather_clean WHERE date = '{target_date}'").collect()[0]
    assert row["temperature"] is None


def test_clean_duplicates_keeps_most_complete_record(spark, tmp_path, raw_dataset_slices):
    """
    Test 3: 2 duplicated (date, location) keys leave 1 row each;
    the row kept is the more complete one (one has NULL humidity, the other has a value; value survives).
    """
    header = list(raw_dataset_slices["header"])
    rows = [list(r) for r in raw_dataset_slices["bengaluru_rows"][:15]]
    hum_idx = header.index("humidity")

    # Key 0: row 0 has real humidity (e.g. 76.0); duplicate copy has NULL humidity
    dup_row_0 = list(rows[0])
    dup_row_0[hum_idx] = ""

    # Key 1: duplicate row 1 exactly
    dup_row_1 = list(rows[1])

    rows.append(dup_row_0)
    rows.append(dup_row_1)

    file_path = tmp_path / "duplicates_test.csv"
    with open(file_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)

    load_res = load_dataset(spark, file_path)
    df_clean, report = clean(spark, std_view=load_res.standardized_view)

    assert report.summary["raw_rows_in"] == 17
    assert report.summary["clean_rows_out"] == 15
    assert report.summary["rows_removed"]["duplicate_surplus_dropped"] == 2

    # Assert the non-null humidity survived for Key 0
    date_0 = rows[0][0]
    surviving_row = spark.sql(f"SELECT humidity FROM weather_clean WHERE date = '{date_0}'").collect()[0]
    expected_hum = float(rows[0][hum_idx])
    assert surviving_row["humidity"] == expected_hum


def test_clean_gap_filling_linear_interpolation(spark, tmp_path, raw_dataset_slices):
    """
    Test 4: 2-day stretch of temperature is linearly interpolated between real neighbours;
    *_was_filled is True only for those 2 rows; a 5-day stretch stays NULL with no flag.
    """
    header = list(raw_dataset_slices["header"])
    rows = [list(r) for r in raw_dataset_slices["bengaluru_rows"][:15]]
    temp_idx = header.index("temperature")

    t0 = float(rows[0][temp_idx])
    t3 = float(rows[3][temp_idx])

    # 2-day gap (indices 1 and 2)
    rows[1][temp_idx] = ""
    rows[2][temp_idx] = ""

    # 5-day gap (indices 6 to 10)
    for i in range(6, 11):
        rows[i][temp_idx] = ""

    file_path = tmp_path / "gap_fill_test.csv"
    with open(file_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)

    load_res = load_dataset(spark, file_path)
    df_clean, report = clean(spark, std_view=load_res.standardized_view)

    assert report.summary["values_changed"]["interpolated"].get("temperature") == 2

    clean_records = spark.sql(
        "SELECT date, temperature, temperature_was_filled FROM weather_clean ORDER BY date"
    ).collect()

    # Verify linear interpolation arithmetic for the 2-day gap
    expected_t1 = round(t0 + (1.0 / 3.0) * (t3 - t0), 4)
    expected_t2 = round(t0 + (2.0 / 3.0) * (t3 - t0), 4)

    assert abs(clean_records[1]["temperature"] - expected_t1) < 1e-3
    assert abs(clean_records[2]["temperature"] - expected_t2) < 1e-3
    assert clean_records[1]["temperature_was_filled"] is True
    assert clean_records[2]["temperature_was_filled"] is True
    assert clean_records[0]["temperature_was_filled"] is False
    assert clean_records[3]["temperature_was_filled"] is False

    # Verify 5-day gap was NOT filled
    for i in range(6, 11):
        assert clean_records[i]["temperature"] is None
        assert clean_records[i]["temperature_was_filled"] is False


def test_clean_rainfall_never_filled(spark, tmp_path, raw_dataset_slices):
    """
    Test 5: Rainfall is never filled; missing values remain NULL and rainfall_was_filled does not exist.
    """
    header = list(raw_dataset_slices["header"])
    rows = [list(r) for r in raw_dataset_slices["bengaluru_rows"][:10]]
    rain_idx = header.index("rainfall")

    # Blank rainfall on consecutive days
    rows[2][rain_idx] = ""
    rows[3][rain_idx] = ""

    file_path = tmp_path / "rain_fill_test.csv"
    with open(file_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)

    load_res = load_dataset(spark, file_path)
    df_clean, report = clean(spark, std_view=load_res.standardized_view)

    assert "rainfall" not in report.summary["values_changed"]["interpolated"]
    assert "rainfall_was_filled" not in df_clean.columns

    records = spark.sql("SELECT rainfall FROM weather_clean ORDER BY date").collect()
    assert records[2]["rainfall"] is None
    assert records[3]["rainfall"] is None


def test_clean_gap_filling_never_crosses_locations(spark, tmp_path, raw_dataset_slices):
    """
    Test 6: Gap filling never crosses locations: Day 1 of City 2 with NULL temp
    is NOT filled from the last day of City 1.
    """
    header = list(raw_dataset_slices["header"])
    b_rows = [list(r) for r in raw_dataset_slices["bengaluru_rows"][:5]]
    m_rows = [list(r) for r in raw_dataset_slices["mumbai_rows"][:5]]

    temp_idx = header.index("temperature")
    # Set day 1 of Mumbai to NULL
    m_rows[0][temp_idx] = ""

    file_path = tmp_path / "two_cities_gap.csv"
    with open(file_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(b_rows)
        writer.writerows(m_rows)

    load_res = load_dataset(spark, file_path)
    df_clean, report = clean(spark, std_view=load_res.standardized_view)

    # Verify Mumbai day 1 remains NULL
    mumbai_day1 = spark.sql(
        "SELECT temperature, temperature_was_filled FROM weather_clean WHERE location = 'Mumbai' ORDER BY date"
    ).collect()[0]

    assert mumbai_day1["temperature"] is None
    assert mumbai_day1["temperature_was_filled"] is False


def test_clean_gap_calendar_days_absent_rows(spark, tmp_path, raw_dataset_slices):
    """
    Test 6b: Calendar-day gap filling rule with absent rows in the dataset:
    (1) A NULL row whose neighbours are 6 calendar days apart due to absent rows must NOT be filled.
    (2) A NULL row whose neighbours are 3 calendar days apart with 1 absent row must be filled correctly.
    """
    header = list(raw_dataset_slices["header"])
    b_rows = [list(r) for r in raw_dataset_slices["bengaluru_rows"]]
    temp_idx = header.index("temperature")

    # (1) Test 6 calendar days apart: Jan 1, Jan 2 (NULL), Jan 7
    # b_rows[0] is 2010-01-01, b_rows[1] is 2010-01-02, b_rows[6] is 2010-01-07
    row_jan1 = list(b_rows[0])
    row_jan2 = list(b_rows[1])
    row_jan2[temp_idx] = ""  # NULL
    row_jan7 = list(b_rows[6])

    path_6days = tmp_path / "gap_6_calendar_days.csv"
    with open(path_6days, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows([row_jan1, row_jan2, row_jan7])

    load_res1 = load_dataset(spark, path_6days)
    df_clean1, rep1 = clean(spark, std_view=load_res1.standardized_view, view_name="clean_gap_6d")

    res_6d = spark.sql("SELECT date, temperature, temperature_was_filled FROM clean_gap_6d ORDER BY date").collect()
    assert res_6d[1]["date"].strftime("%Y-%m-%d") == "2010-01-02"
    assert res_6d[1]["temperature"] is None, "Gap of 5 calendar days (> 3) must NOT be filled"
    assert res_6d[1]["temperature_was_filled"] is False

    # (2) Test 3 calendar days apart with one absent row: Jan 1, Jan 2 (NULL), Jan 4
    # b_rows[0] is 2010-01-01, b_rows[1] is 2010-01-02, b_rows[3] is 2010-01-04 (Jan 3 absent)
    t0 = float(b_rows[0][temp_idx])
    t3 = float(b_rows[3][temp_idx])
    row_jan4 = list(b_rows[3])

    path_3days = tmp_path / "gap_3_calendar_days.csv"
    with open(path_3days, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows([row_jan1, row_jan2, row_jan4])

    load_res2 = load_dataset(spark, path_3days)
    df_clean2, rep2 = clean(spark, std_view=load_res2.standardized_view, view_name="clean_gap_3d")

    res_3d = spark.sql("SELECT date, temperature, temperature_was_filled FROM clean_gap_3d ORDER BY date").collect()
    assert res_3d[1]["date"].strftime("%Y-%m-%d") == "2010-01-02"
    expected_interp = round(t0 + (1.0 / 3.0) * (t3 - t0), 4)
    assert abs(res_3d[1]["temperature"] - expected_interp) < 1e-3, "Calendar-gap <= 3 days must be linearly interpolated"
    assert res_3d[1]["temperature_was_filled"] is True


def test_clean_season_mapping_all_twelve_months(spark):
    """
    Test 7: Season mapping assigns standard Indian meteorological seasons for all 12 months.
    """
    season_df = spark.sql("""
    SELECT
        m,
        CASE
            WHEN m IN (1, 2) THEN 'Winter'
            WHEN m IN (3, 4, 5) THEN 'Summer'
            WHEN m IN (6, 7, 8, 9) THEN 'Monsoon'
            WHEN m IN (10, 11, 12) THEN 'Post-monsoon'
        END AS season
    FROM (SELECT explode(sequence(1, 12)) AS m)
    ORDER BY m
    """)
    rows = season_df.collect()
    seasons = {r["m"]: r["season"] for r in rows}

    assert seasons[1] == "Winter" and seasons[2] == "Winter"
    assert seasons[3] == "Summer" and seasons[4] == "Summer" and seasons[5] == "Summer"
    assert seasons[6] == "Monsoon" and seasons[7] == "Monsoon" and seasons[8] == "Monsoon" and seasons[9] == "Monsoon"
    assert seasons[10] == "Post-monsoon" and seasons[11] == "Post-monsoon" and seasons[12] == "Post-monsoon"


@pytest.mark.skipif(not PRIMARY_DATASET_PATH.exists(), reason="Primary dataset not found")
def test_clean_real_primary_dataset_zero_changes(spark):
    """
    Test 8: Real primary dataset passes through clean() with 46,752 rows,
    0 values changed, 0 rows removed, and checksum equality.
    """
    load_res = load_dataset(spark, PRIMARY_DATASET_PATH)
    df_clean, report = clean(spark, std_view=load_res.standardized_view)

    assert report.summary["raw_rows_in"] == 46752
    assert report.summary["clean_rows_out"] == 46752
    assert report.summary["rows_removed"]["total_rows_removed"] == 0
    assert report.summary["values_changed"]["sentinels_to_null"] == {}
    assert report.summary["values_changed"]["out_of_range_to_null"] == {}
    assert report.summary["values_changed"]["interpolated"] == {}

    # Checksum equality across all numeric metrics
    sum_clauses = [f"ROUND(SUM(`{f.name}`), 2) AS `sum_{f.name}`" for f in STANDARD_SCHEMA.fields if f.name not in ["date", "location"]]
    chk_sql_std = f"SELECT {', '.join(sum_clauses)} FROM {load_res.standardized_view}"
    chk_sql_cln = f"SELECT {', '.join(sum_clauses)} FROM weather_clean"

    std_chk = spark.sql(chk_sql_std).collect()[0].asDict()
    cln_chk = spark.sql(chk_sql_cln).collect()[0].asDict()

    assert std_chk == cln_chk, f"Checksum mismatch: {std_chk} != {cln_chk}"


@pytest.mark.skipif(not PRIMARY_DATASET_PATH.exists(), reason="Primary dataset not found")
def test_clean_idempotence(spark):
    """
    Test 9: Running clean() a second time on the clean output changes nothing.
    """
    load_res = load_dataset(spark, PRIMARY_DATASET_PATH)
    df_clean1, rep1 = clean(spark, std_view=load_res.standardized_view, view_name="clean_pass_1")

    # Re-register clean output as standardized view input for second pass
    std_cols = [f.name for f in STANDARD_SCHEMA.fields]
    df_pass2_in = df_clean1.select(std_cols)
    df_pass2_in.createOrReplaceTempView("std_pass_2")

    df_clean2, rep2 = clean(spark, std_view="std_pass_2", view_name="clean_pass_2")

    assert rep2.summary["raw_rows_in"] == rep1.summary["clean_rows_out"]
    assert rep2.summary["clean_rows_out"] == rep1.summary["clean_rows_out"]
    assert rep2.summary["rows_removed"]["total_rows_removed"] == 0
    assert rep2.summary["values_changed"]["sentinels_to_null"] == {}
    assert rep2.summary["values_changed"]["out_of_range_to_null"] == {}
    assert rep2.summary["values_changed"]["interpolated"] == {}


def test_clean_report_json_serialization(tmp_path, spark, raw_dataset_slices):
    """
    Test 10: to_json writes valid JSON with steps, summary, and warnings.
    """
    header = list(raw_dataset_slices["header"])
    rows = [list(r) for r in raw_dataset_slices["bengaluru_rows"][:10]]

    file_path = tmp_path / "json_test.csv"
    with open(file_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)

    load_res = load_dataset(spark, file_path)
    df_clean, report = clean(spark, std_view=load_res.standardized_view)

    json_path = tmp_path / "report.json"
    report.to_json(json_path)

    assert json_path.exists()
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    assert "steps" in data and isinstance(data["steps"], list)
    assert "summary" in data and isinstance(data["summary"], dict)
    assert "warnings" in data and isinstance(data["warnings"], list)


@pytest.mark.skipif(not PRIMARY_DATASET_PATH.exists(), reason="Primary dataset not found")
def test_clean_raw_data_safety(spark):
    """
    Test 11: The standardized view and source CSV file are untouched after cleaning.
    """
    file_stat_before = PRIMARY_DATASET_PATH.stat()

    load_res = load_dataset(spark, PRIMARY_DATASET_PATH)
    rows_std_before = spark.sql(f"SELECT COUNT(*) AS c FROM {load_res.standardized_view}").collect()[0]["c"]
    temp_sum_before = spark.sql(f"SELECT ROUND(SUM(temperature), 2) AS s FROM {load_res.standardized_view}").collect()[0]["s"]

    # Run cleaning
    clean(spark, std_view=load_res.standardized_view)

    # Verify standardized view is identical
    rows_std_after = spark.sql(f"SELECT COUNT(*) AS c FROM {load_res.standardized_view}").collect()[0]["c"]
    temp_sum_after = spark.sql(f"SELECT ROUND(SUM(temperature), 2) AS s FROM {load_res.standardized_view}").collect()[0]["s"]

    assert rows_std_before == rows_std_after == 46752
    assert temp_sum_before == temp_sum_after

    # Verify CSV on disk is untouched
    file_stat_after = PRIMARY_DATASET_PATH.stat()
    assert file_stat_before.st_size == file_stat_after.st_size
    assert file_stat_before.st_mtime == file_stat_after.st_mtime
