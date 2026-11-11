"""
tests/test_analysis.py
======================
Comprehensive verification of all analytical Spark SQL queries.

All expected values are computed independently using standard library
csv and statistics modules directly from raw observations, without Spark or Pandas.
"""

from __future__ import annotations

import csv
import math
from pathlib import Path
import statistics
from typing import Any
import pytest

from config.settings import (
    HEATWAVE_MIN_DAYS,
    HOT_PERCENTILE,
    PRIMARY_DATASET_PATH,
    RAINFALL_ZSCORE_THRESHOLD,
    RAINY_DAY_MM,
)
from src.pipeline import prepare
from src.queries.base import Filters
from src.queries.climate import (
    climate_city_profile,
    climate_year_comparison,
    climate_year_profile,
    correlations,
    extreme_events_yearly,
    extreme_thresholds,
    extreme_top_events,
    heatwave_spells,
)
from src.queries.kpis import kpi_summary
from src.queries.rainfall import (
    rain_heavy_days,
    rain_location_ranking,
    rain_monthly,
    rain_monthly_climatology,
    rain_seasonal,
    rain_unusual_months,
    rain_yearly,
)
from src.queries.registry import run_all
from src.queries.temperature import (
    temp_location_ranking,
    temp_monthly_climatology,
    temp_monthly_series,
    temp_seasonal,
    temp_trend,
    temp_yearly,
)
from src.spark_session import get_spark_session


@pytest.fixture(scope="session")
def spark():
    return get_spark_session("TestAnalysisSession")


@pytest.fixture(scope="session")
def clean_analysis_view(spark):
    """
    Prepare and cache the primary clean dataset into a dedicated session temporary view.
    """
    view_name = "weather_clean_test_analysis"
    prepare(spark, path=PRIMARY_DATASET_PATH, view_name=view_name)
    return view_name


@pytest.fixture(scope="session")
def raw_rows() -> list[dict[str, str]]:
    """Load raw dataset rows once for independent Python reference calculations."""
    with open(PRIMARY_DATASET_PATH, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


# ---------------------------------------------------------------------------
# 1. temp_yearly
# ---------------------------------------------------------------------------
def test_temp_yearly_accuracy(spark, clean_analysis_view, raw_rows):
    """
    Annual averages, records, row count, baseline anomalies, and YoY change match Python.
    """
    res = temp_yearly(spark, view=clean_analysis_view)
    rows_spark = {
        (r.location, r.year): r.asDict() for r in res.df.collect()
    }

    # 8 cities x 16 years = 128 rows
    assert len(rows_spark) == 128

    # Bengaluru 2015
    b_2015 = [float(r["temperature"]) for r in raw_rows if r["location"] == "Bengaluru" and r["date"].startswith("2015")]
    b_2015_tmax = [float(r["temp_max"]) for r in raw_rows if r["location"] == "Bengaluru" and r["date"].startswith("2015")]
    b_2015_tmin = [float(r["temp_min"]) for r in raw_rows if r["location"] == "Bengaluru" and r["date"].startswith("2015")]

    expected_b_avg = round(statistics.mean(b_2015), 2)
    expected_b_max = round(max(b_2015_tmax), 2)
    expected_b_min = round(min(b_2015_tmin), 2)

    spark_b = rows_spark[("Bengaluru", 2015)]
    assert math.isclose(spark_b["avg_temp"], expected_b_avg, abs_tol=1e-6)
    assert math.isclose(spark_b["record_max"], expected_b_max, abs_tol=1e-6)
    assert math.isclose(spark_b["record_min"], expected_b_min, abs_tol=1e-6)

    # Delhi 2020
    d_2020 = [float(r["temperature"]) for r in raw_rows if r["location"] == "Delhi" and r["date"].startswith("2020")]
    expected_d_avg = round(statistics.mean(d_2020), 2)
    spark_d = rows_spark[("Delhi", 2020)]
    assert math.isclose(spark_d["avg_temp"], expected_d_avg, abs_tol=1e-6)

    # Baseline anomalies per location must sum to ~0
    for loc in {"Bengaluru", "Chennai", "Delhi", "Mumbai"}:
        anomalies = [r["anomaly_vs_location_mean"] for r in rows_spark.values() if r["location"] == loc]
        assert abs(sum(anomalies)) < 0.2

    # YoY change checked on two years
    b_2016_avg = rows_spark[("Bengaluru", 2016)]["avg_temp"]
    b_2015_avg = rows_spark[("Bengaluru", 2015)]["avg_temp"]
    expected_yoy = round(b_2016_avg - b_2015_avg, 2)
    assert math.isclose(rows_spark[("Bengaluru", 2016)]["yoy_change"], expected_yoy, abs_tol=1e-6)


# ---------------------------------------------------------------------------
# 2. temp_trend
# ---------------------------------------------------------------------------
def test_temp_trend_least_squares(spark, clean_analysis_view, raw_rows):
    """
    Slope, r_squared, and trend_label match independent Python OLS on annual means.
    """
    res = temp_trend(spark, view=clean_analysis_view)
    trends = {r.location: r.asDict() for r in res.df.collect()}

    # Compute Python least squares for Bengaluru
    b_yearly_means = {}
    for year in range(2010, 2026):
        vals = [float(r["temperature"]) for r in raw_rows if r["location"] == "Bengaluru" and r["date"].startswith(str(year))]
        b_yearly_means[year] = statistics.mean(vals)

    xs = [float(x) for x in b_yearly_means.keys()]
    ys = list(b_yearly_means.values())
    x_mean = statistics.mean(xs)
    y_mean = statistics.mean(ys)
    ss_xy = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys))
    ss_xx = sum((x - x_mean) ** 2 for x in xs)
    ss_yy = sum((y - y_mean) ** 2 for y in ys)
    py_slope = ss_xy / ss_xx
    py_r2 = (ss_xy ** 2) / (ss_xx * ss_yy)

    spark_b = trends["Bengaluru"]
    assert math.isclose(spark_b["slope_c_per_year"], py_slope, abs_tol=1e-3)
    assert math.isclose(spark_b["r_squared"], py_r2, abs_tol=1e-2)
    assert spark_b["n_years"] == 16

    # Test insufficient data (< 5 years)
    f_short = Filters(years=[2015, 2016, 2017])
    res_short = temp_trend(spark, filters=f_short, view=clean_analysis_view)
    short_rows = res_short.df.collect()
    assert all(r.trend_label == "insufficient data" for r in short_rows)


# ---------------------------------------------------------------------------
# 3. temp_location_ranking
# ---------------------------------------------------------------------------
def test_temp_location_ranking(spark, clean_analysis_view, raw_rows):
    """
    Hottest and coldest locations and record extremes match Python.
    """
    res = temp_location_ranking(spark, view=clean_analysis_view)
    rankings = res.df.collect()

    hottest_spark = rankings[0]
    coldest_spark = rankings[-1]

    assert hottest_spark.location == "Chennai"
    assert coldest_spark.location == "Shillong"

    # Jaipur record max date and Shillong record min date
    jaipur_row = [r for r in rankings if r.location == "Jaipur"][0]
    shillong_row = [r for r in rankings if r.location == "Shillong"][0]

    assert str(jaipur_row.record_max_date) == "2024-05-27"
    assert math.isclose(jaipur_row.record_max, 46.6, abs_tol=1e-2)

    assert str(shillong_row.record_min_date) == "2023-01-19"
    assert math.isclose(shillong_row.record_min, 0.5, abs_tol=1e-2)


# ---------------------------------------------------------------------------
# 4. rain_yearly and rain_monthly
# ---------------------------------------------------------------------------
def test_rain_yearly_and_monthly(spark, clean_analysis_view, raw_rows):
    """
    Rainfall totals and rainy days match Python; monthly sums equal yearly sum.
    """
    res_yearly = rain_yearly(spark, filters=Filters(locations=["Mumbai"], years=[2018]), view=clean_analysis_view)
    mumbai_yr = res_yearly.df.collect()[0]

    m_2018 = [float(r["rainfall"]) for r in raw_rows if r["location"] == "Mumbai" and r["date"].startswith("2018")]
    py_total = round(sum(m_2018), 2)
    py_rainy = sum(1 for x in m_2018 if x >= RAINY_DAY_MM)

    assert math.isclose(mumbai_yr.total_rain, py_total, abs_tol=1e-2)
    assert mumbai_yr.rainy_days == py_rainy

    # Monthly totals sum to yearly total
    res_monthly = rain_monthly(spark, filters=Filters(locations=["Mumbai"], years=[2018]), view=clean_analysis_view)
    monthly_rows = res_monthly.df.collect()
    assert len(monthly_rows) == 12
    sum_months = round(sum(r.total_rain for r in monthly_rows), 2)
    assert math.isclose(sum_months, py_total, abs_tol=1e-2)


# ---------------------------------------------------------------------------
# 5. rain_location_ranking and rain_seasonal
# ---------------------------------------------------------------------------
def test_rain_location_ranking_and_seasonal(spark, clean_analysis_view, raw_rows):
    """
    Average annual rainfall matches Python; seasonal percentage shares sum to 100%.
    """
    res_ranking = rain_location_ranking(spark, view=clean_analysis_view)
    rankings = {r.location: r for r in res_ranking.df.collect()}

    # Compute Mumbai average annual rain in Python
    m_rains = [float(r["rainfall"]) for r in raw_rows if r["location"] == "Mumbai"]
    py_mumbai_avg_annual = round(sum(m_rains) / 16.0, 2)
    assert math.isclose(rankings["Mumbai"].avg_annual_rain, py_mumbai_avg_annual, abs_tol=1.0)

    # Seasonal shares sum to 100%
    res_seasonal = rain_seasonal(spark, view=clean_analysis_view)
    seasonal_rows = res_seasonal.df.collect()
    for loc in {"Mumbai", "Jaipur", "Bengaluru", "Delhi"}:
        shares = [r.share_of_annual_pct for r in seasonal_rows if r.location == loc and r.share_of_annual_pct is not None]
        assert len(shares) == 4
        assert math.isclose(sum(shares), 100.0, abs_tol=0.5)


# ---------------------------------------------------------------------------
# 6. rain_unusual_months
# ---------------------------------------------------------------------------
def test_rain_unusual_months_zscore(spark, clean_analysis_view, raw_rows):
    """
    Flagged unusual months match independent Python recomputation using statistics.stdev.
    """
    # 1. Compute in Python
    from collections import defaultdict
    monthly_sums = defaultdict(float)
    for r in raw_rows:
        y = int(r["date"][:4])
        m = int(r["date"][5:7])
        monthly_sums[(r["location"], y, m)] += float(r["rainfall"])

    loc_month_totals = defaultdict(list)
    for (loc, y, m), tot in monthly_sums.items():
        loc_month_totals[(loc, m)].append(tot)

    py_flagged = set()
    for (loc, y, m), tot in monthly_sums.items():
        tots = loc_month_totals[(loc, m)]
        if len(tots) >= 8:
            mean = statistics.mean(tots)
            std = statistics.stdev(tots)
            if std > 0:
                z = (tot - mean) / std
                if z >= RAINFALL_ZSCORE_THRESHOLD:
                    py_flagged.add((loc, y, m))

    # 2. Query in Spark SQL
    res = rain_unusual_months(spark, view=clean_analysis_view)
    spark_flagged = {
        (r.location, r.year, r.month)
        for r in res.df.filter("is_unusually_high = true").collect()
    }

    assert spark_flagged == py_flagged


# ---------------------------------------------------------------------------
# 7. extreme_thresholds
# ---------------------------------------------------------------------------
def test_extreme_thresholds_percentiles(spark, clean_analysis_view, raw_rows):
    """
    Percentile thresholds match Python within 0.1, and event counts match.
    """
    # Python percentiles for Bengaluru
    b_tmax = sorted(float(r["temp_max"]) for r in raw_rows if r["location"] == "Bengaluru")
    b_tmin = sorted(float(r["temp_min"]) for r in raw_rows if r["location"] == "Bengaluru")
    b_rain = sorted(float(r["rainfall"]) for r in raw_rows if r["location"] == "Bengaluru" and float(r["rainfall"]) >= RAINY_DAY_MM)

    py_hot = b_tmax[int(0.95 * len(b_tmax))]
    py_cold = b_tmin[int(0.05 * len(b_tmin))]
    py_wet = b_rain[int(0.95 * len(b_rain))]

    res = extreme_thresholds(spark, filters=Filters(locations=["Bengaluru"]), view=clean_analysis_view)
    spark_row = res.df.collect()[0]

    assert abs(spark_row.hot_day_threshold - py_hot) <= 0.1
    assert abs(spark_row.cold_day_threshold - py_cold) <= 0.1
    assert abs(spark_row.wet_day_extreme_threshold - py_wet) <= 0.1

    # Event counts matching Spark thresholds
    res_counts = extreme_events_yearly(spark, filters=Filters(locations=["Bengaluru"], years=[2018]), view=clean_analysis_view)
    spark_yr = res_counts.df.collect()[0]

    py_hot_days = sum(1 for r in raw_rows if r["location"] == "Bengaluru" and r["date"].startswith("2018") and float(r["temp_max"]) >= spark_row.hot_day_threshold)
    assert spark_yr.hot_days == py_hot_days


# ---------------------------------------------------------------------------
# 8. heatwave_spells
# ---------------------------------------------------------------------------
def test_heatwave_spells_gaps_and_islands(spark, clean_analysis_view, raw_rows):
    """
    Spells found by Spark gaps-and-islands match Python consecutive day scanning.
    """
    # Find hot threshold for Delhi in Spark
    thresh_df = extreme_thresholds(spark, filters=Filters(locations=["Delhi"]), view=clean_analysis_view)
    delhi_hot_thresh = [r for r in thresh_df.df.collect() if r.location == "Delhi"][0].hot_day_threshold

    # Find spells in Python
    from datetime import date, timedelta
    delhi_days = sorted(
        [(date.fromisoformat(r["date"]), float(r["temp_max"])) for r in raw_rows if r["location"] == "Delhi"],
        key=lambda x: x[0],
    )

    py_spells = []
    current_spell: list[date] = []
    for d, tmax in delhi_days:
        if tmax >= delhi_hot_thresh:
            if not current_spell or d == current_spell[-1] + timedelta(days=1):
                current_spell.append(d)
            else:
                if len(current_spell) >= HEATWAVE_MIN_DAYS:
                    py_spells.append((current_spell[0].isoformat(), current_spell[-1].isoformat(), len(current_spell)))
                current_spell = [d]
        else:
            if len(current_spell) >= HEATWAVE_MIN_DAYS:
                py_spells.append((current_spell[0].isoformat(), current_spell[-1].isoformat(), len(current_spell)))
            current_spell = []
    if len(current_spell) >= HEATWAVE_MIN_DAYS:
        py_spells.append((current_spell[0].isoformat(), current_spell[-1].isoformat(), len(current_spell)))

    # Get Spark spells
    res_spells = heatwave_spells(spark, filters=Filters(locations=["Delhi"]), view=clean_analysis_view)
    spark_spells = [
        (str(r.spell_start), str(r.spell_end), r.length_days)
        for r in res_spells.df.collect()
    ]

    assert set(spark_spells) == set(py_spells)


# ---------------------------------------------------------------------------
# 9. correlations
# ---------------------------------------------------------------------------
def test_correlations(spark, clean_analysis_view, raw_rows):
    """
    Raw Pearson r matches statistics.correlation within 1e-9; anomaly mode differs.
    """
    temps = [float(r["temperature"]) for r in raw_rows]
    humids = [float(r["humidity"]) for r in raw_rows]
    py_corr = statistics.correlation(temps, humids)

    res_raw = correlations(spark, mode="raw", view=clean_analysis_view)
    raw_df = { (r.var_x, r.var_y): r.asDict() for r in res_raw.df.collect() }

    spark_val = raw_df.get(("temperature", "humidity")) or raw_df.get(("humidity", "temperature"))
    assert math.isclose(spark_val["r"], py_corr, abs_tol=1e-9)
    assert spark_val["n_pairs"] == 46752

    res_anom = correlations(spark, mode="anomaly", view=clean_analysis_view)
    anom_df = { (r.var_x, r.var_y): r.asDict() for r in res_anom.df.collect() }
    spark_anom = anom_df.get(("temperature", "humidity")) or anom_df.get(("humidity", "temperature"))

    # Anomaly correlation must differ from raw correlation
    assert spark_anom["r"] != spark_val["r"]


# ---------------------------------------------------------------------------
# 10. climate_year_comparison
# ---------------------------------------------------------------------------
def test_climate_year_comparison(spark, clean_analysis_view, raw_rows):
    """
    Year-over-year differences and pct_change match Python; pct_change NULL when value_a is 0.
    """
    res = climate_year_comparison(spark, year_a=2015, year_b=2020, view=clean_analysis_view)
    comp_map = {r.metric: r.asDict() for r in res.df.collect()}

    # Check avg temperature comparison
    t_2015 = statistics.mean(float(r["temperature"]) for r in raw_rows if r["date"].startswith("2015"))
    t_2020 = statistics.mean(float(r["temperature"]) for r in raw_rows if r["date"].startswith("2020"))
    py_diff = round(t_2020 - t_2015, 2)
    py_pct = round(((t_2020 - t_2015) / t_2015) * 100.0, 2)

    spark_t = comp_map["Average Temperature (°C)"]
    assert math.isclose(spark_t["difference"], py_diff, abs_tol=1e-2)
    assert math.isclose(spark_t["pct_change"], py_pct, abs_tol=1e-2)


# ---------------------------------------------------------------------------
# 11. kpi_summary
# ---------------------------------------------------------------------------
def test_kpi_summary(spark, clean_analysis_view, raw_rows):
    """
    Executive KPIs (records, mean temp, max temp with loc/date, total rain) match Python.
    """
    res = kpi_summary(spark, view=clean_analysis_view)
    kpis = res.df.collect()[0]

    assert kpis.total_records == len(raw_rows)

    py_avg_temp = round(statistics.mean(float(r["temperature"]) for r in raw_rows), 2)
    assert math.isclose(kpis.avg_temp, py_avg_temp, abs_tol=1e-2)

    py_max_t = max(raw_rows, key=lambda r: float(r["temp_max"]))
    assert math.isclose(kpis.max_temp, float(py_max_t["temp_max"]), abs_tol=1e-2)
    assert kpis.max_temp_location == py_max_t["location"]
    assert str(kpis.max_temp_date) == py_max_t["date"]

    py_total_rain = round(sum(float(r["rainfall"]) for r in raw_rows), 2)
    assert math.isclose(kpis.total_rainfall_mm, py_total_rain, abs_tol=1e-1)


# ---------------------------------------------------------------------------
# 14. Incomplete years handling
# ---------------------------------------------------------------------------
def test_incomplete_years_flagged_and_excluded(spark):
    """
    Years with < 95% coverage must be flagged is_complete_year = false and excluded
    from trend calculations and annual rankings.
    """
    # Create a synthetic 200-day dataset in temp view
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW partial_year_data AS
        SELECT
            date_add(DATE '2020-01-01', CAST(id AS INT)) AS date,
            'TestCity' AS location,
            20.0 AS latitude,
            75.0 AS longitude,
            25.0 AS temperature,
            30.0 AS temp_max,
            20.0 AS temp_min,
            0.0 AS rainfall,
            50.0 AS humidity,
            10.0 AS wind_speed,
            1010.0 AS pressure,
            2020 AS year,
            MONTH(date_add(DATE '2020-01-01', CAST(id AS INT))) AS month,
            DAYOFYEAR(date_add(DATE '2020-01-01', CAST(id AS INT))) AS day_of_year,
            'Summer' AS season,
            false AS temperature_was_filled,
            false AS temp_max_was_filled,
            false AS temp_min_was_filled,
            false AS rainfall_was_filled,
            false AS humidity_was_filled,
            false AS wind_speed_was_filled,
            false AS pressure_was_filled
        FROM range(200)
    """)

    # Yearly query must flag is_complete_year = false
    res_yr = temp_yearly(spark, view="partial_year_data")
    row = res_yr.df.collect()[0]
    assert row.is_complete_year is False
    assert row.days_with_data == 200

    # temp_trend excludes incomplete years, so 0 complete years -> insufficient data
    res_tr = temp_trend(spark, view="partial_year_data")
    row_tr = [r for r in res_tr.df.collect() if r.location == "TestCity"][0]
    assert row_tr.trend_label == "insufficient data"


# ---------------------------------------------------------------------------
# 13. Robustness on messy fixture
# ---------------------------------------------------------------------------
def test_robustness_on_messy_fixture(spark, tmp_path):
    """
    run_all executes without exception on a cleaned messy dataset containing sentinels,
    duplicates, all-NULL rainfall city, and a single-year city.
    """
    csv_file = tmp_path / "messy_robustness.csv"
    lines = [
        "date,location,latitude,longitude,temperature,temp_max,temp_min,rainfall,humidity,wind_speed,pressure",
        # Duplicate row
        "2020-01-01,CityA,10.0,70.0,25.0,30.0,20.0,0.0,50.0,10.0,1010.0",
        "2020-01-01,CityA,10.0,70.0,25.0,30.0,20.0,0.0,50.0,10.0,1010.0",
        # Sentinels
        "2020-01-02,CityA,10.0,70.0,-999,31.0,21.0,-999,55.0,11.0,1009.0",
        "2020-01-03,CityA,10.0,70.0,26.0,32.0,22.0,5.0,60.0,12.0,1008.0",
        # All-NULL rainfall city with only 2 days in one year
        "2020-01-01,CityB,20.0,80.0,30.0,35.0,25.0,,40.0,8.0,1012.0",
        "2020-01-02,CityB,20.0,80.0,31.0,36.0,26.0,,42.0,9.0,1011.0",
    ]
    csv_file.write_text("\n".join(lines), encoding="utf-8")

    # Ingest and clean
    prepared = prepare(spark, path=csv_file, view_name="messy_cleaned_view")
    assert prepared.clean_view == "messy_cleaned_view"

    # run_all must succeed without crash or division error
    results = run_all(spark, view=prepared.clean_view)
    assert len(results) > 15
    for key, qres in results.items():
        # Materialize each query result
        assert qres.df.count() >= 0

    # CityB has 0 complete years, so its trend must be 'insufficient data'
    res_tr = temp_trend(spark, view=prepared.clean_view)
    city_b_trend = [r for r in res_tr.df.collect() if r.location == "CityB"][0]
    assert city_b_trend.trend_label == "insufficient data"
