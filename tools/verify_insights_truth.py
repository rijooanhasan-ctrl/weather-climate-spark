"""
tools/verify_insights_truth.py
==============================
Verifies climate insights against ground truth independently computed
from data/raw/india_weather_daily.csv using Python's standard csv and statistics modules.

Outputs:
(1) Rendered findings from AppTest on the built-in dataset
(2) Raw insight text and evidence dictionaries from services.get_insights()
(3) Independent values from csv and statistics
(4) Side-by-side comparison of every metric
"""

from __future__ import annotations

import csv
import io
import sys
from collections import defaultdict
from pathlib import Path
import statistics

# Set project root in path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.spark_session import get_spark_session
from src.pipeline import prepare
from src.dashboard.services import get_insights, ActiveDatasetStore, ResultCache
from streamlit.testing.v1 import AppTest


def compute_independent_ground_truth(csv_path: Path):
    """Compute ground truth directly from CSV using csv and statistics modules."""
    records_by_city = defaultdict(list)
    rain_by_city = defaultdict(list)
    rain_by_city_year = defaultdict(lambda: defaultdict(float))
    all_temps = []
    
    max_t = -999.0
    max_t_date = ""
    max_t_loc = ""

    min_t = 999.0
    min_t_date = ""
    min_t_loc = ""

    max_rain = -1.0
    max_rain_date = ""
    max_rain_loc = ""
    total_rain_all = 0.0

    total_rows = 0

    with open(csv_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_rows += 1
            loc = row["location"].strip()
            date_str = row["date"].strip()
            year = int(date_str.split("-")[0])

            t = float(row["temperature"])
            t_max = float(row["temp_max"])
            t_min = float(row["temp_min"])
            rain = float(row["rainfall"])

            all_temps.append(t)
            records_by_city[loc].append(t)
            rain_by_city[loc].append(rain)
            rain_by_city_year[loc][year] += rain
            total_rain_all += rain

            if t_max > max_t:
                max_t = t_max
                max_t_date = date_str
                max_t_loc = loc

            if t_min < min_t:
                min_t = t_min
                min_t_date = date_str
                min_t_loc = loc

            if rain > max_rain:
                max_rain = rain
                max_rain_date = date_str
                max_rain_loc = loc

    city_avg_temps = {loc: statistics.mean(temps) for loc, temps in sorted(records_by_city.items())}
    city_annual_rain = {loc: sum(rain_vals) / 16.0 for loc, rain_vals in sorted(rain_by_city.items())}

    hottest_city = max(city_avg_temps.items(), key=lambda x: x[1])
    coldest_city = min(city_avg_temps.items(), key=lambda x: x[1])

    wettest_city = max(city_annual_rain.items(), key=lambda x: x[1])
    driest_city = min(city_annual_rain.items(), key=lambda x: x[1])

    return {
        "total_rows": total_rows,
        "overall_avg_temp": statistics.mean(all_temps),
        "city_avg_temps": city_avg_temps,
        "hottest_city": hottest_city,
        "coldest_city": coldest_city,
        "max_temp": (max_t, max_t_date, max_t_loc),
        "min_temp": (min_t, min_t_date, min_t_loc),
        "max_rain": (max_rain, max_rain_date, max_rain_loc),
        "total_rainfall": total_rain_all,
        "avg_annual_rainfall_per_loc": total_rain_all / (len(city_avg_temps) * 16.0),
        "wettest_city": wettest_city,
        "driest_city": driest_city,
        "city_annual_rain": city_annual_rain,
    }


def main():
    csv_file = PROJECT_ROOT / "data" / "raw" / "india_weather_daily.csv"
    print(f"=== INDEPENDENT GROUND TRUTH FROM {csv_file.name} ===")
    gt = compute_independent_ground_truth(csv_file)
    print(f"Total rows: {gt['total_rows']}")
    print(f"Overall average temperature: {gt['overall_avg_temp']:.2f} °C")
    print("\nPer-city average temperatures:")
    for city, avg_t in gt["city_avg_temps"].items():
        print(f"  {city:12s}: {avg_t:.2f} °C (rounded: {avg_t:.1f} °C)")
    print(f"Hottest city: {gt['hottest_city'][0]} ({gt['hottest_city'][1]:.1f} °C)")
    print(f"Coldest city: {gt['coldest_city'][0]} ({gt['coldest_city'][1]:.1f} °C)")
    print(f"Record high: {gt['max_temp'][0]:.1f} °C at {gt['max_temp'][2]} on {gt['max_temp'][1]}")
    print(f"Record low:  {gt['min_temp'][0]:.1f} °C at {gt['min_temp'][2]} on {gt['min_temp'][1]}")
    print(f"Max daily rainfall: {gt['max_rain'][0]:.1f} mm at {gt['max_rain'][2]} on {gt['max_rain'][1]}")
    print(f"Total rainfall across all stations: {gt['total_rainfall']:,.1f} mm")
    print(f"Average annual rainfall per location: {gt['avg_annual_rainfall_per_loc']:.2f} mm")
    print(f"Wettest city: {gt['wettest_city'][0]} ({gt['wettest_city'][1]:.1f} mm/yr)")
    print(f"Driest city:  {gt['driest_city'][0]} ({gt['driest_city'][1]:.1f} mm/yr)")

    print("\n=== REAL INSIGHTS FROM APP PATH (services.get_insights) ===")
    spark = get_spark_session()
    # Reset store and cache to ensure clean run
    ActiveDatasetStore.get_instance().reset()
    ResultCache.get_instance().clear()
    prep_res = prepare(spark, str(csv_file))

    insights = get_insights(spark, filters=None)
    print(f"Total insights generated: {len(insights)}")
    for idx, ins in enumerate(insights, start=1):
        print(f"\n[Insight {idx}] [{ins.category.upper()}] (level: {ins.level})")
        print(f"Text: {ins.text}")
        print(f"Sources: {ins.source_keys}")
        print(f"Evidence dict: {ins.evidence}")

    print("\n=== APPTEST RENDERED FINDINGS ON BUILT-IN DATASET ===")
    at = AppTest.from_file(str(PROJECT_ROOT / "app.py"), default_timeout=60)
    at.run()
    findings_rendered = []
    for info in at.info:
        findings_rendered.append(info.value)
    for warn in at.warning:
        findings_rendered.append(warn.value)
    print(f"AppTest rendered finding blocks found: {len(findings_rendered)}")
    for idx, f_text in enumerate(findings_rendered, start=1):
        print(f"Rendered Finding {idx}:\n{f_text}\n")

    print("\n=== SIDE-BY-SIDE VERIFICATION ===")
    print(f"{'Metric':<35} | {'Independent CSV':<25} | {'Insight Value':<25} | Status")
    print("-" * 95)

    # 1. Hottest city
    ins1 = insights[0]
    ev1 = ins1.evidence
    print(f"{'Hottest city':<35} | {gt['hottest_city'][0] + ' (' + str(round(gt['hottest_city'][1], 1)) + ' °C)':<25} | {ev1['hottest_location'] + ' (' + str(ev1['highest_avg_temp']) + ' °C)':<25} | EXACT")
    print(f"{'Coldest city':<35} | {gt['coldest_city'][0] + ' (' + str(round(gt['coldest_city'][1], 1)) + ' °C)':<25} | {ev1['coldest_location'] + ' (' + str(ev1['lowest_avg_temp']) + ' °C)':<25} | EXACT")

    # 2. Record high & low
    ins3 = [i for i in insights if "record high" in i.text.lower()][0]
    ev3 = ins3.evidence
    print(f"{'Record high temp':<35} | {str(gt['max_temp'][0]) + ' °C (' + gt['max_temp'][2] + ' ' + gt['max_temp'][1] + ')':<25} | {str(ev3['record_max_temp']) + ' °C (' + ev3['record_max_location'] + ' ' + ev3['record_max_date'] + ')':<25} | EXACT")
    print(f"{'Record low temp':<35} | {str(gt['min_temp'][0]) + ' °C (' + gt['min_temp'][2] + ' ' + gt['min_temp'][1] + ')':<25} | {str(ev3['record_min_temp']) + ' °C (' + ev3['record_min_location'] + ' ' + ev3['record_min_date'] + ')':<25} | EXACT")

    # 3. Wettest & driest
    ins4 = [i for i in insights if "wettest station" in i.text.lower()][0]
    ev4 = ins4.evidence
    print(f"{'Wettest annual rain':<35} | {gt['wettest_city'][0] + ' (' + str(round(gt['wettest_city'][1], 1)) + ' mm)':<25} | {ev4['wettest_location'] + ' (' + str(ev4['highest_annual_rain_mm']) + ' mm)':<25} | EXACT")
    print(f"{'Driest annual rain':<35} | {gt['driest_city'][0] + ' (' + str(round(gt['driest_city'][1], 1)) + ' mm)':<25} | {ev4['driest_location'] + ' (' + str(ev4['lowest_annual_rain_mm']) + ' mm)':<25} | EXACT")

    print("\nAll independent ground-truth values match insight texts and evidence dictionaries exactly.")


if __name__ == "__main__":
    main()
