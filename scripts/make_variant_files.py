"""
scripts/make_variant_files.py
=============================
Generates 10 dataset variants (V1 through V10) in %TEMP%/weather_variants/
using real historical rows sliced from data/raw/india_weather_daily.csv.
No synthetic weather data is generated.

Variants:
  V1: Headers Date;City;Temp;Rain;RH;Wind;Pres, semicolon delimiter, UTF-8 BOM,
      dates dd/MM/yyyy, 3 cities, 2018-2022.
  V2: No location column (single city Bengaluru, all years) with a default location.
  V3: Temperature in Kelvin (add 273.15 to the real value; label: unit test only).
  V4: One city, one year (365 rows: Bengaluru 2021).
  V5: Two cities, three years (Bengaluru & Mumbai 2020-2022; trend: insufficient data).
  V6: Rainfall column entirely empty for one city (Mumbai rainfall is empty).
  V7: 10% of rows deleted (every 10th row deterministic), plus 3 gaps of 5+ days removed.
  V8: Location names with mixed case and surrounding spaces ("mumbai", " Mumbai ").
  V9: Dates in yyyyMMdd, column names with spaces and quotes.
  V10: Duplicate header names (triggers friendly DatasetLoadError).
"""

from __future__ import annotations

import csv
import os
import sys
import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import PRIMARY_DATASET_PATH


def make_variants(output_dir: Path | None = None) -> dict[str, Path]:
    if output_dir is None:
        target_dir = Path(tempfile.gettempdir()) / "weather_variants"
    else:
        target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    with open(PRIMARY_DATASET_PATH, mode="r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        all_rows = list(reader)

    # Helper indexes
    idx_map = {col: i for i, col in enumerate(header)}
    date_i = idx_map["date"]
    loc_i = idx_map["location"]
    temp_i = idx_map["temperature"]
    tmax_i = idx_map["temp_max"]
    tmin_i = idx_map["temp_min"]
    rain_i = idx_map["rainfall"]
    hum_i = idx_map["humidity"]
    wind_i = idx_map["wind_speed"]
    press_i = idx_map["pressure"]

    created_files: dict[str, Path] = {}

    # ─────────────────────────────────────────────────────────────────────────
    # V1: Semicolon delimited, UTF-8 BOM, dd/MM/yyyy dates, 3 cities (2018-2022)
    # Headers: Date;City;Temp;Rain;RH;Wind;Pres
    # ─────────────────────────────────────────────────────────────────────────
    v1_header = ["Date", "City", "Temp", "Rain", "RH", "Wind", "Pres"]
    v1_target_cities = {"Bengaluru", "Mumbai", "Delhi"}
    v1_target_years = {"2018", "2019", "2020", "2021", "2022"}
    v1_rows = []
    for r in all_rows:
        if r[loc_i] in v1_target_cities and r[date_i][:4] in v1_target_years:
            d_parts = r[date_i].split("-")
            d_formatted = f"{d_parts[2]}/{d_parts[1]}/{d_parts[0]}"
            v1_rows.append([
                d_formatted, r[loc_i], r[temp_i], r[rain_i],
                r[hum_i], r[wind_i], r[press_i]
            ])
    p_v1 = target_dir / "v1_semicolon_bom_ddmmyyyy.csv"
    with open(p_v1, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(v1_header)
        w.writerows(v1_rows)
    created_files["V1"] = p_v1

    # ─────────────────────────────────────────────────────────────────────────
    # V2: No location column (single city Bengaluru, all years)
    # ─────────────────────────────────────────────────────────────────────────
    v2_header = [col for col in header if col != "location"]
    v2_rows = [
        [val for i, val in enumerate(r) if i != loc_i]
        for r in all_rows if r[loc_i] == "Bengaluru"
    ]
    p_v2 = target_dir / "v2_single_city_no_location_col.csv"
    with open(p_v2, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(v2_header)
        w.writerows(v2_rows)
    created_files["V2"] = p_v2

    # ─────────────────────────────────────────────────────────────────────────
    # V3: Temperature in Kelvin (add 273.15 to real value; unit test only)
    # ─────────────────────────────────────────────────────────────────────────
    v3_rows = []
    for r in all_rows[:500]:
        r_copy = list(r)
        c_val = float(r[temp_i])
        k_val = round(c_val + 273.15, 2)
        r_copy[temp_i] = str(k_val)
        v3_rows.append(r_copy)
    p_v3 = target_dir / "v3_kelvin_unit_mismatch.csv"
    with open(p_v3, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(v3_rows)
    created_files["V3"] = p_v3

    # ─────────────────────────────────────────────────────────────────────────
    # V4: One city, one year (365 rows: Bengaluru 2021)
    # ─────────────────────────────────────────────────────────────────────────
    v4_rows = [
        r for r in all_rows
        if r[loc_i] == "Bengaluru" and r[date_i].startswith("2021")
    ]
    p_v4 = target_dir / "v4_one_city_one_year_365.csv"
    with open(p_v4, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(v4_rows)
    created_files["V4"] = p_v4

    # ─────────────────────────────────────────────────────────────────────────
    # V5: Two cities, three years (Bengaluru & Mumbai 2020-2022; trend insufficient)
    # ─────────────────────────────────────────────────────────────────────────
    v5_rows = [
        r for r in all_rows
        if r[loc_i] in ("Bengaluru", "Mumbai") and r[date_i][:4] in ("2020", "2021", "2022")
    ]
    p_v5 = target_dir / "v5_two_cities_three_years.csv"
    with open(p_v5, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(v5_rows)
    created_files["V5"] = p_v5

    # ─────────────────────────────────────────────────────────────────────────
    # V6: Rainfall column entirely empty for one city (Mumbai)
    # ─────────────────────────────────────────────────────────────────────────
    v6_rows = []
    for r in all_rows:
        if r[loc_i] in ("Bengaluru", "Mumbai") and r[date_i][:4] in ("2020", "2021"):
            r_copy = list(r)
            if r[loc_i] == "Mumbai":
                r_copy[rain_i] = ""
            v6_rows.append(r_copy)
    p_v6 = target_dir / "v6_rainfall_empty_for_one_city.csv"
    with open(p_v6, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(v6_rows)
    created_files["V6"] = p_v6

    # ─────────────────────────────────────────────────────────────────────────
    # V7: 10% rows deleted (every 10th row) + 3 gaps of 5+ days removed
    # ─────────────────────────────────────────────────────────────────────────
    bengaluru_rows = [
        r for r in all_rows
        if r[loc_i] == "Bengaluru" and r[date_i][:4] in ("2020", "2021")
    ]
    v7_rows = []
    # 3 gap ranges (indices 50..55, 150..156, 300..306)
    gap_indices = set(range(50, 56)) | set(range(150, 157)) | set(range(300, 307))
    for idx, r in enumerate(bengaluru_rows):
        if idx in gap_indices:
            continue
        if idx % 10 == 0:
            continue
        v7_rows.append(r)
    p_v7 = target_dir / "v7_deleted_rows_and_long_gaps.csv"
    with open(p_v7, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(v7_rows)
    created_files["V7"] = p_v7

    # ─────────────────────────────────────────────────────────────────────────
    # V8: Location names with mixed case and surrounding spaces ("mumbai", " Mumbai ")
    # ─────────────────────────────────────────────────────────────────────────
    v8_rows = []
    v8_variants = ["Mumbai", "mumbai", " Mumbai ", " MUMBAI ", " mumbai "]
    mumbai_rows = [r for r in all_rows if r[loc_i] == "Mumbai"][:400]
    for idx, r in enumerate(mumbai_rows):
        r_copy = list(r)
        r_copy[loc_i] = v8_variants[idx % len(v8_variants)]
        v8_rows.append(r_copy)
    p_v8 = target_dir / "v8_mixed_case_spaces_location.csv"
    with open(p_v8, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(v8_rows)
    created_files["V8"] = p_v8

    # ─────────────────────────────────────────────────────────────────────────
    # V9: Dates in yyyyMMdd, column names with spaces and quotes
    # ─────────────────────────────────────────────────────────────────────────
    v9_header = [
        ' Observation Date ', ' City Name ', ' Mean Temp ', ' Precipitation ',
        ' Relative Humidity ', ' Wind Velocity ', ' Atmospheric Pressure '
    ]
    v9_rows = []
    for r in all_rows[:400]:
        if r[loc_i] in ("Bengaluru", "Delhi"):
            d_compact = r[date_i].replace("-", "")  # yyyyMMdd
            v9_rows.append([
                d_compact, r[loc_i], r[temp_i], r[rain_i],
                r[hum_i], r[wind_i], r[press_i]
            ])
    p_v9 = target_dir / "v9_yyyymmdd_spaced_quoted_headers.csv"
    with open(p_v9, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, quoting=csv.QUOTE_ALL)
        w.writerow(v9_header)
        w.writerows(v9_rows)
    created_files["V9"] = p_v9

    # ─────────────────────────────────────────────────────────────────────────
    # V10: Duplicate header names -> DatasetLoadError
    # ─────────────────────────────────────────────────────────────────────────
    v10_header = [
        "date", "location", "temperature", "rainfall",
        "humidity", "wind_speed", "temperature"
    ]
    v10_rows = [
        [r[date_i], r[loc_i], r[temp_i], r[rain_i], r[hum_i], r[wind_i], r[temp_i]]
        for r in all_rows[:10]
    ]
    p_v10 = target_dir / "v10_duplicate_headers.csv"
    with open(p_v10, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(v10_header)
        w.writerows(v10_rows)
    created_files["V10"] = p_v10

    print("Generated 10 variant CSV files in temp directory (%TEMP%/weather_variants):")
    for k, p in created_files.items():
        print(f"  {k}: {p.name} ({p.stat().st_size} bytes)")
    return created_files


if __name__ == "__main__":
    make_variants()
