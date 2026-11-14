"""
tools/compare_validators.py
===========================
Phase 10 A5: Validator Comparison
1. Displays the source of the merged validator checks in src/validator.py.
2. Compares previous validator (commit f6cffb8) loaded from tools.validator_old
   against current validator on:
   - 402-row defect fixture
   - Variant V3 (Kelvin unit mismatch)
   - Variant V6 (Rainfall empty for one city)
   - Real dataset (data/raw/india_weather_daily.csv)
3. Compares complete ValidationReport (errors, warnings, info, column_stats)
   including all 7 numeric columns:
   temperature, temp_max, temp_min, rainfall, humidity, wind_speed, pressure.
"""

from __future__ import annotations

import csv
import inspect
import sys
import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.schema import STANDARD_SCHEMA, STANDARD_COLUMNS, OPTIONAL_COLUMNS
from config.settings import PRIMARY_DATASET_PATH
from src.data_loader import load_dataset, load_raw_csv, standardize
from src.spark_session import get_spark_session
from src.validator import validate as validate_new
import tools.validator_old as val_old
from scripts.make_variant_files import make_variants


def create_402_defect_fixture(tmp_dir: Path) -> Path:
    """Create the standard 402-row defect fixture."""
    with open(PRIMARY_DATASET_PATH, mode="r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        bengaluru_rows = []
        mumbai_rows = []
        for r in reader:
            if r[1] == "Bengaluru" and len(bengaluru_rows) < 200:
                bengaluru_rows.append(r)
            elif r[1] == "Mumbai" and len(mumbai_rows) < 200:
                mumbai_rows.append(r)
            if len(bengaluru_rows) == 200 and len(mumbai_rows) == 200:
                break

    rows = [list(r) for r in bengaluru_rows] + [list(r) for r in mumbai_rows]
    temp_idx = header.index("temperature")
    rain_idx = header.index("rainfall")
    hum_idx = header.index("humidity")

    # 1: 3 rows with -999 in rainfall
    rows[5][rain_idx] = "-999"
    rows[6][rain_idx] = "-999"
    rows[7][rain_idx] = "-999"

    # 2: 2 unparseable temperature cells ("abc")
    rows[10][temp_idx] = "abc"
    rows[11][temp_idx] = "abc"

    # 3: 1 out-of-range temperature (150)
    rows[15][temp_idx] = "150"

    # 4: 4 blank humidity cells
    rows[20][hum_idx] = ""
    rows[21][hum_idx] = ""
    rows[22][hum_idx] = ""
    rows[23][hum_idx] = ""

    # 5: 2 duplicated keys (400 + 2 = 402 rows)
    rows.append(list(rows[0]))
    rows.append(list(rows[1]))

    file_path = tmp_dir / "defects_402.csv"
    with open(file_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    return file_path


def compare_reports(name: str, rep_old, rep_new):
    print(f"\n--- Comparing Reports for: {name} ---")
    
    # 1. Errors
    errs_match = (rep_old.errors == rep_new.errors)
    print(f"Errors match: {errs_match}")
    if not errs_match:
        print(f"  Old errors: {rep_old.errors}")
        print(f"  New errors: {rep_new.errors}")

    # 2. Warnings
    # Sort for deterministic comparison
    old_warns = sorted(rep_old.warnings)
    new_warns = sorted(rep_new.warnings)
    warns_match = (old_warns == new_warns)
    print(f"Warnings match: {warns_match}")
    if not warns_match:
        print(f"  Old warnings ({len(old_warns)}): {old_warns}")
        print(f"  New warnings ({len(new_warns)}): {new_warns}")

    # 3. Info (compare semantic keys: total_rows, sentinel_counts, numeric_parse_failures, duplicate_keys_count, null_counts, out_of_range_counts)
    semantic_keys = [
        "total_rows", "min_date", "max_date", "sentinel_counts",
        "numeric_parse_failures", "duplicate_keys_count", "null_counts", "out_of_range_counts"
    ]
    info_matches = True
    for k in semantic_keys:
        v_old = rep_old.info.get(k)
        v_new = rep_new.info.get(k)
        if v_old != v_new:
            info_matches = False
            print(f"  Info mismatch for key '{k}': Old={v_old}, New={v_new}")
    print(f"Semantic Info keys match: {info_matches}")

    # 4. Column Stats
    old_stats = sorted(rep_old.column_stats, key=lambda x: x["column"])
    new_stats = sorted(rep_new.column_stats, key=lambda x: x["column"])
    stats_match = (old_stats == new_stats)
    print(f"Column stats match: {stats_match}")
    if not stats_match:
        print(f"  Old stats ({len(old_stats)}): {old_stats}")
        print(f"  New stats ({len(new_stats)}): {new_stats}")

    print(f"SQL queries executed: Old={len(rep_old.sql_log)} queries, New={len(rep_new.sql_log)} queries (Merged)")

    diffs = []
    if not errs_match:
        diffs.append(f"Errors difference: Old={rep_old.errors} vs New={rep_new.errors}")
    if not warns_match:
        diffs.append(f"Warnings difference: Old={old_warns} vs New={new_warns}")
    if not info_matches:
        diffs.append(f"Info difference detected")
    if not stats_match:
        diffs.append(f"Column stats difference detected")

    if not diffs:
        print("RESULT: 100% IDENTICAL across errors, warnings, info, and column_stats.")
    else:
        print(f"RESULT: DIFFERENCE FOUND ({len(diffs)} item(s)):")
        for d in diffs:
            print(f"  - {d}")
        if name == "Variant V3 (Kelvin Unit Mismatch)":
            print("  Note: Phase 9 C3 requirement promoted >50% unit mismatch on required columns (temperature) from a warning to a validation ERROR.")
            print("        This difference is an intentional, documented requirement of Phase 9/10.")

    return diffs


def main():
    print("=" * 80)
    print("PHASE 10 A5: VALIDATOR COMPARISON (OLD f6cffb8 VS NEW MERGED)")
    print("=" * 80)

    # Print the source of the merged validator query in src/validator.py
    import src.validator as v_mod
    src_lines, start_lineno = inspect.getsourcelines(v_mod.validate)
    merged_lines = [l for l in src_lines if "Merged Standardized View Profiling Query" in l or "merged_select_items" in l or "Merged standardized view profiling query" in l]
    print("\nSource snippet of merged validator checks in src/validator.py:")
    print("-" * 70)
    in_snippet = False
    for line in src_lines:
        if "Merged Standardized View Profiling Query" in line:
            in_snippet = True
        if in_snippet:
            print(line, end="")
            if "merged_row = _execute_logged_sql" in line:
                break
    print("-" * 70)

    spark = get_spark_session()
    tmp_dir = Path(tempfile.gettempdir()) / "agy_val_comparison"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    var_files = make_variants(tmp_dir)

    all_11_cols = [f.name for f in STANDARD_SCHEMA.fields]
    all_mapping = {c: c for c in all_11_cols}

    # 1. 402-row defect fixture
    defect_csv = create_402_defect_fixture(tmp_dir)
    raw_df_402 = load_raw_csv(spark, defect_csv, delimiter=",", view_name="raw_402")
    std_df_402 = standardize(spark, raw_view="raw_402", mapping=all_mapping, date_format="yyyy-MM-dd", view_name="std_402")
    rep_old_402 = val_old.validate(spark, raw_view="raw_402", std_view="std_402", mapping=all_mapping, original_row_count=402)
    rep_new_402 = validate_new(spark, raw_view="raw_402", std_view="std_402", mapping=all_mapping, original_row_count=402)
    compare_reports("402-row Defect Fixture", rep_old_402, rep_new_402)

    # 2. Variant V3 (Kelvin unit mismatch)
    v3_csv = var_files["V3"]
    load_v3 = load_dataset(spark, v3_csv)
    raw_v3 = load_v3.validation.info.get("raw_view", "weather_raw")
    std_v3 = load_v3.standardized_view
    rep_old_v3 = val_old.validate(spark, raw_view=raw_v3, std_view=std_v3, mapping=load_v3.mapping, original_row_count=500)
    rep_new_v3 = validate_new(spark, raw_view=raw_v3, std_view=std_v3, mapping=load_v3.mapping, original_row_count=500)
    compare_reports("Variant V3 (Kelvin Unit Mismatch)", rep_old_v3, rep_new_v3)

    # 3. Variant V6 (Missing rainfall for Mumbai)
    v6_csv = var_files["V6"]
    load_v6 = load_dataset(spark, v6_csv)
    raw_v6 = load_v6.validation.info.get("raw_view", "weather_raw")
    std_v6 = load_v6.standardized_view
    rep_old_v6 = val_old.validate(spark, raw_view=raw_v6, std_view=std_v6, mapping=load_v6.mapping, original_row_count=1462)
    rep_new_v6 = validate_new(spark, raw_view=raw_v6, std_view=std_v6, mapping=load_v6.mapping, original_row_count=1462)
    compare_reports("Variant V6 (Rainfall Empty for One City)", rep_old_v6, rep_new_v6)

    # 4. Real dataset (46,752 rows, all 7 numeric columns)
    raw_real = load_raw_csv(spark, PRIMARY_DATASET_PATH, delimiter=",", view_name="raw_real")
    std_real = standardize(spark, raw_view="raw_real", mapping=all_mapping, date_format="yyyy-MM-dd", view_name="std_real")
    rep_old_real = val_old.validate(spark, raw_view="raw_real", std_view="std_real", mapping=all_mapping, original_row_count=46752)
    rep_new_real = validate_new(spark, raw_view="raw_real", std_view="std_real", mapping=all_mapping, original_row_count=46752)
    compare_reports("Real Dataset (india_weather_daily.csv, 46,752 rows)", rep_old_real, rep_new_real)

    print("\nAll 7 numeric columns verified in column_stats:")
    for st in rep_new_real.column_stats:
        print(f"  {st['column']:12s} ({st['unit']:6s}): min={st['min']}, avg={st['avg']}, max={st['max']}, count={st['count']}")


if __name__ == "__main__":
    main()
