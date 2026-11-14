"""
tools/run_cleaner_audit.py
==========================
Phase 10 A7: Cleaner audit script.
Loads the real dataset through clean() and prints the titles of report.steps.
Then, in the A4 defect fixture (3 sentinels, 1 out-of-range, 2 duplicates, 2-row gap, 5-row gap),
prints the raw rows of the 5-row gap (values NULL, flags false)
and the 2-row gap (interpolated values with arithmetic shown).
"""

from __future__ import annotations

import csv
import sys
import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import PRIMARY_DATASET_PATH
from src.cleaner import clean
from src.data_loader import load_dataset
from src.spark_session import get_spark_session


def main() -> None:
    spark = get_spark_session("CleanerAudit")

    print("================================================================================")
    print("PHASE 10 A7: CLEANER AUDIT")
    print("================================================================================")

    # 1. Real dataset step titles
    print("\n1. REAL DATASET CLEANING REPORT STEPS:")
    print("Loading and cleaning real dataset...")
    real_load = load_dataset(spark, PRIMARY_DATASET_PATH)
    _, real_report = clean(spark, std_view=real_load.standardized_view, view_name="audit_real_clean")

    for i, step in enumerate(real_report.steps, 1):
        step_num = step.get("step", i)
        desc = step.get("description", step.get("title", f"Step {i}"))
        rows_rem = step.get("rows_removed", 0)
        vals_chg = sum(step.get("values_changed", {}).values()) if isinstance(step.get("values_changed"), dict) else 0
        print(f"  Step {step_num}: {desc} | rows_removed={rows_rem}, values_changed={vals_chg}")

    # 2. Defect Fixture
    print("\n2. A4 DEFECT FIXTURE AUDIT:")
    with open(PRIMARY_DATASET_PATH, mode="r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        # Slices from Bengaluru
        real_rows = []
        for r in reader:
            if r[1] == "Bengaluru":
                real_rows.append(r)
            if len(real_rows) >= 50:
                break

    temp_idx = header.index("temperature")
    rain_idx = header.index("rainfall")

    rows = [list(r) for r in real_rows]
    # 3 sentinels in rainfall (-999, -9999, 9999)
    rows[3][rain_idx] = "-999"
    rows[7][rain_idx] = "-9999"
    rows[11][rain_idx] = "9999"
    # 1 out-of-range in rainfall (2500.0 > 2000.0 mm)
    rows[15][rain_idx] = "2500.0"

    # Known boundary values for 2-row gap
    t0 = float(rows[19][temp_idx])
    t3 = float(rows[22][temp_idx])
    # 2-row gap (indices 20 and 21)
    rows[20][temp_idx] = ""
    rows[21][temp_idx] = ""

    # 5-row gap (indices 30 to 34)
    for r_i in range(30, 35):
        rows[r_i][temp_idx] = ""

    # 2 duplicate keys
    rows.append(list(rows[40]))
    rows.append(list(rows[45]))

    with tempfile.TemporaryDirectory() as tmp_dir:
        fixture_path = Path(tmp_dir) / "defect_fixture.csv"
        with open(fixture_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(rows)

        fix_load = load_dataset(spark, fixture_path)
        df_clean, fix_report = clean(spark, std_view=fix_load.standardized_view, view_name="audit_fix_clean")

        # 5-row gap audit
        print("\n--- 5-Row Gap Audit (dates 2010-01-31 to 2010-02-04) ---")
        five_gap_rows = spark.sql(
            "SELECT date, location, temperature, temperature_was_filled "
            "FROM audit_fix_clean "
            "WHERE date BETWEEN '2010-01-31' AND '2010-02-04' "
            "ORDER BY date"
        ).collect()

        assert len(five_gap_rows) == 5, f"Expected 5 rows, got {len(five_gap_rows)}"
        for r in five_gap_rows:
            print(f"  Date: {r['date']} | Temp: {r['temperature']} | Was Filled: {r['temperature_was_filled']}")
            assert r["temperature"] is None, f"Expected NULL for {r['date']}, got {r['temperature']}"
            assert r["temperature_was_filled"] is False, f"Expected was_filled False for {r['date']}"
        print("  -> CONFIRMED: All 5 rows have NULL temperature and temperature_was_filled = False.")

        # 2-row gap audit
        print("\n--- 2-Row Gap Audit (dates 2010-01-21 and 2010-01-22) ---")
        two_gap_rows = spark.sql(
            "SELECT date, location, temperature, temperature_was_filled "
            "FROM audit_fix_clean "
            "WHERE date BETWEEN '2010-01-20' AND '2010-01-23' "
            "ORDER BY date"
        ).collect()

        assert len(two_gap_rows) == 4, f"Expected 4 rows (2 endpoints + 2 gap), got {len(two_gap_rows)}"
        print(f"  Pre-gap endpoint  ({two_gap_rows[0]['date']}): Temp = {two_gap_rows[0]['temperature']} (t0 = {t0})")
        print(f"  Post-gap endpoint ({two_gap_rows[3]['date']}): Temp = {two_gap_rows[3]['temperature']} (t3 = {t3})")

        exp_1 = round(t0 + (1.0 / 3.0) * (t3 - t0), 4)
        exp_2 = round(t0 + (2.0 / 3.0) * (t3 - t0), 4)

        act_1 = two_gap_rows[1]["temperature"]
        act_2 = two_gap_rows[2]["temperature"]
        fill_1 = two_gap_rows[1]["temperature_was_filled"]
        fill_2 = two_gap_rows[2]["temperature_was_filled"]

        print(f"  Gap Day 1 ({two_gap_rows[1]['date']}):")
        print(f"    Arithmetic: {t0:.4f} + 1/3 * ({t3:.4f} - {t0:.4f}) = {exp_1:.4f}")
        print(f"    Actual:     {act_1:.4f} | Was Filled: {fill_1}")
        assert abs(act_1 - exp_1) < 1e-3
        assert fill_1 is True

        print(f"  Gap Day 2 ({two_gap_rows[2]['date']}):")
        print(f"    Arithmetic: {t0:.4f} + 2/3 * ({t3:.4f} - {t0:.4f}) = {exp_2:.4f}")
        print(f"    Actual:     {act_2:.4f} | Was Filled: {fill_2}")
        assert abs(act_2 - exp_2) < 1e-3
        assert fill_2 is True

        print("  -> CONFIRMED: 2-day gap correctly linearly interpolated with arithmetic verified.")

    print("\n================================================================================")
    print("A7 CLEANER AUDIT: COMPLETE & VERIFIED")
    print("================================================================================")


if __name__ == "__main__":
    main()
