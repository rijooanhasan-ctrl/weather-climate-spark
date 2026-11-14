"""
scripts/verify_gap_filling.py
=============================
Phase 8 Audit Fix D3: Step 5 gap filling on all six columns with 2-row gaps
on a real 200-row fixture from data/raw/india_weather_daily.csv.

Verifies:
1. Recorded Spark SQL for Step 5 windowed linear interpolation.
2. Arithmetic verification for all 6 columns:
   (temperature, temp_max, temp_min, humidity, wind_speed, pressure).
3. Quality flags: <col>_was_filled is True only for reconstructed rows.
4. Rainfall stays unfilled (remains NULL, no rainfall_was_filled column).
"""

from __future__ import annotations

import csv
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import FILL_COLUMNS, PRIMARY_DATASET_PATH
from src.cleaner import clean
from src.data_loader import load_dataset
from src.spark_session import get_spark_session


def main() -> int:
    print("=" * 80)
    print(" PHASE 8 AUDIT FIX D3: GAP FILLING ON ALL SIX COLUMNS (200-ROW FIXTURE)")
    print("=" * 80)

    # 1. Load first 200 Bengaluru rows from real dataset
    print("\n--> Extracting 200 real Bengaluru rows from primary dataset...")
    header: list[str] = []
    rows: list[list[str]] = []
    with open(PRIMARY_DATASET_PATH, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        loc_idx = header.index("location")
        for r in reader:
            if r and r[loc_idx] == "Bengaluru":
                rows.append(r)
                if len(rows) == 200:
                    break

    assert len(rows) == 200, f"Expected 200 rows, got {len(rows)}"
    print(f"    Extracted {len(rows)} real records for station 'Bengaluru'.")

    # 2. Inject 2-row gaps on each of the 6 FILL_COLUMNS and rainfall
    gap_plan = {
        "temperature": (10, 11),
        "temp_max": (20, 21),
        "temp_min": (30, 31),
        "humidity": (40, 41),
        "wind_speed": (50, 51),
        "pressure": (60, 61),
        "rainfall": (70, 71),
    }

    baseline_anchors: dict[str, tuple[float, float]] = {}
    for col, (g1, g2) in gap_plan.items():
        c_idx = header.index(col)
        v0 = float(rows[g1 - 1][c_idx])
        v3 = float(rows[g2 + 1][c_idx])
        baseline_anchors[col] = (v0, v3)
        rows[g1][c_idx] = ""
        rows[g2][c_idx] = ""

    # Write fixture to scratch/temp CSV
    fixture_dir = PROJECT_ROOT / "data" / "scratch"
    fixture_dir.mkdir(parents=True, exist_ok=True)
    fixture_csv = fixture_dir / "gap_fill_200_fixture.csv"
    with open(fixture_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)

    # 3. Ingest and Clean
    print(f"    Created fixture at data/scratch/{fixture_csv.name}")
    spark = get_spark_session("GapFillVerification")
    load_res = load_dataset(spark, fixture_csv)
    df_clean, report = clean(spark, std_view=load_res.standardized_view, view_name="weather_clean_gap_test")

    # 4. Print Recorded SQL for Step 5
    step5_entry = [s for s in report.steps if s["step"] == 5][0]
    print("\n" + "=" * 80)
    print(" RECORDED SPARK SQL FOR STEP 5 (WINDOWED LINEAR INTERPOLATION)")
    print("=" * 80)
    print(step5_entry["sql"])
    print("=" * 80)

    # 5. Arithmetic Verification on all 6 fillable columns
    print("\n--> Arithmetic Verification for All 6 Interpolated Columns:")
    clean_records = spark.sql("SELECT * FROM weather_clean_gap_test ORDER BY date").collect()

    all_passed = True
    for col in FILL_COLUMNS:
        g1, g2 = gap_plan[col]
        v0, v3 = baseline_anchors[col]
        exp_1 = round(v0 + (1.0 / 3.0) * (v3 - v0), 4)
        exp_2 = round(v0 + (2.0 / 3.0) * (v3 - v0), 4)

        act_1 = float(clean_records[g1][col])
        act_2 = float(clean_records[g2][col])

        flag_1 = bool(clean_records[g1][f"{col}_was_filled"])
        flag_2 = bool(clean_records[g2][f"{col}_was_filled"])
        flag_before = bool(clean_records[g1 - 1][f"{col}_was_filled"])
        flag_after = bool(clean_records[g2 + 1][f"{col}_was_filled"])

        diff1 = abs(act_1 - exp_1)
        diff2 = abs(act_2 - exp_2)

        is_ok = (diff1 < 1e-3 and diff2 < 1e-3 and flag_1 and flag_2 and not flag_before and not flag_after)
        status = "PASSED" if is_ok else "FAILED"
        if not is_ok:
            all_passed = False

        print(f"\n  [{status}] Column '{col}':")
        print(f"      Anchors: row {g1-1}={v0:.4f}, row {g2+1}={v3:.4f}")
        print(f"      Row {g1}: Expected={exp_1:.4f}, Actual={act_1:.4f}, was_filled={flag_1}")
        print(f"      Row {g2}: Expected={exp_2:.4f}, Actual={act_2:.4f}, was_filled={flag_2}")
        print(f"      Adjacent flags: row {g1-1}={flag_before}, row {g2+1}={flag_after}")

    # 6. Rainfall Unfilled Verification
    print("\n--> Rainfall Unfilled Verification:")
    r_g1, r_g2 = gap_plan["rainfall"]
    r_act_1 = clean_records[r_g1]["rainfall"]
    r_act_2 = clean_records[r_g2]["rainfall"]
    has_rain_flag = "rainfall_was_filled" in df_clean.columns

    rain_ok = (r_act_1 is None and r_act_2 is None and not has_rain_flag)
    if not rain_ok:
        all_passed = False
    r_status = "PASSED" if rain_ok else "FAILED"

    print(f"  [{r_status}] Rainfall at row {r_g1}: {r_act_1} (Expected None)")
    print(f"  [{r_status}] Rainfall at row {r_g2}: {r_act_2} (Expected None)")
    print(f"  [{r_status}] 'rainfall_was_filled' column absent from clean view: {not has_rain_flag}")

    # Clean up fixture
    try:
        fixture_csv.unlink()
    except Exception:
        pass

    print("\n" + "=" * 80)
    if all_passed:
        print(" GAP FILLING VERIFICATION SUCCESSFUL: ALL CHECKS PASSED")
    else:
        print(" GAP FILLING VERIFICATION FAILED")
    print("=" * 80)
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())

