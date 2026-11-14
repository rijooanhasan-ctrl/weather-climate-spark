"""
scripts/verify_cleaner_counts.py
================================
Cleaner counts fixture verification (Phase 9 A4):
1. Create defect fixture with real rows:
   - 3 sentinels (-999, -9999, 9999)
   - 1 out-of-range (temperature = 150.0)
   - 2 duplicate keys (surplus rows = 2)
   - 2-row gap in temperature (gap <= 3 days, filled)
   - 5-row gap in temperature (gap > 3 days, NOT filled)
2. Ingest with load_dataset() and execute clean().
3. Compare report counts with direct Spark SQL COUNT(*) queries on step views
   clean_step_1 through clean_step_5.
4. Verify 100% agreement between report and direct SQL counts.
"""

from __future__ import annotations

import csv
import sys
import tempfile
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from src.spark_session import get_spark_session
from src.data_loader import load_dataset
from src.cleaner import clean
from config.settings import PRIMARY_DATASET_PATH


def run_cleaner_counts_verification() -> bool:
    print("=" * 80)
    print("PHASE 9 A4: CLEANER COUNTS FIXTURE & STEP VIEW VERIFICATION")
    print("=" * 80)

    # Read 50 real rows from primary dataset
    with open(PRIMARY_DATASET_PATH, mode="r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        real_rows = [next(reader) for _ in range(50)]

    temp_idx = header.index("temperature")
    rain_idx = header.index("rainfall")
    humid_idx = header.index("humidity")
    press_idx = header.index("pressure")

    # Clone rows
    rows = [list(r) for r in real_rows]

    # 1. Inject 3 sentinels in rainfall (-999, -9999, 9999)
    rows[3][rain_idx] = "-999"
    rows[7][rain_idx] = "-9999"
    rows[11][rain_idx] = "9999"

    # 2. Inject 1 out-of-range in rainfall (2500.0 > 2000.0 mm)
    rows[15][rain_idx] = "2500.0"

    # 3. Inject 2-row gap in temperature (rows 20, 21: dates 2010-01-21, 2010-01-22)
    # Bounded by valid rows 19 and 22 -> gap = 2 days <= 3 days -> will be filled
    rows[20][temp_idx] = ""
    rows[21][temp_idx] = ""

    # 4. Inject 5-row gap in temperature (rows 30, 31, 32, 33, 34: dates 2010-01-31 to 2010-02-04)
    # Bounded by valid rows 29 and 35 -> gap = 5 days > 3 days -> will NOT be filled
    for r_i in range(30, 35):
        rows[r_i][temp_idx] = ""

    # 5. Inject 2 duplicate keys (duplicate row 40 and duplicate row 45)
    dup1 = list(rows[40])
    dup2 = list(rows[45])
    rows.append(dup1)
    rows.append(dup2)

    total_fixture_rows = len(rows)  # 50 + 2 = 52 rows
    print(f"\n--> Created defect fixture with {total_fixture_rows} rows from real observations:")
    print("    - Sentinels injected: 3 (rainfall: -999, -9999, 9999)")
    print("    - Out-of-range injected: 1 (rainfall: 1500.0 > 1000.0 mm)")
    print("    - Duplicate keys injected: 2 (dates duplicated: row 40, row 45)")
    print("    - 2-row gap injected: temperature in rows 20, 21 (gap <= 3 days, will be filled)")
    print("    - 5-row gap injected: temperature in rows 30..34 (gap > 3 days, will NOT be filled)")

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_csv = Path(tmp_dir) / "cleaner_defect_fixture.csv"
        with open(tmp_csv, mode="w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(rows)

        spark = get_spark_session()
        print("\n--> Loading dataset via data_loader.load_dataset()...")
        load_res = load_dataset(spark, tmp_csv)

        print("--> Executing clean()...")
        df_clean, report = clean(spark, std_view=load_res.standardized_view, view_name="fixture_clean", options={"keep_step_views": True})

        # Step-by-step verification against direct Spark SQL on clean_step_1 .. clean_step_5
        print("\n--> Comparing CleaningReport counts against direct Spark SQL on step views:")
        print("-" * 80)
        print(f"{'Step & Verification Item':<40} | {'Report Count':<16} | {'Direct SQL Count':<16} | Status")
        print("-" * 80)

        # Step 1: Unusable rows
        s1_report = report.steps[0]["rows_removed"]
        s1_sql = int(spark.sql(f"SELECT COUNT(*) AS n FROM {load_res.standardized_view} WHERE date IS NULL OR location IS NULL OR trim(location) = ''").collect()[0]["n"])
        assert s1_report == s1_sql, f"Step 1 mismatch: report={s1_report}, sql={s1_sql}"
        print(f"{'Step 1: Dropped unusable rows':<40} | {s1_report:<16} | {s1_sql:<16} | MATCH")

        # Step 2: Sentinels converted to NULL
        s2_report_dict = report.steps[1]["values_changed"]
        s2_report_total = sum(s2_report_dict.values())
        s2_sql_rain = int(spark.sql("SELECT COUNT(*) AS n FROM clean_step_1 WHERE rainfall IN (-999, -9999, 9999)").collect()[0]["n"])
        assert s2_report_total == 3 and s2_sql_rain == 3, f"Step 2 sentinel count mismatch: {s2_report_total} vs {s2_sql_rain}"
        # Also verify in clean_step_2 all sentinels are 0
        s2_remaining = int(spark.sql("SELECT COUNT(*) AS n FROM clean_step_2 WHERE rainfall IN (-999, -9999, 9999)").collect()[0]["n"])
        assert s2_remaining == 0, f"Step 2 sentinels still present in clean_step_2: {s2_remaining}"
        print(f"{'Step 2: Sentinels converted to NULL':<40} | {s2_report_total:<16} | {s2_sql_rain:<16} | MATCH (0 in step 2)")

        # Step 3: Out-of-range converted to NULL
        s3_report_dict = report.steps[2]["values_changed"]
        s3_report_rain = s3_report_dict.get("rainfall", 0)
        s3_sql_rain = int(spark.sql("SELECT COUNT(*) AS n FROM clean_step_2 WHERE rainfall < 0.0 OR rainfall > 2000.0").collect()[0]["n"])
        assert s3_report_rain == 1 and s3_sql_rain == 1, f"Step 3 out-of-range mismatch: report={s3_report_rain}, sql={s3_sql_rain}"
        s3_remaining = int(spark.sql("SELECT COUNT(*) AS n FROM clean_step_3 WHERE rainfall < 0.0 OR rainfall > 2000.0").collect()[0]["n"])
        assert s3_remaining == 0, f"Step 3 outliers still present in clean_step_3: {s3_remaining}"
        print(f"{'Step 3: Out-of-range values to NULL':<40} | {s3_report_rain:<16} | {s3_sql_rain:<16} | MATCH (0 in step 3)")

        # Step 4: Duplicate keys deduplicated
        s4_report_dups = report.steps[3]["values_changed"]["duplicate_keys"]
        s4_report_surplus = report.steps[3]["rows_removed"]
        s4_sql_dups = int(spark.sql("SELECT COUNT(*) AS n FROM (SELECT date, location FROM clean_step_3 GROUP BY date, location HAVING COUNT(*) > 1)").collect()[0]["n"])
        s4_sql_surplus = int(spark.sql("SELECT COALESCE(SUM(c - 1), 0) AS n FROM (SELECT COUNT(*) AS c FROM clean_step_3 GROUP BY date, location HAVING COUNT(*) > 1)").collect()[0]["n"])
        assert s4_report_dups == s4_sql_dups == 2, f"Step 4 dups mismatch: report={s4_report_dups}, sql={s4_sql_dups}"
        assert s4_report_surplus == s4_sql_surplus == 2, f"Step 4 surplus mismatch: report={s4_report_surplus}, sql={s4_sql_surplus}"
        s4_remaining_dups = int(spark.sql("SELECT COUNT(*) AS n FROM (SELECT date, location FROM clean_step_4 GROUP BY date, location HAVING COUNT(*) > 1)").collect()[0]["n"])
        assert s4_remaining_dups == 0, f"Step 4 dups still present in clean_step_4: {s4_remaining_dups}"
        print(f"{'Step 4: Duplicate keys removed':<40} | {s4_report_dups} (surplus {s4_report_surplus}) | {s4_sql_dups} (surplus {s4_sql_surplus}) | MATCH (0 in step 4)")

        # Step 5: Short gap linear interpolation (<= 3 days)
        s5_report_filled = report.steps[4]["values_changed"].get("temperature", 0)
        s5_sql_filled = int(spark.sql("SELECT COUNT(*) AS n FROM clean_step_5 WHERE temperature_was_filled = true").collect()[0]["n"])
        assert s5_report_filled == 2 and s5_sql_filled == 2, f"Step 5 filled mismatch: report={s5_report_filled}, sql={s5_sql_filled}"
        
        # Verify that the 5-row gap was NOT filled
        s5_sql_unfilled_gap = int(spark.sql("SELECT COUNT(*) AS n FROM clean_step_5 WHERE temperature IS NULL AND date BETWEEN '2010-01-31' AND '2010-02-04'").collect()[0]["n"])
        assert s5_sql_unfilled_gap == 5, f"Expected 5 unfilled nulls in 5-day gap, found {s5_sql_unfilled_gap}"
        print(f"{'Step 5: Gaps <= 3d filled':<40} | {s5_report_filled:<16} | {s5_sql_filled:<16} | MATCH")
        print(f"{'Step 5: Gaps > 3d untouched (null)':<40} | {'5 rows':<16} | {s5_sql_unfilled_gap} rows        | MATCH")

        print("-" * 80)
        print("--> Summary Row Count Arithmetic:")
        print(f"    Raw Rows In:            {report.summary['raw_rows_in']} (fixture total: {total_fixture_rows})")
        print(f"    Duplicate Rows Dropped: {report.summary['rows_removed']['duplicate_surplus_dropped']}")
        print(f"    Clean Rows Out:         {report.summary['clean_rows_out']} (expected: {total_fixture_rows - 2})")
        assert report.summary["clean_rows_out"] == total_fixture_rows - 2

        print("\n" + "=" * 80)
        print("CLEANER COUNTS FIXTURE & STEP VIEW AUDIT: PASSED (100% VERIFIED)")
        print("=" * 80)
        return True


if __name__ == "__main__":
    success = run_cleaner_counts_verification()
    sys.exit(0 if success else 1)
