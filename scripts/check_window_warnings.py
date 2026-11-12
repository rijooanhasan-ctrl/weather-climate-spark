"""
scripts/check_window_warnings.py
Runs each of the 22 analyses individually to identify which ones emit:
"No Partition Defined for Window operation" in stderr.
"""

import io
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import PRIMARY_DATASET_PATH
from src.cleaner import clean
from src.data_loader import load_dataset
from src.queries.registry import ANALYSES
from src.spark_session import get_spark_session


def main():
    spark = get_spark_session("WindowWarningChecker")
    load_res = load_dataset(spark, PRIMARY_DATASET_PATH)
    clean(spark, std_view=load_res.standardized_view, view_name="weather_clean")

    warning_keys = []
    print("=" * 80)
    print("CHECKING 22 ANALYSES FOR GLOBAL WINDOW WARNINGS")
    print("=" * 80)

    for item in ANALYSES:
        key = item["key"]
        fn = item["function"]

        # Run query and collect result to trigger physical plan execution
        # Note: Spark stderr is written at JVM level to process stderr,
        # so we run each query and note any window queries.
        res = fn(spark, view="weather_clean")
        count = res.df.count()
        sql_text = res.sql.upper()
        has_unpartitioned_over = False
        if "OVER ()" in sql_text or "OVER (ORDER BY" in sql_text or "OVER (ROWS" in sql_text:
            has_unpartitioned_over = True
            warning_keys.append((key, count))
            print(f"  [GLOBAL WINDOW DETECTED] {key:25s} -> Result rows: {count}")
        else:
            print(f"  [OK]                    {key:25s} -> Result rows: {count}")

    print()
    print("=" * 80)
    print(f"SUMMARY: {len(warning_keys)} analysis queries use global window without PARTITION BY:")
    for k, c in warning_keys:
        print(f"  - Key: {k} (result rows: {c})")
    print("=" * 80)


if __name__ == "__main__":
    main()

