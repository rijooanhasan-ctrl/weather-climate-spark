"""
scripts/check_second_dataset.py
===============================
Check and evaluate data/external/second_dataset.csv (Phase 9 C4).
If present, ingests, validates, cleans, and runs all 22 analyses.
If absent, prints 'second dataset NOT VERIFIED'.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.spark_session import get_spark_session
from src.pipeline import prepare
from src.queries.registry import ANALYSES
from src.queries.base import Filters


def main() -> int:
    print("=" * 80)
    print("PHASE 9 C4: SECOND DATASET EVALUATION")
    print("=" * 80)

    dataset_path = PROJECT_ROOT / "data" / "external" / "second_dataset.csv"
    print(f"Checking for external dataset: data/external/second_dataset.csv...")

    if not dataset_path.exists():
        print(f"File status: NOT FOUND at data/external/second_dataset.csv")
        print("\nsecond dataset NOT VERIFIED")
        print("=" * 80)
        return 0

    print(f"File status: FOUND ({dataset_path.stat().st_size} bytes)")
    print("--> Ingesting, validating, and cleaning second dataset via prepare()...")
    spark = get_spark_session()
    prep_res = prepare(spark, dataset_path, view_name="second_clean")

    clean_out = prep_res.cleaning_report.summary.get("clean_rows_out", 0)
    print(f"    Second dataset clean rows: {clean_out:,}")

    print("--> Running all 22 analysis queries on second dataset...")
    df_clean = spark.table("second_clean")
    filters = Filters(locations=[])
    success_count = 0
    for key, entry in ANALYSES.items():
        try:
            df = entry.func(spark, df=df_clean, filters=filters)
            print(f"    [{entry.index:02d}/22] {key:<28} -> {df.shape[0]} rows")
            success_count += 1
        except Exception as exc:
            print(f"    [{entry.index:02d}/22] {key:<28} -> ERROR: {exc}")

    print(f"\nCompleted {success_count}/22 analyses on second dataset.")
    print("=" * 80)
    return 0


if __name__ == "__main__":
    sys.exit(main())

