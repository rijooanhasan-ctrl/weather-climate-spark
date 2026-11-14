"""
tools/run_location_trim_audit.py
================================
Phase 10 A8: Location whitespace audit.
Prints the SQL of standardize() showing TRIM(CAST(raw_location AS STRING)) AS location.
Loads variant V8 and verifies it produces exactly 3 distinct locations:
'MUMBAI', 'Mumbai', 'mumbai'.
"""

from __future__ import annotations

import inspect
import sys
import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.make_variant_files import make_variants
from src.data_loader import load_dataset, standardize
from src.spark_session import get_spark_session


def main() -> None:
    spark = get_spark_session("LocationTrimAudit")

    print("================================================================================")
    print("PHASE 10 A8: LOCATION WHITESPACE AUDIT")
    print("================================================================================")

    # 1. Print SQL generation snippet from standardize()
    print("\n1. STANDARDIZE() LOCATION CLAUSE IMPLEMENTATION:")
    lines, start_line = inspect.getsourcelines(standardize)
    for idx in range(316, 328):
        # Line numbers in standardize source
        rel_idx = idx - 280
        if 0 <= rel_idx < len(lines):
            print(f"   {lines[rel_idx].rstrip()}")

    # 2. Verify V8 distinct locations
    print("\n2. VARIANT V8 DISTINCT LOCATIONS:")
    with tempfile.TemporaryDirectory() as tmp_dir:
        variants = make_variants(Path(tmp_dir))
        v8_path = variants["V8"]

        load_res = load_dataset(spark, v8_path)
        dist_locs = [
            r["location"]
            for r in spark.sql(f"SELECT DISTINCT location FROM {load_res.standardized_view} ORDER BY location").collect()
        ]

        print(f"   Input file: {v8_path.name}")
        print(f"   Distinct locations found ({len(dist_locs)}): {dist_locs}")

        expected = ["MUMBAI", "Mumbai", "mumbai"]
        assert dist_locs == expected, f"Expected {expected}, got {dist_locs}"
        print(f"   [VERIFIED] Distinct locations exactly match expected: {dist_locs}")

    print("\n================================================================================")
    print("A8 LOCATION TRIM AUDIT: COMPLETE & VERIFIED")
    print("================================================================================")


if __name__ == "__main__":
    main()
