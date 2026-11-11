"""
scripts/clean_dataset.py
========================
Command-line driver for the Spark SQL weather data cleaning pipeline.

Loads, standardizes, validates, and cleans weather observations using deterministic
Spark SQL transformations. Optionally exports the clean dataset and audit report.

Usage
-----
python scripts/clean_dataset.py
python scripts/clean_dataset.py --save
python scripts/clean_dataset.py --file path/to/dataset.csv --no-fill
python scripts/clean_dataset.py --file dirty.csv --drop-out-of-range --save

Exit Codes
----------
0 : Cleaning completed successfully
1 : Validation errors detected; cleaning refused
2 : Fatal file loading or configuration error
"""

import argparse
from pathlib import Path
import shutil
import sys
import uuid

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.schema import STANDARD_SCHEMA
from config.settings import (
    CLEAN_VIEW_NAME,
    CLEANING_REPORT_PATH,
    PRIMARY_DATASET_PATH,
    PROCESSED_CLEAN_PATH,
    PROCESSED_DATA_DIR,
)
from src.cleaner import clean
from src.data_loader import DatasetLoadError, load_dataset
from src.spark_session import get_spark_session
from src.utils import display_path, to_spark_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clean standardized weather observations using Spark SQL."
    )
    parser.add_argument(
        "--file",
        type=str,
        default=str(PRIMARY_DATASET_PATH),
        help="Path to the weather CSV dataset to ingest and clean (default: primary dataset).",
    )
    parser.add_argument(
        "--map",
        action="append",
        dest="mapping_args",
        metavar="TARGET=SOURCE",
        help="Custom column mapping overrides, e.g. --map temperature=Temp or --map Temp=temperature.",
    )
    parser.add_argument(
        "--date-format",
        type=str,
        default=None,
        help="Explicit date format pattern (e.g. yyyy-MM-dd, dd/MM/yyyy).",
    )
    parser.add_argument(
        "--location-name",
        type=str,
        default=None,
        help="Fallback location name if the dataset has no location column.",
    )
    parser.add_argument(
        "--no-fill",
        action="store_true",
        help="Disable linear interpolation for missing numeric observation stretches.",
    )
    parser.add_argument(
        "--drop-out-of-range",
        action="store_true",
        help="Drop rows with out-of-range temperature rather than setting them to NULL.",
    )
    parser.add_argument(
        "--save",
        action="store_true",
        help="Export cleaned dataset to data/processed/weather_clean.csv and report to cleaning_report.json.",
    )
    return parser.parse_args()


def parse_mapping_args(mapping_args: list[str] | None) -> dict[str, str]:
    """Parse repeatable --map arguments into a {standard_col: orig_col} dict."""
    if not mapping_args:
        return {}
    standard_names = {f.name for f in STANDARD_SCHEMA.fields}
    result: dict[str, str] = {}
    for item in mapping_args:
        if "=" not in item:
            raise ValueError(f"Invalid mapping syntax '{item}'. Expected KEY=VALUE.")
        left, right = item.split("=", 1)
        left = left.strip()
        right = right.strip()
        if left in standard_names:
            result[left] = right
        elif right in standard_names:
            result[right] = left
        else:
            result[left] = right
    return result


def save_clean_dataset(spark, df_clean, report) -> None:
    """
    Save the cleaned DataFrame as a single CSV file to PROCESSED_CLEAN_PATH
    and write the cleaning audit report JSON.
    """
    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    temp_dir = PROCESSED_DATA_DIR / f"_temp_write_{uuid.uuid4().hex[:8]}"

    save_method = ""
    try:
        spark_temp_path = to_spark_path(temp_dir)
        df_clean.coalesce(1).write.option("header", True).mode("overwrite").csv(spark_temp_path)

        # Locate single part file and move it
        part_files = list(temp_dir.glob("part-*.csv"))
        if not part_files:
            raise RuntimeError(f"No part-*.csv file produced in {display_path(temp_dir)}")

        shutil.move(str(part_files[0]), str(PROCESSED_CLEAN_PATH))
        save_method = "Native Spark CSV write (coalesce 1)"
        print(f"    Export method : {save_method}")
    except Exception as exc:
        reason = f"Spark native write failed ({type(exc).__name__}: {exc}); falling back to Pandas export."
        print(f"    [NOTICE] {reason}")
        pdf = df_clean.toPandas()
        pdf.to_csv(PROCESSED_CLEAN_PATH, index=False, encoding="utf-8")
        save_method = f"Pandas to_csv fallback (Reason: {exc})"
        print(f"    Export method : Pandas DataFrame to_csv fallback")
    finally:
        # Clean up temporary directory and any remaining .crc or _SUCCESS files
        if temp_dir.exists():
            shutil.rmtree(str(temp_dir), ignore_errors=True)

    report.summary["save_info"] = {
        "dataset_path": display_path(PROCESSED_CLEAN_PATH),
        "report_path": display_path(CLEANING_REPORT_PATH),
        "save_method": save_method,
    }
    report.to_json(CLEANING_REPORT_PATH)
    print(f"    Clean dataset : {display_path(PROCESSED_CLEAN_PATH)}")
    print(f"    Audit report  : {display_path(CLEANING_REPORT_PATH)}")


def main() -> int:
    args = parse_args()
    file_path = Path(args.file)

    print("=" * 80)
    print(" WEATHER & CLIMATE DATA CLEANING PIPELINE (SPARK SQL) ")
    print("=" * 80)
    print(f"Target dataset: {display_path(file_path)}")

    if not file_path.exists():
        print(f"\n[ERROR] File not found: {display_path(file_path)}", file=sys.stderr)
        return 2

    try:
        custom_mapping = parse_mapping_args(args.mapping_args)
    except ValueError as exc:
        print(f"\n[ERROR] {exc}", file=sys.stderr)
        return 2

    # Start SparkSession
    try:
        spark = get_spark_session(app_name="WeatherCleaningPipeline")
    except Exception as exc:
        print(f"\n[ERROR] Failed to start Spark session: {exc}", file=sys.stderr)
        return 2

    # 1. Ingestion & Validation
    try:
        load_res = load_dataset(
            spark=spark,
            path=file_path,
            mapping=custom_mapping if custom_mapping else None,
            date_format=args.date_format,
            default_location=args.location_name,
        )
    except DatasetLoadError as exc:
        print(f"\n[ERROR] Dataset loading failed: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"\n[ERROR] Unexpected error during ingestion: {exc}", file=sys.stderr)
        return 2

    if not load_res.validation.is_valid:
        print("\n[VALIDATION FAILED] Refusing to clean invalid dataset:")
        for err in load_res.validation.errors:
            print(f"  - [FAIL] {err}")
        return 1

    # 2. Execute Cleaning Pipeline
    clean_options = {
        "fill_gaps": not args.no_fill,
        "drop_out_of_range_rows": args.drop_out_of_range,
    }

    try:
        df_clean, report = clean(
            spark=spark,
            std_view=load_res.standardized_view,
            view_name=CLEAN_VIEW_NAME,
            options=clean_options,
        )
    except Exception as exc:
        print(f"\n[ERROR] Cleaning pipeline encountered an error: {exc}", file=sys.stderr)
        return 2

    # ── Display Pipeline Step Audit ───────────────────────────────────────────
    print(f"\n[1] PIPELINE STEP AUDIT TABLE")
    print(f"    {'Step':<5} {'Description':<54} {'Before':<8} {'After':<8} {'Dropped'}")
    print(f"    {'-'*5} {'-'*54} {'-'*8} {'-'*8} {'-'*8}")
    for s in report.steps:
        desc = s["description"]
        if len(desc) > 52:
            desc = desc[:49] + "..."
        b = s["rows_before"]
        a = s["rows_after"]
        d = s["rows_removed"]
        print(f"    {s['step']:<5} {desc:<54} {b:<8} {a:<8} {d:<8}")

    # ── Display Cleaning Summary ──────────────────────────────────────────────
    print(f"\n[2] CLEANING SUMMARY METRICS")
    sum_data = report.summary
    print(f"    Raw rows in         : {sum_data['raw_rows_in']}")
    print(f"    Clean rows out      : {sum_data['clean_rows_out']}")
    print(f"    Total rows removed  : {sum_data['rows_removed']['total_rows_removed']}")
    print(f"      - Unusable rows   : {sum_data['rows_removed']['unusable_rows']}")
    print(f"      - Duplicate rows  : {sum_data['rows_removed']['duplicate_surplus_dropped']}")
    print(f"      - Out-of-range    : {sum_data['rows_removed']['out_of_range_dropped']}")

    print(f"\n    Values transformed to NULL:")
    s_changes = sum_data["values_changed"]["sentinels_to_null"]
    if s_changes:
        for col, cnt in s_changes.items():
            print(f"      - Sentinels in {col}: {cnt}")
    else:
        print("      - Sentinels: None")

    r_changes = sum_data["values_changed"]["out_of_range_to_null"]
    if r_changes:
        for col, cnt in r_changes.items():
            print(f"      - Out-of-range in {col}: {cnt}")
    else:
        print("      - Out-of-range: None")

    print(f"\n    Values filled via linear interpolation (<= 3-day gaps):")
    f_changes = sum_data["values_changed"]["interpolated"]
    if f_changes:
        for col, cnt in f_changes.items():
            print(f"      - {col}: {cnt}")
    else:
        print("      - None (0 gaps interpolated)")

    print(f"\n    Remaining NULL values in clean view:")
    for col, cnt in sum_data["remaining_nulls"].items():
        if cnt > 0:
            print(f"      - {col}: {cnt}")

    # ── Display Warnings ──────────────────────────────────────────────────────
    if report.warnings:
        print(f"\n[3] PIPELINE WARNINGS & QUALITY NOTICES")
        for w in report.warnings:
            print(f"    - [WARN] {w}")

    # ── Display Sample Records ────────────────────────────────────────────────
    print(f"\n[4] SAMPLE CLEANED DATA (LIMIT 5)")
    spark.sql(f"SELECT * FROM {CLEAN_VIEW_NAME} LIMIT 5").show(truncate=False)

    # ── Display Spark SQL Log ─────────────────────────────────────────────────
    print(f"\n[5] SPARK SQL CLEANING STATEMENT LOG")
    for s in report.steps:
        print(f"    Step {s['step']}: {s['description']}")
        for line in s["sql"].splitlines():
            print(f"        {line}")

    # ── Save if Requested ─────────────────────────────────────────────────────
    if args.save:
        print(f"\n[6] PERSISTING OUTPUT DATASET & AUDIT REPORT")
        save_clean_dataset(spark, df_clean, report)

    print("\n" + "=" * 80)
    print(" RESULT: CLEANING PIPELINE SUCCEEDED (Exit Code 0)")
    print("=" * 80)
    return 0


if __name__ == "__main__":
    sys.exit(main())

