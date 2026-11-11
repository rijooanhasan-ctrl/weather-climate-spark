"""
scripts/ingest_dataset.py
=========================
Command-line ingestion and validation utility for the Weather & Climate Spark system.

Demonstrates CSV ingestion, delimiter detection, header mapping, date format detection,
schema standardization, and comprehensive Spark SQL data quality validation.

Usage
-----
python scripts/ingest_dataset.py
python scripts/ingest_dataset.py --file path/to/dataset.csv
python scripts/ingest_dataset.py --file data.csv --map temp=temperature --date-format yyyy-MM-dd
python scripts/ingest_dataset.py --file single_city.csv --location-name "Bengaluru"

Exit Codes
----------
0 : Validation passed with 0 errors
1 : Ingestion completed but validation errors were found
2 : Fatal file loading or configuration error
"""

import argparse
from pathlib import Path
import sys

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.schema import STANDARD_SCHEMA
from config.settings import PRIMARY_DATASET_PATH
from src.data_loader import DatasetLoadError, load_dataset
from src.spark_session import get_spark_session
from src.utils import display_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest, standardize, and validate weather CSV datasets using Spark SQL."
    )
    parser.add_argument(
        "--file",
        type=str,
        default=str(PRIMARY_DATASET_PATH),
        help="Path to the weather CSV dataset to ingest (default: primary dataset).",
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
            # Default to left as standard target
            result[left] = right
    return result


def main() -> int:
    args = parse_args()
    file_path = Path(args.file)

    print("=" * 80)
    print(" WEATHER & CLIMATE DATASET INGESTION & VALIDATION ")
    print("=" * 80)
    print(f"Target file: {display_path(file_path)}")

    if not file_path.exists():
        print(f"\n[ERROR] File not found: {display_path(file_path)}", file=sys.stderr)
        return 2

    try:
        custom_mapping = parse_mapping_args(args.mapping_args)
    except ValueError as exc:
        print(f"\n[ERROR] {exc}", file=sys.stderr)
        return 2

    # Initialize SparkSession
    try:
        spark = get_spark_session(app_name="DatasetIngestionCLI")
    except Exception as exc:
        print(f"\n[ERROR] Failed to start Spark session: {exc}", file=sys.stderr)
        return 2

    try:
        result = load_dataset(
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

    # ── Display Ingestion Summary ─────────────────────────────────────────────
    print(f"\n[1] DELIMITER DETECTION")
    print(f"    Detected delimiter: {repr(result.delimiter)}")

    print(f"\n[2] COLUMN MAPPING")
    print(f"    {'Standard Column':<16} {'Mapped Original Column':<24} {'Status'}")
    print(f"    {'-'*16} {'-'*24} {'-'*12}")
    for std_field in STANDARD_SCHEMA.fields:
        name = std_field.name
        orig = result.mapping.get(name)
        if orig:
            status = "Mapped"
        elif name == "location" and args.location_name:
            orig = f"'{args.location_name}'"
            status = "Default"
        else:
            orig = "(none)"
            status = "Unmapped"
        print(f"    {name:<16} {orig:<24} {status}")

    print(f"\n[3] DATE FORMAT DETECTION")
    df_info = result.date_format_info
    fmt = df_info.get("format") or "(none)"
    rate = df_info.get("parse_rate", 0.0)
    ambiguous = df_info.get("is_ambiguous", False)
    note = df_info.get("note", "")
    print(f"    Detected Format : {fmt}")
    print(f"    Parse Success   : {rate:.1%}")
    print(f"    Is Ambiguous    : {'Yes' if ambiguous else 'No'}")
    print(f"    Notes           : {note}")

    print(f"\n[4] DATA QUALITY VALIDATION REPORT")
    val = result.validation
    status_label = "PASS" if val.is_valid else "FAIL"
    print(f"    Validation Status: [{status_label}] (Errors: {len(val.errors)}, Warnings: {len(val.warnings)})")

    if val.errors:
        print("\n    Errors (must be resolved):")
        for err in val.errors:
            print(f"      - [FAIL] {err}")

    if val.warnings:
        print("\n    Warnings (data quality notices):")
        for warn in val.warnings:
            print(f"      - [WARN] {warn}")

    if val.column_stats:
        print(f"\n[5] NUMERIC COLUMN SUMMARY STATISTICS")
        print(f"    {'Column':<14} {'Unit':<6} {'Min':<10} {'Avg':<10} {'Max':<10} {'Valid Rows'}")
        print(f"    {'-'*14} {'-'*6} {'-'*10} {'-'*10} {'-'*10} {'-'*10}")
        for cs in val.column_stats:
            col_name = cs["column"]
            unit = cs.get("unit", "")
            c_min = f"{cs['min']:.2f}" if cs["min"] is not None else "N/A"
            c_avg = f"{cs['avg']:.2f}" if cs["avg"] is not None else "N/A"
            c_max = f"{cs['max']:.2f}" if cs["max"] is not None else "N/A"
            cnt = cs["count"]
            print(f"    {col_name:<14} {unit:<6} {c_min:<10} {c_avg:<10} {c_max:<10} {cnt}")

    if result.standardized_view and val.info.get("total_rows", 0) > 0:
        print(f"\n[6] SAMPLE STANDARDIZED DATA (LIMIT 5)")
        spark.sql(f"SELECT * FROM {result.standardized_view} LIMIT 5").show(truncate=False)

    print(f"\n[7] SPARK SQL EXECUTION AUDIT LOG")
    print(f"    Total queries executed: {len(result.sql_log)}")
    for i, entry in enumerate(result.sql_log, start=1):
        print(f"    Q{i}: {entry['title']}")
        # Indent SQL lines for clean reading
        for line in entry['sql'].splitlines():
            print(f"        {line}")

    print("\n" + "=" * 80)
    if val.is_valid:
        print(" RESULT: INGESTION SUCCEEDED (Exit Code 0)")
        print("=" * 80)
        return 0
    else:
        print(" RESULT: VALIDATION FAILED (Exit Code 1)")
        print("=" * 80)
        return 1


if __name__ == "__main__":
    sys.exit(main())

