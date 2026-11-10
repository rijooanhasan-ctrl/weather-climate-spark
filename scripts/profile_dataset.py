"""
scripts/profile_dataset.py
==========================
Profiles the primary weather dataset using Apache Spark SQL.

Validates schema alignment, checks data completeness, null counts, duplicate keys,
summary statistics, and physical plausibility ranges using Spark SQL.
Outputs results to the console and writes a persistent report to docs/dataset_profile.txt.
"""

import io
import sys
from pathlib import Path

# Project root on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.schema import PLAUSIBLE_RANGES, STANDARD_SCHEMA  # noqa: E402
from config.settings import PRIMARY_DATASET_PATH, RAW_VIEW_NAME  # noqa: E402
from src.spark_session import get_spark_session  # noqa: E402
from src.utils import display_path, to_spark_path  # noqa: E402


class ProfileOutputCollector:
    """Tee writer that collects printed lines for saving to file."""

    def __init__(self) -> None:
        self.buffer = io.StringIO()

    def print(self, *args: object, **kwargs: object) -> None:
        file = kwargs.pop("file", None)
        text = " ".join(str(a) for a in args)
        print(text, file=file or sys.stdout, **kwargs)
        self.buffer.write(text + "\n")

    def get_output(self) -> str:
        return self.buffer.getvalue()


def run_profiling() -> None:
    collector = ProfileOutputCollector()
    p = collector.print

    p("=" * 80)
    p("  Weather and Climate Pattern Discovery – Dataset Profiling (Spark SQL)")
    p("=" * 80)

    # 1. Verify dataset file existence
    if not PRIMARY_DATASET_PATH.exists():
        p(
            f"\n[ERROR] Primary dataset not found at: {PRIMARY_DATASET_PATH}\n"
            "  Please run the acquisition script first:\n"
            "    python scripts/download_weather_data.py\n",
            file=sys.stderr,
        )
        sys.exit(1)

    spark_path = to_spark_path(PRIMARY_DATASET_PATH)
    p(f"Dataset Path : {display_path(PRIMARY_DATASET_PATH)}")
    p(f"Spark Path   : {display_path(PRIMARY_DATASET_PATH)}")
    p(f"View Name    : {RAW_VIEW_NAME}")

    # 2. Initialize SparkSession
    try:
        spark = get_spark_session("DatasetProfiling")
    except Exception as exc:
        p(f"\n[ERROR] Could not start Spark session: {exc}", file=sys.stderr)
        sys.exit(1)

    expected_column_names = [f.name for f in STANDARD_SCHEMA.fields]

    # 3. Read header only (without schema) to verify exact column names & order
    p("\n[Check 1] Header & Column Order Validation...")
    try:
        header_df = spark.read.option("header", "true").csv(spark_path)
        actual_columns = header_df.columns
    except Exception as exc:
        p(f"[ERROR] Failed to read CSV header: {exc}", file=sys.stderr)
        spark.stop()
        sys.exit(1)

    if actual_columns != expected_column_names:
        p(
            "\n[ERROR] CSV header does not match expected STANDARD_SCHEMA names/order.\n"
            f"  Expected: {expected_column_names}\n"
            f"  Found   : {actual_columns}\n",
            file=sys.stderr,
        )
        spark.stop()
        sys.exit(1)

    p("  PASS: CSV header perfectly matches STANDARD_SCHEMA column names and order.")

    # 4. Read file with explicit STANDARD_SCHEMA and FAILFAST mode
    p("\n[Check 2] Loading dataset with explicit STANDARD_SCHEMA (mode=FAILFAST)...")
    try:
        df = (
            spark.read
            .option("header", "true")
            .option("dateFormat", "yyyy-MM-dd")
            .option("mode", "FAILFAST")
            .schema(STANDARD_SCHEMA)
            .csv(spark_path)
        )
        df.createOrReplaceTempView(RAW_VIEW_NAME)
        p(f"  PASS: Registered temp view '{RAW_VIEW_NAME}' successfully.")
    except Exception as exc:
        p(f"[ERROR] Schema enforcement or view creation failed: {exc}", file=sys.stderr)
        spark.stop()
        sys.exit(1)

    # 5. Execute Profiling Queries
    # Query A: Dataset Overview
    p("\n" + "=" * 80)
    p("Query A: Dataset Overview (Total rows, distinct locations, date boundaries)")
    p("=" * 80)
    sql_a = f"""
    SELECT
        COUNT(*) AS total_rows,
        COUNT(DISTINCT location) AS distinct_locations,
        MIN(date) AS min_date,
        MAX(date) AS max_date
    FROM {RAW_VIEW_NAME}
    """
    p(sql_a.strip())
    res_a = spark.sql(sql_a).toPandas()
    p(res_a.to_string(index=False))

    # Query B: Coverage and completeness per location
    p("\n" + "=" * 80)
    p("Query B: Geographic & Temporal Completeness per Location")
    p("=" * 80)
    sql_b = f"""
    SELECT
        location,
        COUNT(*) AS row_count,
        MIN(date) AS min_date,
        MAX(date) AS max_date,
        DATEDIFF(MAX(date), MIN(date)) + 1 AS expected_days,
        (DATEDIFF(MAX(date), MIN(date)) + 1) - COUNT(*) AS missing_days
    FROM {RAW_VIEW_NAME}
    GROUP BY location
    ORDER BY location
    """
    p(sql_b.strip())
    res_b = spark.sql(sql_b).toPandas()
    p(res_b.to_string(index=False))

    # Query C: Null count per column
    p("\n" + "=" * 80)
    p("Query C: Missing Values (Null Count) Per Column")
    p("=" * 80)
    null_clauses = [
        f"SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS {col}_nulls"
        for col in expected_column_names
    ]
    sql_c = f"""
    SELECT
        {",\n        ".join(null_clauses)}
    FROM {RAW_VIEW_NAME}
    """
    p(sql_c.strip())
    res_c = spark.sql(sql_c).toPandas()
    # Transpose for clean presentation
    p(res_c.T.rename(columns={0: "null_count"}).to_string())

    # Query D: Duplicate key check
    p("\n" + "=" * 80)
    p("Query D: Duplicate Key Check on (date, location)")
    p("=" * 80)
    sql_d = f"""
    SELECT
        date,
        location,
        COUNT(*) AS duplicate_count
    FROM {RAW_VIEW_NAME}
    GROUP BY date, location
    HAVING COUNT(*) > 1
    """
    p(sql_d.strip())
    res_d = spark.sql(sql_d).toPandas()
    if len(res_d) == 0:
        p("  PASS: 0 duplicate (date, location) records found. Primary key integrity holds.")
    else:
        p(f"  WARNING: Found {len(res_d)} duplicate keys:")
        p(res_d.to_string(index=False))

    # Query E: Summary statistics
    p("\n" + "=" * 80)
    p("Query E: Overall Physical Statistics (MIN, AVG, MAX)")
    p("=" * 80)
    stats_cols = ["temperature", "rainfall", "humidity", "wind_speed", "pressure"]
    stats_unions = []
    for col in stats_cols:
        stats_unions.append(
            f"SELECT '{col}' AS metric, "
            f"ROUND(MIN({col}), 2) AS min_val, "
            f"ROUND(AVG({col}), 2) AS avg_val, "
            f"ROUND(MAX({col}), 2) AS max_val "
            f"FROM {RAW_VIEW_NAME}"
        )
    sql_e = "\nUNION ALL\n".join(stats_unions)
    p(sql_e.strip())
    res_e = spark.sql(sql_e).toPandas()
    p(res_e.to_string(index=False))

    # Query F: Physical plausibility range violations
    p("\n" + "=" * 80)
    p("Query F: Plausibility Range Outlier Check (Dynamic from PLAUSIBLE_RANGES)")
    p("=" * 80)
    plausible_clauses = []
    for col_name, (low, high) in PLAUSIBLE_RANGES.items():
        plausible_clauses.append(
            f"SUM(CASE WHEN {col_name} < {low} OR {col_name} > {high} THEN 1 ELSE 0 END) AS {col_name}_outliers"
        )
    sql_f = f"""
    SELECT
        {",\n        ".join(plausible_clauses)}
    FROM {RAW_VIEW_NAME}
    """
    p(sql_f.strip())
    res_f = spark.sql(sql_f).toPandas()
    p(res_f.T.rename(columns={0: "outlier_count"}).to_string())

    spark.stop()

    # 6. Save profiling output to docs/dataset_profile.txt
    docs_dir = PROJECT_ROOT / "docs"
    docs_dir.mkdir(parents=True, exist_ok=True)
    profile_txt_path = docs_dir / "dataset_profile.txt"
    profile_txt_path.write_text(collector.get_output(), encoding="utf-8")
    p("\n" + "=" * 80)
    p(f"Profiling report successfully saved to: {display_path(profile_txt_path)}")
    p("=" * 80)


if __name__ == "__main__":
    run_profiling()

