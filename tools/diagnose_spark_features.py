"""
scripts/diagnose_spark_features.py
==================================
Diagnostic script to test and report which PySpark / Spark SQL features
work reliably on the current host machine and runtime environment.

Tests performed (each in an isolated subprocess with a 120s timeout):
  T1. Spark SQL on built-in data: spark.sql("SELECT id, id*2 AS d FROM range(5)").collect()
  T2. Spark SQL aggregation + toPandas(): GROUP BY on range() -> .toPandas()
  T3. createDataFrame from a Pandas DataFrame -> temp view -> SQL query
  T4. RDD operation: spark.sparkContext.parallelize(range(5)).map(lambda x: x*2).collect()
  T5. spark.read.csv on a temporary CSV file -> temp view -> SQL query

This script is strictly for diagnostics and must NEVER be imported by the app.
"""

import argparse
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

# Project root on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# ---------------------------------------------------------------------------
# Individual test worker functions (executed in subprocess via --run-test)
# ---------------------------------------------------------------------------

def run_test_t1() -> None:
    """T1: Spark SQL on built-in data (range)."""
    from src.spark_session import get_spark_session

    spark = get_spark_session("Diag_T1")
    try:
        rows = spark.sql("SELECT id, id * 2 AS d FROM range(5)").collect()
        if len(rows) != 5:
            raise ValueError(f"Expected 5 rows, got {len(rows)}")
    finally:
        spark.stop()


def run_test_t2() -> None:
    """T2: Spark SQL aggregation + toPandas()."""
    import pandas as pd
    from src.spark_session import get_spark_session

    spark = get_spark_session("Diag_T2")
    try:
        df = spark.sql(
            """
            SELECT (id % 2) AS parity, COUNT(*) AS cnt, SUM(id) AS total
            FROM range(10)
            GROUP BY (id % 2)
            """
        )
        pdf = df.toPandas()
        if not isinstance(pdf, pd.DataFrame) or len(pdf) != 2:
            raise ValueError(f"Expected 2-row Pandas DataFrame, got {len(pdf)}")
    finally:
        spark.stop()


def run_test_t3() -> None:
    """T3: createDataFrame from a Pandas DataFrame -> SQL."""
    import pandas as pd
    from src.spark_session import get_spark_session

    spark = get_spark_session("Diag_T3")
    try:
        pdf = pd.DataFrame({"id": [1, 2, 3], "val": [10.0, 20.0, 30.0]})
        df = spark.createDataFrame(pdf)
        df.createOrReplaceTempView("temp_pandas_view")
        rows = spark.sql("SELECT id, val * 2 AS doubled FROM temp_pandas_view").collect()
        if len(rows) != 3:
            raise ValueError(f"Expected 3 rows, got {len(rows)}")
    finally:
        spark.stop()


def run_test_t4() -> None:
    """T4: RDD operation with lambda."""
    from src.spark_session import get_spark_session

    spark = get_spark_session("Diag_T4")
    try:
        res = spark.sparkContext.parallelize(range(5)).map(lambda x: x * 2).collect()
        if res != [0, 2, 4, 6, 8]:
            raise ValueError(f"Expected [0, 2, 4, 6, 8], got {res}")
    finally:
        spark.stop()


def run_test_t5() -> None:
    """T5: spark.read.csv on temporary CSV -> SQL."""
    from src.spark_session import get_spark_session

    spark = get_spark_session("Diag_T5")
    temp_file = tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False, newline="")
    temp_path = Path(temp_file.name)
    try:
        temp_file.write("id,category,amount\n1,Alpha,100.5\n2,Beta,200.75\n3,Gamma,300.0\n")
        temp_file.close()

        df = spark.read.csv(str(temp_path), header=True, inferSchema=True)
        df.createOrReplaceTempView("temp_csv_view")
        rows = spark.sql("SELECT id, category, amount FROM temp_csv_view WHERE amount > 150").collect()
        if len(rows) != 2:
            raise ValueError(f"Expected 2 rows, got {len(rows)}")
    finally:
        try:
            temp_path.unlink(missing_ok=True)
        except Exception:
            pass
        spark.stop()


TEST_RUNNERS = {
    "T1": run_test_t1,
    "T2": run_test_t2,
    "T3": run_test_t3,
    "T4": run_test_t4,
    "T5": run_test_t5,
}

TEST_DESCRIPTIONS = {
    "T1": "Spark SQL on built-in data (range)",
    "T2": "Spark SQL aggregation + toPandas()",
    "T3": "createDataFrame from Pandas DataFrame",
    "T4": "RDD operation (parallelize + map lambda)",
    "T5": "spark.read.csv on temporary CSV + SQL",
}


# ---------------------------------------------------------------------------
# Diagnostics runner
# ---------------------------------------------------------------------------

def _get_java_info() -> str:
    """Return formatted Java version and location."""
    from src.spark_session import find_java_home

    home = find_java_home()
    if not home:
        return "Not detected"

    # Try running java -version
    java_bin = Path(home) / "bin" / ("java.exe" if sys.platform == "win32" else "java")
    if not java_bin.is_file():
        return f"{home} (bin not found)"

    try:
        res = subprocess.run(
            [str(java_bin), "-version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=10,
        )
        first = res.stdout.strip().splitlines()[0] if res.stdout.strip() else ""
        m = re.search(r'"([\d._]+)"', first)
        ver = m.group(1) if m else "unknown"
        return f"Java {ver} ({home})"
    except Exception as exc:
        return f"{home} (version check failed: {exc})"


def _extract_short_error(raw: str) -> str:
    """Extract a concise single-line error cause from stderr/stdout."""
    lines = [line.strip() for line in raw.splitlines() if line.strip()]
    if not lines:
        return "Failed with non-zero exit code"

    # Scan from bottom up for key error phrases
    for line in reversed(lines):
        # Look for typical exception lines
        if any(kw in line for kw in ["Exception:", "Error:", "Caused by:", "SocketTimeoutException", "WinError"]):
            # Strip module paths if overly verbose
            clean = line
            if "Caused by: " in clean:
                clean = clean.split("Caused by: ", 1)[1]
            return clean[:90]

    return lines[-1][:90]


def run_diagnostics() -> None:
    """Execute all tests in isolated subprocesses and print diagnostic table."""
    print("=" * 80)
    print("  PySpark Feature Diagnostics")
    print("=" * 80)

    # Environment Header
    py_ver = sys.version.split()[0]
    try:
        import pyspark

        spark_ver = pyspark.__version__
    except ImportError:
        spark_ver = "Not installed"

    java_info = _get_java_info()

    print(f"  Python  : {py_ver} ({sys.executable})")
    print(f"  PySpark : {spark_ver}")
    print(f"  Java    : {java_info}")
    print("=" * 80)
    print(f"{'Test':<6} | {'Status':<7} | {'Description':<42} | Details")
    print("-" * 80)

    script_path = str(Path(__file__).resolve())

    for test_id, desc in TEST_DESCRIPTIONS.items():
        cmd = [sys.executable, script_path, "--run-test", test_id]

        try:
            res = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=120,
            )
            if res.returncode == 0:
                status = "PASS"
                details = "OK"
            else:
                status = "FAIL"
                raw_err = (res.stderr or "") + "\n" + (res.stdout or "")
                details = _extract_short_error(raw_err)
        except subprocess.TimeoutExpired:
            status = "TIMEOUT"
            details = "Timed out after 120s"

        print(f"{test_id:<6} | {status:<7} | {desc:<42} | {details}")

    print("=" * 80)


def main() -> None:
    parser = argparse.ArgumentParser(description="PySpark feature diagnostic tool")
    parser.add_argument("--run-test", choices=list(TEST_RUNNERS.keys()), help="Run single test in subprocess")
    args = parser.parse_args()

    if args.run_test:
        runner = TEST_RUNNERS[args.run_test]
        runner()
    else:
        run_diagnostics()


if __name__ == "__main__":
    main()

