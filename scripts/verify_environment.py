"""
scripts/verify_environment.py
==============================
Standalone environment verification script.

Run this after creating the virtual environment and installing
requirements.txt to confirm that every component works correctly:

    python scripts/verify_environment.py

Each check prints PASS or FAIL so the result is immediately clear.
No external data files are needed.
"""

import subprocess
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Make sure the project root is on the Python path so we can import from
# config/ and src/ regardless of where this script is called from.
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
BOLD   = "\033[1m"
RESET  = "\033[0m"


def _pass(label: str, detail: str = "") -> None:
    suffix = f"  ({detail})" if detail else ""
    print(f"  {GREEN}PASS{RESET}  {label}{suffix}")


def _fail(label: str, detail: str = "") -> None:
    suffix = f"  ({detail})" if detail else ""
    print(f"  {RED}FAIL{RESET}  {label}{suffix}")


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------

def check_python_version() -> bool:
    """Python 3.9+ is required by PySpark 3.5."""
    version = sys.version
    major, minor = sys.version_info.major, sys.version_info.minor
    ok = (major == 3 and minor >= 9)
    if ok:
        _pass("Python version", version.split()[0])
    else:
        _fail("Python version", f"Found {version.split()[0]} – need 3.9+")
    return ok


def check_java_version() -> bool:
    """
    Java 11+ is required by Spark.  `java -version` writes to stderr, not stdout.
    We parse the first line to extract the version number.

    On Windows, capturing stderr with capture_output=True can deadlock.
    Using shell=True sidesteps that by letting the shell handle the pipe.
    """
    import platform
    try:
        is_windows = platform.system() == "Windows"
        if is_windows:
            # shell=True avoids the stderr-capture deadlock on Windows
            result = subprocess.run(
                "java -version",
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=15,
            )
            raw = result.stdout
        else:
            result = subprocess.run(
                ["java", "-version"],
                capture_output=True,
                text=True,
                timeout=15,
            )
            raw = result.stderr or result.stdout
        first_line = raw.strip().splitlines()[0] if raw.strip() else ""

        # Extract the version string, e.g. '17.0.9', '11.0.21', '1.8.0_392'
        import re
        match = re.search(r'"([\d._]+)"', first_line)
        if not match:
            _fail("Java version", f"Could not parse version from: {first_line!r}")
            return False

        version_str = match.group(1)
        # Old-style (1.8 → 8, 1.11 → 11); new-style (11, 17, 21…)
        parts = version_str.split(".")
        major = int(parts[1]) if parts[0] == "1" else int(parts[0])

        if major >= 11:
            _pass("Java version", f"{version_str} (major {major})")
            return True
        else:
            _fail(
                "Java version",
                f"Found Java {major} – Spark needs Java 11 or 17.\n"
                "    Install Java 17: https://adoptium.net/\n"
                "    Then set JAVA_HOME to the JDK root directory.",
            )
            return False

    except FileNotFoundError:
        _fail(
            "Java version",
            "java command not found.\n"
            "    Please install Java 17 from https://adoptium.net/\n"
            "    and set the JAVA_HOME environment variable.",
        )
        return False
    except subprocess.TimeoutExpired:
        _fail("Java version", "java -version timed out after 15 s")
        return False


def check_package(name: str, import_name: str | None = None) -> tuple[bool, str]:
    """
    Try to import a package and read its __version__.
    Returns (success, version_string).
    """
    import_name = import_name or name
    try:
        mod = __import__(import_name)
        version = getattr(mod, "__version__", "unknown")
        return True, version
    except ImportError:
        return False, "not installed"


def check_pyspark() -> bool:
    ok, version = check_package("pyspark")
    if ok:
        _pass("PySpark", version)
    else:
        _fail("PySpark", "run: pip install pyspark==3.5.3")
    return ok


def check_pandas() -> bool:
    ok, version = check_package("pandas")
    if ok:
        _pass("Pandas", version)
    else:
        _fail("Pandas", "run: pip install pandas==2.2.3")
    return ok


def check_streamlit() -> bool:
    ok, version = check_package("streamlit")
    if ok:
        _pass("Streamlit", version)
    else:
        _fail("Streamlit", "run: pip install streamlit==1.40.2")
    return ok


def check_plotly() -> bool:
    ok, version = check_package("plotly")
    if ok:
        _pass("Plotly", version)
    else:
        _fail("Plotly", "run: pip install plotly==5.24.1")
    return ok


def check_spark_sql() -> bool:
    """
    Create a local SparkSession, run a Spark SQL query using inline VALUES
    (pure SQL – no Python workers needed), and print the result.

    Why VALUES instead of createDataFrame()?
    ----------------------------------------
    createDataFrame() from a Python list calls sc.parallelize(), which spawns
    Python worker sub-processes.  Those workers crash on Python 3.13 + Windows
    due to a socket-handling incompatibility in PySpark 3.5.x.
    spark.sql("SELECT * FROM VALUES ...") executes entirely in the JVM via
    Py4J and never spawns workers – it is the correct approach for a pure
    Spark SQL verification test.

    NOTE: This query is used ONLY for connectivity verification.
    No fake weather data is generated anywhere in the project.
    """
    print("\n  Starting SparkSession (this may take ~10 s the first time)…")
    try:
        from src.spark_session import get_spark_session

        spark = get_spark_session()

        # Three-row connectivity test using pure Spark SQL VALUES syntax.
        # No Python workers are involved – the query runs entirely in the JVM.
        result_df = spark.sql(
            """
            SELECT id, label, value, value * 2 AS doubled
            FROM (
                VALUES (1, 'row_a', 10.0),
                       (2, 'row_b', 20.0),
                       (3, 'row_c', 30.0)
            ) AS t(id, label, value)
            ORDER BY id
            """
        )

        print("\n  Spark SQL result (connectivity test only):")
        result_df.show()

        row_count = result_df.count()
        if row_count == 3:
            _pass("Spark SQL query", f"returned {row_count} rows as expected")
            spark.stop()
            return True
        else:
            _fail("Spark SQL query", f"expected 3 rows, got {row_count}")
            spark.stop()
            return False

    except SystemExit:
        # get_spark_session() already printed a friendly message and called sys.exit.
        _fail("SparkSession creation", "see error above")
        return False
    except Exception as exc:  # noqa: BLE001
        _fail("Spark SQL query", str(exc))
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print(f"\n{BOLD}{'='*60}{RESET}")
    print(f"{BOLD}  Weather & Climate Spark – Environment Verification{RESET}")
    print(f"{BOLD}{'='*60}{RESET}\n")

    results: dict[str, bool] = {}

    print(f"{BOLD}[ Package & Runtime Checks ]{RESET}")
    results["Python"]    = check_python_version()
    results["Java"]      = check_java_version()
    results["PySpark"]   = check_pyspark()
    results["Pandas"]    = check_pandas()
    results["Streamlit"] = check_streamlit()
    results["Plotly"]    = check_plotly()

    print(f"\n{BOLD}[ Spark SQL Connectivity Check ]{RESET}")
    results["Spark SQL"] = check_spark_sql()

    # ── Summary ──────────────────────────────────────────────────────────────
    print(f"\n{BOLD}{'='*60}{RESET}")
    print(f"{BOLD}  SUMMARY{RESET}")
    print(f"{BOLD}{'='*60}{RESET}")
    all_passed = True
    for name, passed in results.items():
        status = f"{GREEN}PASS{RESET}" if passed else f"{RED}FAIL{RESET}"
        print(f"  {status}  {name}")
        if not passed:
            all_passed = False

    print(f"\n{BOLD}{'='*60}{RESET}")
    if all_passed:
        print(f"{BOLD}{GREEN}  All checks PASSED – environment is ready!{RESET}")
    else:
        print(f"{BOLD}{RED}  One or more checks FAILED – see details above.{RESET}")
    print(f"{BOLD}{'='*60}{RESET}\n")

    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()

