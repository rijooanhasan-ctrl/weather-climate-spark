"""
src/spark_session.py
====================
Factory function that creates (or reuses) a local SparkSession.

Design decisions
----------------
* getOrCreate()   – safe to call multiple times; returns the running session.
* local[*]        – uses all available CPU cores on the local machine.
* UTC timezone    – avoids timezone-related surprises when aggregating by date.
* WARN log level  – suppresses the verbose INFO/DEBUG Spark noise in the console.
"""

import logging
import os
import sys

from pyspark.sql import SparkSession

# Import the application name from the central settings file.
# We use a relative import because src/ is a proper Python package.
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
from config.settings import APP_NAME  # noqa: E402

# ── Critical on Windows ───────────────────────────────────────────────────────
# When Spark's JVM spawns Python worker sub-processes it runs the bare `python`
# command.  On Windows that resolves to the Microsoft Store stub rather than the
# venv interpreter, causing "Python worker failed to connect back" errors.
#
# Pointing PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON at sys.executable (which IS
# the venv Python when this module is imported from inside the venv) solves it
# on all platforms without hardcoding any path.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

# Module-level logger (not the Spark logger – this is for our own messages).
logger = logging.getLogger(__name__)


def get_spark_session(app_name: str = APP_NAME) -> SparkSession:
    """
    Return a local SparkSession, creating it if one does not already exist.

    Parameters
    ----------
    app_name : str
        The name shown in the Spark UI / logs.  Defaults to the value in
        config/settings.py so callers rarely need to pass it explicitly.

    Returns
    -------
    SparkSession
        A configured, ready-to-use SparkSession.

    Raises
    ------
    SystemExit
        If the SparkSession cannot be created (e.g. Java missing).
    """
    try:
        spark = (
            SparkSession.builder
            # Run entirely on the local machine, using all CPU cores.
            .master("local[*]")
            .appName(app_name)
            # Treat all timestamps as UTC to avoid daylight-saving surprises.
            .config("spark.sql.session.timeZone", "UTC")
            # Bind the Spark driver to localhost only.
            # On Windows, binding to 0.0.0.0 is often blocked by the firewall;
            # 127.0.0.1 avoids the [WinError 10013] socket permission error.
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            # Disable the Spark web UI (port 4040).
            # The UI is useful for tuning but its port is frequently blocked on
            # Windows and is not needed for this project's core SQL work.
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )

        # Set Spark's internal logger to WARN (separate from Python's logging).
        spark.sparkContext.setLogLevel("WARN")

        logger.info("SparkSession '%s' is ready.", app_name)
        return spark

    except Exception as exc:  # noqa: BLE001
        # Print a friendly message so the user understands what went wrong
        # without having to read a Java stack trace.
        print(
            "\n[ERROR] Could not create a SparkSession.\n"
            f"  Root cause: {exc}\n\n"
            "  Common fixes:\n"
            "  1. Make sure Java 11 or 17 is installed and JAVA_HOME is set.\n"
            "  2. Run: pip install pyspark==3.5.3\n"
            "  3. On Windows, install winutils if prompted.\n",
            file=sys.stderr,
        )
        sys.exit(1)

