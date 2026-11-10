"""
src/spark_session.py
====================
Factory function that creates (or reuses) a local SparkSession.

Design decisions
----------------
* find_java_home() – automatically discovers and validates JAVA_HOME across
  Windows, macOS, and Linux without hardcoding paths.
* getOrCreate()   – safe to call multiple times; returns the running session.
* local[*]        – uses all available CPU cores on the local machine.
* UTC timezone    – avoids timezone-related surprises when aggregating by date.
* WARN log level  – suppresses verbose INFO/DEBUG Spark noise in the console.
* Windows fixes   – localhost binding (127.0.0.1), UI disabled, explicit
  PYSPARK_PYTHON / PYSPARK_DRIVER_PYTHON pointing to the venv interpreter.
"""

import glob
import logging
import os
import re
import subprocess
import sys
from pathlib import Path

from pyspark.sql import SparkSession

# Import the application name from the central settings file.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import APP_NAME  # noqa: E402

# ── Critical on Windows ───────────────────────────────────────────────────────
# When Spark's JVM spawns Python worker sub-processes it runs the bare `python`
# command. On Windows that resolves to the Microsoft Store stub rather than the
# venv interpreter, causing "Python worker failed to connect back" errors.
#
# Pointing PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON at sys.executable (which IS
# the venv Python when this module is imported from inside the venv) solves it
# on all platforms without hardcoding any path.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

# ── Python 3.12+ compatibility (distutils shim for toPandas) ─────────────────
# In Python 3.12+, distutils was removed from the standard library (PEP 632).
# PySpark 3.5.3's pandas integration still imports distutils.version.LooseVersion.
# Provide a transparent fallback shim using packaging.version.Version so that
# DataFrame.toPandas() functions cleanly without requiring setuptools.
try:
    import distutils.version  # noqa: F401
except ModuleNotFoundError:
    import types
    from packaging.version import Version as _LooseVersion

    _distutils = types.ModuleType("distutils")
    _distutils_ver = types.ModuleType("distutils.version")
    _distutils_ver.LooseVersion = _LooseVersion
    _distutils.version = _distutils_ver
    sys.modules["distutils"] = _distutils
    sys.modules["distutils.version"] = _distutils_ver

# Module-level logger.
logger = logging.getLogger(__name__)


def _parse_java_version(name: str) -> tuple[int, tuple[int, ...]]:
    """
    Extract major version and full version tuple from a JDK directory name.

    Examples:
      'jdk-21.0.12'     -> (21, (21, 0, 12))
      'jdk-17.0.9+9'    -> (17, (17, 0, 9))
      'java-11-openjdk' -> (11, (11,))
      'jdk-1.8.0_351'   -> (8, (8, 0))
    """
    m = re.search(r"(?:jdk|java)[-_]?(\d+)(?:\.(\d+))?(?:\.(\d+))?", name, re.IGNORECASE)
    if m:
        parts = [int(p) if p is not None else 0 for p in m.groups()]
        if parts[0] == 1 and len(parts) > 1:
            major = parts[1]
            v_tuple = tuple(parts[1:])
        else:
            major = parts[0]
            v_tuple = tuple(parts)
        return major, v_tuple

    # Generic fallback: look for first number sequence
    m2 = re.search(r"(\d+)", name)
    if m2:
        val = int(m2.group(1))
        return val, (val,)
    return 0, (0,)


def find_java_home() -> str | None:
    """
    Detect and return the path to a valid Java 11+ JDK directory, or None.

    Search order:
      a) existing JAVA_HOME environment variable (if valid and contains bin/java[.exe])
      b) Windows: scan standard JDK installation folders (Program Files/Java,
         Eclipse Adoptium, Microsoft) and pick the highest version >= 11
      c) macOS: run /usr/libexec/java_home
      d) Linux: scan /usr/lib/jvm/* and pick the highest version >= 11

    Returns
    -------
    str | None
        Normalized string path to the JDK root, or None if no valid JDK was found.
    """
    bin_name = "java.exe" if sys.platform == "win32" else "java"

    # a) Existing JAVA_HOME env var
    existing = os.environ.get("JAVA_HOME")
    if existing:
        p = Path(existing)
        if p.is_dir() and (p / "bin" / bin_name).is_file():
            return str(p.resolve())

    # b) Windows scan
    if sys.platform == "win32":
        prog_files = os.environ.get("ProgramFiles", "C:/Program Files").replace("\\", "/")
        patterns = [
            f"{prog_files}/Java/jdk-*",
            f"{prog_files}/Eclipse Adoptium/jdk-*",
            f"{prog_files}/Microsoft/jdk-*",
        ]
        candidates = []
        for pattern in patterns:
            for folder in glob.glob(pattern):
                p = Path(folder)
                if p.is_dir() and (p / "bin" / bin_name).is_file():
                    major, v_tuple = _parse_java_version(p.name)
                    if major >= 11:
                        candidates.append((v_tuple, str(p.resolve())))
        if candidates:
            # Sort by version tuple descending to pick the highest version >= 11
            candidates.sort(key=lambda c: c[0], reverse=True)
            return candidates[0][1]

    # c) macOS check via /usr/libexec/java_home
    if sys.platform == "darwin":
        try:
            out = subprocess.check_output(
                ["/usr/libexec/java_home"],
                text=True,
                stderr=subprocess.DEVNULL,
            ).strip()
            p = Path(out)
            if p.is_dir() and (p / "bin" / bin_name).is_file():
                return str(p.resolve())
        except Exception:
            pass

    # d) Linux scan in /usr/lib/jvm/*
    if sys.platform.startswith("linux"):
        candidates = []
        for folder in glob.glob("/usr/lib/jvm/*"):
            p = Path(folder)
            if p.is_dir() and (p / "bin" / bin_name).is_file():
                major, v_tuple = _parse_java_version(p.name)
                if major >= 11:
                    candidates.append((v_tuple, str(p.resolve())))
        if candidates:
            candidates.sort(key=lambda c: c[0], reverse=True)
            return candidates[0][1]

    return None


def get_spark_session(app_name: str = APP_NAME) -> SparkSession:
    """
    Return a local SparkSession, creating it if one does not already exist.

    If JAVA_HOME is not set in the environment, find_java_home() is called
    to auto-detect a compatible JDK (Java 11+) and set os.environ["JAVA_HOME"]
    for the current process.

    Parameters
    ----------
    app_name : str
        The name shown in the Spark UI / logs. Defaults to APP_NAME.

    Returns
    -------
    SparkSession
        A configured, ready-to-use SparkSession.

    Raises
    ------
    RuntimeError
        If no compatible Java 11+ installation is found.
    SystemExit
        If SparkSession creation fails for other reasons.
    """
    # Auto-detect and configure JAVA_HOME if not already set
    if "JAVA_HOME" not in os.environ:
        detected_home = find_java_home()
        if detected_home:
            os.environ["JAVA_HOME"] = detected_home
            logger.info("Auto-configured JAVA_HOME=%s", detected_home)
        else:
            err_msg = (
                "\n[ERROR] JAVA_HOME is not set and no suitable Java 11+ installation could be found.\n"
                "  Apache Spark requires Java 11, 17, or 21 to run.\n\n"
                "  How to fix:\n"
                "  1. Download and install Eclipse Temurin (Java 17 or 21):\n"
                "     https://adoptium.net/temurin/releases/\n"
                "  2. Set the JAVA_HOME environment variable to your JDK installation path,\n"
                "     e.g., %ProgramFiles%/Eclipse Adoptium/jdk-17.x.x-hotspot\n"
                "  3. Ensure %JAVA_HOME%\\bin is added to your system PATH.\n"
            )
            print(err_msg, file=sys.stderr)
            raise RuntimeError(err_msg)

    try:
        spark = (
            SparkSession.builder
            # Run entirely on the local machine, using all CPU cores.
            .master("local[*]")
            .appName(app_name)
            # Treat all timestamps as UTC to avoid daylight-saving surprises.
            .config("spark.sql.session.timeZone", "UTC")
            # Bind the Spark driver to localhost only (prevents [WinError 10013]).
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            # Disable the Spark web UI (port 4040) to prevent port conflicts on Windows.
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )

        # Set Spark's internal logger to WARN (separate from Python's logging).
        spark.sparkContext.setLogLevel("WARN")

        logger.info("SparkSession '%s' is ready.", app_name)
        return spark

    except Exception as exc:  # noqa: BLE001
        friendly_message = (
            "\n[ERROR] Could not create a SparkSession.\n"
            f"  Root cause: {exc}\n\n"
            "  Common fixes:\n"
            "  1. Make sure Java 11, 17, or 21 is installed and JAVA_HOME is valid.\n"
            "  2. Run: pip install pyspark==3.5.3\n"
            "  3. On Windows, verify no firewall blocks 127.0.0.1 loopback sockets.\n"
        )
        print(friendly_message, file=sys.stderr)
        raise RuntimeError(friendly_message) from exc
