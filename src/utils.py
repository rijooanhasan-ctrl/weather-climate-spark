"""
src/utils.py
============
Path and environment utility functions for Apache Spark interoperability
and secure, privacy-preserving path display.
"""

from pathlib import Path

# Project root (BASE_DIR)
BASE_DIR = Path(__file__).resolve().parent.parent


def to_spark_path(path: str | Path) -> str:
    """
    Convert a filesystem path into an absolute path string compatible with Spark.

    Technical Implementation Choice: as_posix() vs as_uri()
    --------------------------------------------------------
    When project directories contain whitespace:
    1. `path.resolve().as_uri()` converts spaces to URL-encoded `%20` escapes
       (e.g., 'file:///drive/project/my%20folder/data.csv'). Spark's underlying
       Hadoop FileSystem implementation on Windows does not unquote percent-encoded
       characters in local file URIs, resulting in `[PATH_NOT_FOUND]` errors.
    2. `path.resolve().as_posix()` produces a normalized path with forward slashes
       and literal spaces (e.g., 'D:/project/my folder/data.csv'). Hadoop and
       Spark accept this format across Windows, Linux, and macOS without path
       resolution failures.

    Therefore, `path.resolve().as_posix()` is chosen and used consistently.

    Parameters
    ----------
    path : str | Path
        Input path to normalize.

    Returns
    -------
    str
        Absolute, normalized POSIX-style path string accepted by spark.read.
    """
    p = Path(path) if isinstance(path, str) else path
    return p.resolve().as_posix()


def display_path(path: str | Path) -> str:
    """
    Return a relative, privacy-safe path string for display in logs and UI.

    Parameters
    ----------
    path : str | Path
        Input file or directory path.

    Returns
    -------
    str
        Path relative to BASE_DIR using forward slashes (e.g., 'data/raw/dataset.csv'),
        falling back to the file name if the path is outside BASE_DIR.
    """
    p = Path(path).resolve()
    try:
        return p.relative_to(BASE_DIR).as_posix()
    except ValueError:
        return p.name
