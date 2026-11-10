"""
src/utils.py
============
Path and environment utility functions for Apache Spark interoperability.
"""

from pathlib import Path


def to_spark_path(path: str | Path) -> str:
    """
    Convert a filesystem path into an absolute path string compatible with Spark.

    Technical Implementation Choice: as_posix() vs as_uri()
    --------------------------------------------------------
    When project directories contain whitespace (such as user profile directories
    on Windows: 'C:\\Users\\SK MD Rijooan Hasan\\...'):

    1. `path.resolve().as_uri()` converts spaces to URL-encoded `%20` escapes
       (e.g., 'file:///C:/Users/SK%20MD%20Rijooan%20Hasan/...'). Spark's underlying
       Hadoop FileSystem implementation on Windows does not decode percent-encoded
       characters in local file URIs, resulting in `[PATH_NOT_FOUND]` errors.
    2. `path.resolve().as_posix()` produces a normalized path with forward slashes
       and literal spaces (e.g., 'C:/Users/SK MD Rijooan Hasan/...'). Hadoop and
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

