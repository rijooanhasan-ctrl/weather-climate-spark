"""
src/sql_utils.py
================
SQL utility functions for safely constructing Spark SQL statements, escaping
identifiers and string literals to prevent injection attacks and syntax errors.
"""

from __future__ import annotations


def sql_string_literal(text: str | None) -> str:
    """
    Safely quote and escape a Python string for use as a Spark SQL string literal.

    Parameters
    ----------
    text : str or None
        The string to escape. If None, returns the SQL literal NULL.

    Returns
    -------
    str
        Properly escaped and single-quoted string literal (e.g. 'O''Brien').
    """
    if text is None:
        return "NULL"
    # In Spark SQL, backslashes and single quotes must be escaped
    escaped = str(text).replace("\\", "\\\\").replace("'", "''")
    return f"'{escaped}'"


def quote_identifier(name: str) -> str:
    """
    Safely quote a SQL column or table identifier using backticks.

    Parameters
    ----------
    name : str
        Column or table identifier.

    Returns
    -------
    str
        Backtick-quoted identifier with embedded backticks escaped.
    """
    escaped = str(name).replace("`", "``")
    return f"`{escaped}`"
