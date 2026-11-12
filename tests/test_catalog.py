"""
tests/test_catalog.py
=====================
Integrity and completeness tests for the dashboard UI catalog.
Validates that all 22 analysis queries are mapped to valid sections and builders.
"""

from __future__ import annotations

import inspect
import pytest

from src.dashboard import charts
from src.dashboard.catalog import ANALYSIS_UI_MAP, SECTIONS
from src.queries.registry import ANALYSES_BY_KEY


def test_catalog_keys_match_all_registered_analyses():
    """All 22 registered Spark SQL queries must be mapped in ANALYSIS_UI_MAP."""
    registered_keys = set(ANALYSES_BY_KEY.keys())
    catalog_keys = set(ANALYSIS_UI_MAP.keys())

    assert len(registered_keys) == 22, f"Expected 22 registered queries, got {len(registered_keys)}"
    assert catalog_keys == registered_keys, (
        f"Mismatch between registered queries and catalog keys: "
        f"Missing: {registered_keys - catalog_keys}, Extra: {catalog_keys - registered_keys}"
    )


def test_catalog_sections_valid():
    """Every analysis in ANALYSIS_UI_MAP must assign a valid section from SECTIONS."""
    assert len(SECTIONS) == 8
    expected_sections = {
        "Overview",
        "Temperature",
        "Rainfall",
        "Climate patterns",
        "Data quality",
        "Cleaning report",
        "Data preview",
        "Spark SQL",
    }
    assert set(SECTIONS) == expected_sections

    for key, spec in ANALYSIS_UI_MAP.items():
        sec = spec.get("section")
        assert sec in SECTIONS, f"Analysis '{key}' references unknown section '{sec}'"


def test_catalog_builders_exist_in_charts_module():
    """Every non-null builder named in ANALYSIS_UI_MAP must be a callable in src.dashboard.charts."""
    for key, spec in ANALYSIS_UI_MAP.items():
        builder_name = spec.get("builder")
        if builder_name is not None:
            assert hasattr(charts, builder_name), (
                f"Analysis '{key}' specifies builder '{builder_name}', "
                f"but it does not exist in src.dashboard.charts"
            )
            fn = getattr(charts, builder_name)
            assert callable(fn), f"'{builder_name}' in src.dashboard.charts must be callable"


def test_catalog_required_metadata_fields():
    """Every analysis in ANALYSIS_UI_MAP must have title, description, section, and kind."""
    required_fields = {"section", "kind", "builder", "title", "description"}
    for key, spec in ANALYSIS_UI_MAP.items():
        missing = required_fields - set(spec.keys())
        assert not missing, f"Analysis '{key}' missing required metadata fields: {missing}"
        assert len(spec["title"]) > 0
        assert len(spec["description"]) > 0
        assert spec["kind"] in {"chart", "table", "chart+table"}

