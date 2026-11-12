"""
tests/test_insights.py
======================
Unit tests for deterministic natural language climate insights generator.
Ensures conformance to all 10 insight rules, clean language policies,
and evidence dictionary mirroring.
"""

from __future__ import annotations

import re
import pandas as pd
import pytest

from src.dashboard.insights import Insight, generate_insights


@pytest.fixture
def base_kpis() -> dict:
    return {
        "total_records": 46752,
        "locations": 8,
        "avg_temp": 25.4,
        "max_temp": 48.2,
        "max_temp_date": "2024-05-29",
        "max_temp_location": "Delhi",
        "min_temp": 1.2,
        "min_temp_date": "2013-01-06",
        "min_temp_location": "Delhi",
        "total_rainfall_mm": 52400.0,
        "avg_annual_rainfall_per_location": 1050.0,
        "avg_humidity": 65.2,
        "avg_wind_speed": 9.4,
        "avg_pressure": 1008.2,
        "interpolated_value_count": 42,
    }


def test_empty_selection_returns_empty():
    """Empty dataset or 0 records must return an empty list of insights."""
    assert generate_insights({}, {}) == []
    assert generate_insights({}, {"total_records": 0}) == []


def test_hottest_coldest_multi_location(base_kpis):
    """Rule 1: Identifies hottest and coldest stations when >= 2 locations exist."""
    rank_df = pd.DataFrame([
        {"location": "Bengaluru", "avg_temp": 23.8},
        {"location": "Delhi", "avg_temp": 25.1},
        {"location": "Chennai", "avg_temp": 28.5},
    ])
    frames = {"temp_location_ranking": rank_df}
    insights = generate_insights(frames, base_kpis)

    rule1_insights = [i for i in insights if "temp_location_ranking" in i.source_keys]
    assert len(rule1_insights) >= 1
    ins = rule1_insights[0]
    assert "Chennai" in ins.text
    assert "28.5" in ins.text
    assert "Bengaluru" in ins.text
    assert "23.8" in ins.text
    assert ins.evidence["hottest_location"] == "Chennai"
    assert ins.evidence["highest_avg_temp"] == 28.5
    assert ins.evidence["coldest_location"] == "Bengaluru"
    assert ins.evidence["lowest_avg_temp"] == 23.8


def test_single_location_wording(base_kpis):
    """Rule 1: Uses singular phrasing for single-location slices, not comparative words."""
    rank_df = pd.DataFrame([
        {"location": "Mumbai", "avg_temp": 27.2},
    ])
    frames = {"temp_location_ranking": rank_df}
    insights = generate_insights(frames, base_kpis)

    rule1 = [i for i in insights if "temp_location_ranking" in i.source_keys]
    assert len(rule1) >= 1
    ins = rule1[0]
    assert "Across the observed stations" not in ins.text
    assert "The average temperature for Mumbai is 27.2 °C." in ins.text
    assert ins.evidence["location"] == "Mumbai"
    assert ins.evidence["avg_temp"] == 27.2


def test_wettest_location(base_kpis):
    """Rule 4: Correctly identifies wettest location by annual rainfall."""
    rain_rank_df = pd.DataFrame([
        {"location": "Delhi", "total_rain": 12000.0, "avg_annual_rain": 750.0, "wettest_month": 7, "complete_years_used": 16},
        {"location": "Mumbai", "total_rain": 36000.0, "avg_annual_rain": 2250.0, "wettest_month": 7, "complete_years_used": 16},
        {"location": "Jaipur", "total_rain": 9600.0, "avg_annual_rain": 600.0, "wettest_month": 8, "complete_years_used": 16},
    ])
    frames = {"rain_location_ranking": rain_rank_df}
    insights = generate_insights(frames, base_kpis)

    rule4 = [i for i in insights if "rain_location_ranking" in i.source_keys]
    assert len(rule4) >= 1
    ins = rule4[0]
    assert "Mumbai" in ins.text
    assert "2,250.0 mm" in ins.text
    assert ins.evidence["wettest_location"] == "Mumbai"
    assert ins.evidence["highest_annual_rain_mm"] == 2250.0


def test_record_high_and_low_temperatures(base_kpis):
    """Rule 3: Captures historical all-time maximum and minimum temperature records."""
    insights = generate_insights({}, base_kpis)
    rule3 = [i for i in insights if "kpi_summary" in i.source_keys and "historical record high" in i.text]
    assert len(rule3) == 1
    ins = rule3[0]
    assert "48.2 °C" in ins.text
    assert "1.2 °C" in ins.text
    assert "Delhi" in ins.text
    assert ins.evidence["record_max_temp"] == 48.2
    assert ins.evidence["record_min_temp"] == 1.2
    assert ins.evidence["record_max_location"] == "Delhi"


def test_strongest_correlation(base_kpis):
    """Rule 8: Captures strongest non-identity correlation above threshold."""
    corr_df = pd.DataFrame([
        {"location": "All locations", "var_x": "temp_max", "var_y": "temperature", "r": 0.88, "n_pairs": 46000},
        {"location": "All locations", "var_x": "rainfall", "var_y": "humidity", "r": 0.52, "n_pairs": 46000},
        {"location": "All locations", "var_x": "temperature", "var_y": "pressure", "r": -0.45, "n_pairs": 46000},
    ])
    frames = {"correlations": corr_df}
    insights = generate_insights(frames, base_kpis)

    rule8 = [i for i in insights if "correlations" in i.source_keys]
    assert len(rule8) >= 1
    ins = rule8[0]
    assert "temp_max" in ins.text
    assert "temperature" in ins.text
    assert "+0.88" in ins.text or "0.88" in ins.text
    assert ins.evidence["strongest_pair"] == "temp_max vs temperature"
    assert ins.evidence["pearson_r"] == 0.88


def test_insufficient_data_3_year_slice(base_kpis):
    """Short record periods or insufficient data in trend analysis triggers caution."""
    trend_df = pd.DataFrame([
        {"location": "All locations", "trend_label": "insufficient data"}
    ])
    frames = {"temp_trend": trend_df}
    insights = generate_insights(frames, base_kpis)

    cautions = [i for i in insights if "insufficient data" in i.text.lower() or "caution" in i.level]
    assert len(cautions) >= 1
    assert "insufficient data" in cautions[0].text.lower()
    assert cautions[0].evidence.get("trend_label") == "insufficient data"


def test_no_forbidden_causal_or_predictive_words(base_kpis):
    """Insights must NEVER contain 'because', 'caused by', or 'will '."""
    frames = {
        "temp_location_ranking": pd.DataFrame([{"location": "Delhi", "avg_temp": 25.0}]),
        "rain_location_ranking": pd.DataFrame([
            {"location": "Delhi", "total_rain": 5000.0, "avg_annual_rain": 500.0, "wettest_month": 7, "complete_years_used": 16}
        ]),
        "temp_yearly": pd.DataFrame([
            {"location": "Delhi", "year": y, "avg_temp": 25.0, "is_complete_year": True}
            for y in range(2010, 2025)
        ]),
        "correlations": pd.DataFrame([
            {"location": "All locations", "var_x": "temp_max", "var_y": "temperature", "r": 0.85, "n_pairs": 46000}
        ]),
    }
    insights = generate_insights(frames, base_kpis)

    forbidden_patterns = [r"\bbecause\b", r"\bcaused by\b", r"\bwill\s"]
    for ins in insights:
        for pat in forbidden_patterns:
            assert not re.search(pat, ins.text, re.IGNORECASE), (
                f"Forbidden pattern '{pat}' detected in insight: '{ins.text}'"
            )


def test_numbers_in_evidence_dict(base_kpis):
    """Every numeric value displayed in generated text must exist in the evidence dictionary."""
    rank_df = pd.DataFrame([
        {"location": "Bengaluru", "avg_temp": 23.8},
        {"location": "Chennai", "avg_temp": 28.5},
    ])
    frames = {
        "temp_location_ranking": rank_df,
    }
    insights = generate_insights(frames, base_kpis)

    for ins in insights:
        # Extract floating point numbers and integers from text
        tokens = re.findall(r"\b\d+(?:\.\d+)?\b", ins.text)
        evidence_vals = []
        for v in ins.evidence.values():
            if isinstance(v, (int, float)):
                evidence_vals.append(round(float(v), 2))
                evidence_vals.append(round(float(v), 1))
                evidence_vals.append(int(v))
            elif isinstance(v, str):
                tokens_in_ev = re.findall(r"\b\d+(?:\.\d+)?\b", v)
                evidence_vals.extend([float(x) for x in tokens_in_ev if '.' in x])
                evidence_vals.extend([int(x) for x in tokens_in_ev if '.' not in x])

        for tok in tokens:
            num = float(tok)
            # Check if this number is represented in evidence dict
            assert any(abs(num - ev) < 0.05 for ev in evidence_vals if isinstance(ev, (int, float))), (
                f"Number {tok} in text '{ins.text}' was not found in evidence {ins.evidence}"
            )
