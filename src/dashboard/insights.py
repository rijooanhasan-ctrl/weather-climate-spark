"""
src/dashboard/insights.py
=========================
Automated, deterministic natural language climate insights generated from Spark SQL results.
Pure Python and pandas.

PROJECT RULES ENFORCED:
- Zero recomputation of analytical metrics in Pandas or NumPy.
- All metrics and aggregations originate directly from Apache Spark SQL.
- Pure string synthesis, formatting, filtering, and sorting.
- Clean language policy: factual observations without speculation.
- Every number appearing in the generated text MUST be explicitly recorded in its evidence dictionary.
- Deterministic output capped at settings.INSIGHT_MAX_ITEMS.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import math
from typing import Any

import pandas as pd

from config import settings


@dataclass
class Insight:
    """Structured insight synthesized from Spark SQL analytical outputs."""
    category: str  # "temperature", "rainfall", "extremes", "relationships", "data quality"
    text: str
    level: str  # "info" or "caution"
    source_keys: list[str]
    evidence: dict[str, Any] = field(default_factory=dict)


def generate_insights(
    frames: dict[str, pd.DataFrame],
    kpis: dict[str, Any],
    corr_anomaly_df: pd.DataFrame | None = None,
) -> list[Insight]:
    """
    Generate deterministic, evidence-backed natural language climate findings.
    
    Parameters
    ----------
    frames : dict[str, pd.DataFrame]
        Dictionary of analysis query results mapped by key.
    kpis : dict[str, Any]
        KPI summary dictionary from get_kpis().
    corr_anomaly_df : pd.DataFrame | None
        Optional anomaly-mode correlation DataFrame to detect seasonal cycle effects.

    Returns
    -------
    list[Insight]
        List of insights capped at INSIGHT_MAX_ITEMS.
    """
    if not kpis or kpis.get("total_records", 0) == 0:
        return []

    insights: list[Insight] = []

    # ─────────────────────────────────────────────────────────────────────────
    # Rule 1: Hottest and Coldest Location by Average Temperature
    # ─────────────────────────────────────────────────────────────────────────
    rank_df = frames.get("temp_location_ranking")
    if rank_df is not None and not rank_df.empty and "location" in rank_df.columns:
        valid_ranks = rank_df.dropna(subset=["avg_temp"]).sort_values("avg_temp")
        n_locs = len(valid_ranks)
        if n_locs == 1:
            row = valid_ranks.iloc[0]
            loc = str(row["location"])
            avg_t = round(float(row["avg_temp"]), 1)
            insights.append(
                Insight(
                    category="temperature",
                    text=f"The average temperature for {loc} is {avg_t:.1f} °C.",
                    level="info",
                    source_keys=["temp_location_ranking"],
                    evidence={"location": loc, "avg_temp": avg_t},
                )
            )
        elif n_locs >= 2:
            cold_row = valid_ranks.iloc[0]
            hot_row = valid_ranks.iloc[-1]
            c_loc, c_avg = str(cold_row["location"]), round(float(cold_row["avg_temp"]), 1)
            h_loc, h_avg = str(hot_row["location"]), round(float(hot_row["avg_temp"]), 1)
            insights.append(
                Insight(
                    category="temperature",
                    text=(
                        f"Across the observed stations, {h_loc} recorded the highest average "
                        f"temperature at {h_avg:.1f} °C, while {c_loc} recorded the lowest "
                        f"average temperature at {c_avg:.1f} °C."
                    ),
                    level="info",
                    source_keys=["temp_location_ranking"],
                    evidence={
                        "hottest_location": h_loc,
                        "highest_avg_temp": h_avg,
                        "coldest_location": c_loc,
                        "lowest_avg_temp": c_avg,
                    },
                )
            )

    # ─────────────────────────────────────────────────────────────────────────
    # Rule 2: Multi-Decadal Temperature Trends
    # ─────────────────────────────────────────────────────────────────────────
    trend_df = frames.get("temp_trend")
    if trend_df is not None and not trend_df.empty:
        # Prefer "All locations" or first available row
        target_rows = trend_df[trend_df["location"] == "All locations"]
        row = target_rows.iloc[0] if not target_rows.empty else trend_df.iloc[0]
        label = str(row.get("trend_label", ""))

        if label == "insufficient data":
            insights.append(
                Insight(
                    category="temperature",
                    text=(
                        "Temperature trend analysis indicates insufficient data across complete "
                        "years to establish a trend. Note that 16 years of data is a short record "
                        "for climatic trends."
                    ),
                    level="caution",
                    source_keys=["temp_trend"],
                    evidence={"trend_label": "insufficient data", "record_length_years": 16},
                )
            )
        elif label == "no clear trend":
            insights.append(
                Insight(
                    category="temperature",
                    text=(
                        "Temperature trend analysis shows no clear trend across complete years. "
                        "Note that 16 years of data is a short record for climatic trends."
                    ),
                    level="info",
                    source_keys=["temp_trend"],
                    evidence={"trend_label": "no clear trend", "record_length_years": 16},
                )
            )
        elif label in ("warming", "cooling"):
            slope = round(float(row["slope_per_decade"]), 3)
            r2 = round(float(row["r_squared"]), 2)
            n_yrs = int(row["n_years"])
            insights.append(
                Insight(
                    category="temperature",
                    text=(
                        f"Multi-decadal temperature trend indicates a {label} rate of {slope:+.3f} °C "
                        f"per decade (R² = {r2:.2f}, based on {n_yrs} complete years). Note that 16 years "
                        f"of data is a short record for climatic trends."
                    ),
                    level="info",
                    source_keys=["temp_trend"],
                    evidence={
                        "trend_label": label,
                        "slope_per_decade": slope,
                        "r_squared": r2,
                        "n_years": n_yrs,
                        "record_length_years": 16,
                    },
                )
            )

    # ─────────────────────────────────────────────────────────────────────────
    # Rule 3: Historical Record High and Low Temperatures
    # ─────────────────────────────────────────────────────────────────────────
    if kpis.get("max_temp") is not None and kpis.get("min_temp") is not None:
        max_t = round(float(kpis["max_temp"]), 1)
        max_loc = str(kpis.get("max_temp_location", ""))
        max_d = str(kpis.get("max_temp_date", ""))
        min_t = round(float(kpis["min_temp"]), 1)
        min_loc = str(kpis.get("min_temp_location", ""))
        min_d = str(kpis.get("min_temp_date", ""))
        insights.append(
            Insight(
                category="temperature",
                text=(
                    f"The historical record high temperature was {max_t:.1f} °C observed at {max_loc} "
                    f"on {max_d}, and the record low was {min_t:.1f} °C observed at {min_loc} on {min_d}."
                ),
                level="info",
                source_keys=["kpi_summary"],
                evidence={
                    "record_max_temp": max_t,
                    "record_max_location": max_loc,
                    "record_max_date": max_d,
                    "record_min_temp": min_t,
                    "record_min_location": min_loc,
                    "record_min_date": min_d,
                },
            )
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Rule 4: Wettest and Driest Location by Annual Rainfall
    # ─────────────────────────────────────────────────────────────────────────
    rain_rank_df = frames.get("rain_location_ranking")
    if rain_rank_df is not None and not rain_rank_df.empty:
        valid_rain = rain_rank_df.dropna(subset=["avg_annual_rain"]).sort_values("avg_annual_rain")
        n_rain_locs = len(valid_rain)
        if n_rain_locs >= 2:
            dry_r = valid_rain.iloc[0]
            wet_r = valid_rain.iloc[-1]
            w_loc, w_val = str(wet_r["location"]), round(float(wet_r["avg_annual_rain"]), 1)
            w_m_num = int(wet_r.get("wettest_month", 1))
            w_m_name = settings.MONTH_NAMES[w_m_num - 1] if 1 <= w_m_num <= 12 else str(w_m_num)
            w_yrs = int(wet_r.get("complete_years_used", 16))

            d_loc, d_val = str(dry_r["location"]), round(float(dry_r["avg_annual_rain"]), 1)
            d_yrs = int(dry_r.get("complete_years_used", 16))

            insights.append(
                Insight(
                    category="rainfall",
                    text=(
                        f"{w_loc} is the wettest station with an annual average of {w_val:,.1f} mm "
                        f"({w_yrs} complete years used), peaking in {w_m_name}. {d_loc} is the driest "
                        f"station with {d_val:,.1f} mm per year ({d_yrs} complete years used)."
                    ),
                    level="info",
                    source_keys=["rain_location_ranking"],
                    evidence={
                        "wettest_location": w_loc,
                        "highest_annual_rain_mm": w_val,
                        "wettest_month": w_m_name,
                        "wettest_complete_years": w_yrs,
                        "driest_location": d_loc,
                        "lowest_annual_rain_mm": d_val,
                        "driest_complete_years": d_yrs,
                    },
                )
            )
        elif n_rain_locs == 1:
            row = valid_rain.iloc[0]
            loc, val = str(row["location"]), round(float(row["avg_annual_rain"]), 1)
            m_num = int(row.get("wettest_month", 1))
            m_name = settings.MONTH_NAMES[m_num - 1] if 1 <= m_num <= 12 else str(m_num)
            yrs = int(row.get("complete_years_used", 16))
            insights.append(
                Insight(
                    category="rainfall",
                    text=(
                        f"The average annual rainfall for {loc} is {val:,.1f} mm across {yrs} complete "
                        f"years, peaking in {m_name}."
                    ),
                    level="info",
                    source_keys=["rain_location_ranking"],
                    evidence={
                        "location": loc,
                        "avg_annual_rain_mm": val,
                        "complete_years_used": yrs,
                        "wettest_month": m_name,
                    },
                )
            )

    # ─────────────────────────────────────────────────────────────────────────
    # Rule 5: Monsoon Share of Annual Rainfall
    # ─────────────────────────────────────────────────────────────────────────
    season_df = frames.get("rain_seasonal")
    if season_df is not None and not season_df.empty and "season" in season_df.columns:
        monsoon_rows = season_df[season_df["season"] == "Monsoon"].sort_values("share_of_annual_pct")
        if len(monsoon_rows) >= 2:
            min_m = monsoon_rows.iloc[0]
            max_m = monsoon_rows.iloc[-1]
            max_loc, max_pct = str(max_m["location"]), round(float(max_m["share_of_annual_pct"]), 1)
            min_loc, min_pct = str(min_m["location"]), round(float(min_m["share_of_annual_pct"]), 1)
            insights.append(
                Insight(
                    category="rainfall",
                    text=(
                        f"Monsoon rainfall concentration is highest in {max_loc} where the summer "
                        f"monsoon constitutes {max_pct:.1f} % of annual precipitation, compared to "
                        f"{min_pct:.1f} % in {min_loc}."
                    ),
                    level="info",
                    source_keys=["rain_seasonal"],
                    evidence={
                        "highest_monsoon_location": max_loc,
                        "highest_monsoon_share_pct": max_pct,
                        "lowest_monsoon_location": min_loc,
                        "lowest_monsoon_share_pct": min_pct,
                    },
                )
            )
        elif len(monsoon_rows) == 1:
            row = monsoon_rows.iloc[0]
            loc, pct = str(row["location"]), round(float(row["share_of_annual_pct"]), 1)
            insights.append(
                Insight(
                    category="rainfall",
                    text=f"Monsoon rainfall constitutes {pct:.1f} % of the annual precipitation in {loc}.",
                    level="info",
                    source_keys=["rain_seasonal"],
                    evidence={"location": loc, "monsoon_share_pct": pct},
                )
            )

    # ─────────────────────────────────────────────────────────────────────────
    # Rule 6: Unusual Rainfall Months (Z-Score >= 2.0)
    # ─────────────────────────────────────────────────────────────────────────
    unusual_df = frames.get("rain_unusual_months")
    if unusual_df is not None and not unusual_df.empty:
        if "z_score" in unusual_df.columns and unusual_df["z_score"].isna().all():
            insights.append(
                Insight(
                    category="rainfall",
                    text="There are not enough years of historical observations to compute standardized rainfall anomaly Z-scores.",
                    level="info",
                    source_keys=["rain_unusual_months"],
                    evidence={"status": "insufficient_baseline_years"},
                )
            )
        elif "is_unusually_high" in unusual_df.columns:
            flagged = unusual_df[unusual_df["is_unusually_high"] == True]
            cnt = len(flagged)
            if cnt > 0:
                top_r = flagged.sort_values("z_score", ascending=False).iloc[0]
                t_loc = str(top_r["location"])
                t_yr = int(top_r["year"])
                t_m = int(top_r["month"])
                m_name = settings.MONTH_NAMES[t_m - 1] if 1 <= t_m <= 12 else str(t_m)
                t_z = round(float(top_r["z_score"]), 2)
                t_tot = round(float(top_r["total_rain"]), 1)
                insights.append(
                    Insight(
                        category="rainfall",
                        text=(
                            f"Identified {cnt} unusual rainfall month(s) exceeding Z >= 2.0, with "
                            f"the most extreme anomaly at {t_loc} in {m_name} {t_yr} "
                            f"(Z = {t_z:.2f}, total rain: {t_tot:,.1f} mm)."
                        ),
                        level="info",
                        source_keys=["rain_unusual_months"],
                        evidence={
                            "unusual_months_count": cnt,
                            "most_extreme_location": t_loc,
                            "year": t_yr,
                            "month": m_name,
                            "max_z_score": t_z,
                            "total_rain_mm": t_tot,
                        },
                    )
                )

    # ─────────────────────────────────────────────────────────────────────────
    # Rule 7: Extreme Heat (Hot Days & Longest Heatwave Spell)
    # ─────────────────────────────────────────────────────────────────────────
    events_df = frames.get("extreme_events_yearly")
    hw_df = frames.get("heatwave_spells")
    if events_df is not None and not events_df.empty and "hot_days" in events_df.columns:
        valid_ev = events_df.dropna(subset=["hot_days"])
        if not valid_ev.empty:
            top_ev = valid_ev.sort_values("hot_days", ascending=False).iloc[0]
            ev_loc = str(top_ev["location"])
            ev_yr = int(top_ev["year"])
            ev_cnt = int(top_ev["hot_days"])

            # Spell component
            spell_part = "no spells of at least 3 days were recorded"
            spell_ev: dict[str, Any] = {"longest_spell_days": 0}
            if hw_df is not None and not hw_df.empty and "length_days" in hw_df.columns:
                valid_hw = hw_df.dropna(subset=["length_days"])
                if not valid_hw.empty:
                    top_hw = valid_hw.sort_values("length_days", ascending=False).iloc[0]
                    h_loc = str(top_hw.get("location", ""))
                    h_len = int(top_hw["length_days"])
                    h_st = str(top_hw["spell_start"])
                    h_peak = round(float(top_hw.get("peak_temp_max", 0.0)), 1)
                    spell_part = (
                        f"the longest heatwave spell lasted {h_len} consecutive days in {h_loc} "
                        f"starting {h_st} (peak {h_peak:.1f} °C)"
                    )
                    spell_ev = {
                        "longest_spell_location": h_loc,
                        "longest_spell_days": h_len,
                        "spell_start": h_st,
                        "peak_temp_max": h_peak,
                    }

            insights.append(
                Insight(
                    category="extremes",
                    text=(
                        f"The highest annual extreme heat occurrence was {ev_cnt} hot days in "
                        f"{ev_loc} during {ev_yr}; {spell_part}."
                    ),
                    level="info",
                    source_keys=["extreme_events_yearly", "heatwave_spells"],
                    evidence={
                        "most_hot_days_location": ev_loc,
                        "year": ev_yr,
                        "hot_days_count": ev_cnt,
                        **spell_ev,
                    },
                )
            )

    # ─────────────────────────────────────────────────────────────────────────
    # Rule 8: Heavy Rain Frequency
    # ─────────────────────────────────────────────────────────────────────────
    if events_df is not None and not events_df.empty and "heavy_rain_days" in events_df.columns:
        valid_rain_ev = events_df.dropna(subset=["heavy_rain_days"])
        if not valid_rain_ev.empty:
            top_r_ev = valid_rain_ev.sort_values("heavy_rain_days", ascending=False).iloc[0]
            r_loc = str(top_r_ev["location"])
            r_yr = int(top_r_ev["year"])
            r_days = int(top_r_ev["heavy_rain_days"])
            insights.append(
                Insight(
                    category="extremes",
                    text=(
                        f"The highest frequency of heavy rainfall occurred in {r_loc} during {r_yr} "
                        f"with {r_days} deluge days recorded."
                    ),
                    level="info",
                    source_keys=["extreme_events_yearly"],
                    evidence={"location": r_loc, "year": r_yr, "heavy_rain_days_count": r_days},
                )
            )

    # ─────────────────────────────────────────────────────────────────────────
    # Rule 9: Atmospheric Variable Correlations
    # ─────────────────────────────────────────────────────────────────────────
    corr_df = frames.get("correlations")
    if corr_df is not None and not corr_df.empty:
        # Filter for all-location entries and exclude self-correlations
        all_corr = corr_df[
            (corr_df.get("location", "All locations") == "All locations")
            & (corr_df["var_x"] != corr_df["var_y"])
        ].copy()

        if not all_corr.empty:
            all_corr["abs_r"] = all_corr["r"].abs()
            sorted_corr = all_corr.sort_values("abs_r", ascending=False)
            top_row = sorted_corr.iloc[0]
            vx, vy = str(top_row["var_x"]), str(top_row["var_y"])
            r_val = round(float(top_row["r"]), 2)
            n_pairs = int(top_row["n_pairs"])
            abs_val = abs(r_val)

            strength = "strong" if abs_val >= settings.INSIGHT_MIN_ABS_CORR else ("moderate" if abs_val >= 0.3 else "weak")

            # Check if anomaly mode differs by >= INSIGHT_ANOMALY_DIFF
            seasonal_note = ""
            if corr_anomaly_df is not None and not corr_anomaly_df.empty:
                anom_match = corr_anomaly_df[
                    ((corr_anomaly_df["var_x"] == vx) & (corr_anomaly_df["var_y"] == vy))
                    | ((corr_anomaly_df["var_x"] == vy) & (corr_anomaly_df["var_y"] == vx))
                ]
                if not anom_match.empty:
                    anom_r = float(anom_match.iloc[0]["r"])
                    if abs(r_val - anom_r) >= settings.INSIGHT_ANOMALY_DIFF:
                        seasonal_note = " Much of this association reflects the shared seasonal cycle."

            insights.append(
                Insight(
                    category="relationships",
                    text=(
                        f"The strongest atmospheric correlation is between {vx} and {vy} with a "
                        f"{strength} Pearson coefficient of r = {r_val:+.2f} (N = {n_pairs:,} pairs).{seasonal_note} "
                        f"Correlation does not imply causation."
                    ),
                    level="info",
                    source_keys=["correlations"],
                    evidence={
                        "strongest_pair": f"{vx} vs {vy}",
                        "pearson_r": r_val,
                        "n_pairs": n_pairs,
                        "strength": strength,
                    },
                )
            )

    # ─────────────────────────────────────────────────────────────────────────
    # Rule 10: Data-Quality Cautions (Interpolation & Incomplete Years)
    # ─────────────────────────────────────────────────────────────────────────
    interp_cnt = int(kpis.get("interpolated_value_count", 0))
    if interp_cnt > 0:
        insights.append(
            Insight(
                category="data quality",
                text=f"A total of {interp_cnt:,} missing observations were reconstructed using linear interpolation (short gaps <= 3 days).",
                level="caution",
                source_keys=["kpi_summary"],
                evidence={"interpolated_value_count": interp_cnt, "max_gap_days": 3},
            )
        )

    # Check incomplete years from yearly frames
    y_df = frames.get("rain_yearly")
    if y_df is None or y_df.empty:
        y_df = frames.get("temp_yearly")
    if y_df is not None and not y_df.empty and "is_complete_year" in y_df.columns:
        has_incomp = (y_df["is_complete_year"] == False).any()
        if has_incomp:
            insights.append(
                Insight(
                    category="data quality",
                    text=(
                        "Incomplete observation years (under 95 % completeness) are present in the "
                        "dataset and have been excluded from multi-decadal trend calculations and "
                        "annual climatological baselines."
                    ),
                    level="caution",
                    source_keys=["rain_yearly", "temp_yearly"],
                    evidence={
                        "incomplete_years_present": True,
                        "min_complete_year_coverage_pct": 95,
                    },
                )
            )

    return insights[: settings.INSIGHT_MAX_ITEMS]
