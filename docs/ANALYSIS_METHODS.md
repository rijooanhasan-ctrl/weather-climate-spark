# Meteorological & Climate Analysis Methods

This document describes the analytical questions, mathematical formulations, Spark SQL techniques,
meteorological thresholds, and scientific limitations of the analysis engine in **Weather and Climate Pattern Discovery using Spark SQL**.

---

## 1. Architecture & Execution Model

The analysis engine operates strictly on the standardized and cleaned temporary view (`weather_clean`).
- **Engine**: Apache Spark 3.5.3 (Spark SQL via local PySpark).
- **Execution Strategy**: Lazy evaluation via Catalyst Optimizer. Queries are expressed as declarative SQL statements returning `QueryResult` wrappers containing Spark `DataFrame` objects.
- **Memory & Scale**: The clean view is cached in memory (`spark.catalog.cacheTable`), enabling high-throughput interactive query execution across multi-decadal time series.
- **Safety**: Safe collection limit (`ANALYSIS_RESULT_MAX_ROWS = 5000`) prevents Out-Of-Memory (OOM) errors when converting query results to Pandas or dashboard visualization objects.

---

## 2. Temperature Analyses

### 2.1 `temp_yearly`: Annual Temperature Summary & Anomalies
- **Question**: How has the annual mean temperature and thermal range evolved for each city over time, and what are the yearly anomalies relative to the historical baseline?
- **Columns**: `location`, `year`, `avg_temp`, `avg_temp_max`, `avg_temp_min`, `record_max`, `record_min`, `days_with_data`, `is_complete_year`, `anomaly_vs_location_mean`, `yoy_change`.
- **SQL Techniques**:
  - `DATEDIFF(make_date(year + 1, 1, 1), make_date(year, 1, 1))` to compute actual calendar days per year (handling leap years).
  - Completeness flag: `days_with_data / days_in_year >= 0.95`.
  - Window functions:
    - `ROUND(avg_temp - AVG(avg_temp) OVER (PARTITION BY location), 2)` computes baseline anomaly against the multi-year location mean.
    - `ROUND(avg_temp - LAG(avg_temp, 1) OVER (PARTITION BY location ORDER BY year), 2)` computes year-over-year (YoY) temperature change.

### 2.2 `temp_monthly_climatology`: Monthly Temperature Climatology
- **Question**: What is the typical annual temperature cycle across calendar months (January to December) for each station?
- **Columns**: `location`, `month`, `month_name`, `climatological_mean_temp`, `avg_temp_max`, `avg_temp_min`, `record_high`, `record_low`, `years_observed`.
- **SQL Techniques**:
  - Grouping by `location` and `month`.
  - `CASE month WHEN 1 THEN 'January' ... END` for calendar month labeling.
  - Multi-year averaging: `AVG(temperature)` over 16 years.

### 2.3 `temp_monthly_series`: Monthly Temperature Time Series
- **Question**: How do month-by-month temperatures track over the chronological observation period?
- **Columns**: `location`, `year`, `month`, `avg_temp`, `avg_temp_max`, `avg_temp_min`, `record_max`, `record_min`, `days_with_data`, `monthly_anomaly`.
- **SQL Techniques**:
  - Window function `AVG(avg_temp) OVER (PARTITION BY location, month)` computes the station's monthly climatological normal.
  - `monthly_anomaly = avg_temp - climatological_normal` isolates specific weather events from seasonal cycles.

### 2.4 `temp_location_ranking`: Location Temperature Rankings
- **Question**: Which observation stations are the hottest and coldest overall, and what are their all-time temperature records and dates?
- **Columns**: `location`, `avg_temp`, `record_max`, `record_max_date`, `record_min`, `record_min_date`, `hottest_rank`, `coldest_rank`.
- **SQL Techniques**:
  - Station summary CTE computing overall mean and absolute min/max.
  - Window ranking: `RANK() OVER (ORDER BY avg_temp DESC)` (hottest) and `RANK() OVER (ORDER BY avg_temp ASC)` (coldest).
  - Exact extreme date lookup via conditional aggregation: `FIRST(CASE WHEN b.temp_max = s.max_val THEN CAST(b.date AS STRING) END, true)`.

### 2.5 `temp_trend`: Decadal Temperature Trends (Linear Regression)
- **Question**: Are temperatures warming, cooling, or stable over the multi-year observation period, and what is the decadal rate of change?
- **Columns**: `location`, `slope_c_per_year`, `slope_per_decade`, `r_squared`, `n_years`, `first_year_avg`, `last_year_avg`, `trend_label`.
- **SQL Techniques**:
  - Filters strictly for `is_complete_year = true` (incomplete years excluded from trend fitting).
  - Native Spark aggregate regression functions:
    - `regr_slope(avg_temp, CAST(year AS DOUBLE))` computes the ordinary least squares (OLS) linear slope ($\beta_1$).
    - `regr_r2(avg_temp, CAST(year AS DOUBLE))` computes the coefficient of determination ($R^2$).
  - `slope_per_decade = slope_c_per_year * 10`.
  - Trend classification:
    - If $N < 5$: `'insufficient data'`
    - Else if $R^2 < 0.10$: `'no clear trend'`
    - Else if $\text{slope} > 0$: `'warming'`
    - Else: `'cooling'`
  - Station-specific regression unioned with overall aggregate (`'All locations'`).

### 2.6 `temp_seasonal`: Seasonal Temperature Analysis
- **Question**: What are the seasonal temperature characteristics across Winter, Summer, Monsoon, and Post-monsoon?
- **Columns**: `location`, `season`, `avg_temp`, `avg_temp_max`, `avg_temp_min`, `record_max`, `record_min`, `diurnal_temperature_range`.
- **SQL Techniques**:
  - `diurnal_temperature_range = avg_temp_max - avg_temp_min`.
  - Grouping by `location` and meteorological `season`.

---

## 3. Rainfall Analyses

### 3.1 `rain_monthly`: Monthly Precipitation Totals & Rainy Days
- **Question**: How much rain fell in each calendar month, and how many rainy days were recorded?
- **Columns**: `location`, `year`, `month`, `total_rain`, `rainy_days`, `max_daily`, `days_with_data`.
- **SQL Techniques**:
  - Indian Meteorological Department (IMD) rainy day criterion: `COUNT(CASE WHEN rainfall >= 2.5 THEN 1 END)`.
  - `SUM(rainfall)` and `MAX(rainfall)` per month.

### 3.2 `rain_yearly`: Annual Precipitation Totals & Rainy Days
- **Question**: What are the annual precipitation volumes, rainy day counts, and daily records?
- **Columns**: `location`, `year`, `total_rain`, `rainy_days`, `max_daily_rain`, `days_with_data`, `is_complete_year`, `anomaly_vs_mean`, `yoy_pct_change`.
- **SQL Techniques**:
  - Window functions for baseline annual anomalies: `total_rain - AVG(total_rain) OVER (PARTITION BY location)`.
  - Year-over-year percentage change: `((total_rain - prev_rain) / prev_rain) * 100.0`.

### 3.3 `rain_monthly_climatology`: Monthly Rainfall Climatology
- **Question**: What is the expected monthly rainfall and rainy day distribution across a typical year?
- **Columns**: `location`, `month`, `month_name`, `avg_monthly_rain`, `avg_rainy_days`, `record_monthly_rain`, `years_observed`.
- **SQL Techniques**:
  - Multi-year averaging of monthly aggregates.

### 3.4 `rain_location_ranking`: Location Rainfall Ranking
- **Question**: Which stations receive the highest and lowest annual precipitation?
- **Columns**: `location`, `avg_annual_rain`, `avg_annual_rainy_days`, `wettest_year`, `wettest_year_rain`, `driest_year`, `driest_year_rain`, `wettest_rank`, `driest_rank`.
- **SQL Techniques**:
  - Restricts calculations to complete calendar years.
  - Window functions for ranking: `RANK() OVER (ORDER BY avg_annual_rain DESC)`.
  - Conditional aggregation for identifying wettest and driest years.

### 3.5 `rain_seasonal`: Seasonal Rainfall Distribution & Shares
- **Question**: What percentage of annual rainfall falls during each season (e.g. Monsoon vs Winter)?
- **Columns**: `location`, `season`, `avg_seasonal_rain`, `avg_rainy_days`, `share_of_annual_pct`.
- **SQL Techniques**:
  - Window function `SUM(avg_seasonal_rain) OVER (PARTITION BY location)` calculates the annual total.
  - `share_of_annual_pct = (avg_seasonal_rain / annual_total) * 100.0`.

### 3.6 `rain_unusual_months`: Unusual Rainfall Detection (Z-Score)
- **Question**: Which calendar months experienced extreme precipitation anomalies (> 2.0 standard deviations above historical normal)?
- **Columns**: `location`, `year`, `month`, `total_rain`, `month_mean`, `month_std`, `z_score`, `is_unusually_high`.
- **SQL Techniques**:
  - Standard deviation across multi-year monthly totals: `STDDEV_SAMP(total_rain)`.
  - Baseline sample size requirement: at least 8 years of historical observations (`MIN_YEARS_FOR_ZSCORE = 8`).
  - Standardized Z-Score:
    $$Z = \frac{\text{total\_rain} - \mu}{\sigma}$$
  - Flagged when $Z \ge 2.0$.

### 3.7 `rain_heavy_days`: IMD Heavy Rainfall Classification
- **Question**: How frequently do heavy, very heavy, and extremely heavy rainfall days occur?
- **Columns**: `location`, `total_heavy_days`, `very_heavy_days`, `extremely_heavy_days`, `record_daily_rain`, `record_daily_date`.
- **SQL Techniques**:
  - Conditional counting based on official IMD thresholds:
    - Heavy Rain: $\ge 64.5\text{ mm}$
    - Very Heavy Rain: $\ge 115.6\text{ mm}$
    - Extremely Heavy Rain: $\ge 204.5\text{ mm}$

---

## 4. Climate Profiles, Extremes, and Correlations

### 4.1 `climate_year_profile`: Comprehensive Annual Profile
- **Question**: What was the complete meteorological profile (temperature, rain, humidity, wind, pressure) across all stations for each year?
- **Columns**: `year`, `locations_reporting`, `days_recorded`, `avg_temperature`, `avg_temp_max`, `avg_temp_min`, `total_rainfall_per_loc`, `avg_rainy_days`, `avg_humidity`, `avg_wind_speed`, `avg_pressure`.
- **SQL Techniques**:
  - Aggregates multi-station observations per calendar year.

### 4.2 `climate_year_comparison`: Year-over-Year Head-to-Head Comparison
- **Question**: How do two specific years compare across all atmospheric indicators?
- **Columns**: `metric`, `value_a`, `value_b`, `difference`, `pct_change`.
- **SQL Techniques**:
  - Unpivoting 8 meteorological indicators into a tabular vertical comparison via `UNION ALL`.
  - Safe delta calculation with NULL handling when `value_a == 0.0`.

### 4.3 `climate_city_profile`: Multi-City Meteorological Profile
- **Question**: What is the multi-decadal climatological summary for each station across all atmospheric variables?
- **Columns**: `location`, `latitude`, `longitude`, `years_active`, `mean_temp`, `mean_temp_max`, `mean_temp_min`, `mean_annual_rain`, `mean_annual_rainy_days`, `mean_humidity`, `mean_wind_speed`, `mean_pressure`.

### 4.4 `extreme_thresholds`: Station Extreme Weather Percentiles
- **Question**: What are the station-specific thresholds for hot days, cold days, and extreme wet days?
- **Columns**: `location`, `hot_day_threshold`, `cold_day_threshold`, `wet_day_extreme_threshold`.
- **SQL Techniques**:
  - `percentile_approx(temp_max, 0.95, 10000)` computes the 95th percentile maximum temperature.
  - `percentile_approx(temp_min, 0.05, 10000)` computes the 5th percentile minimum temperature.
  - `percentile_approx(rainfall, 0.95, 10000)` evaluated on wet days (`rainfall >= 2.5 mm`).
  - Evaluated on the full clean dataset to maintain rigorous climatological baselines independent of user filters.

### 4.5 `extreme_events_yearly`: Annual Extreme Event Frequencies
- **Question**: How many extreme hot days, cold days, and extreme precipitation days occurred each year?
- **Columns**: `location`, `year`, `hot_days`, `cold_days`, `extreme_rain_days`, `total_rain_mm`.
- **SQL Techniques**:
  - Joins station observations with full-period extreme thresholds.

### 4.6 `extreme_top_events`: Historical Extreme Records
- **Question**: What were the absolute top extreme events (hottest days, coldest days, wettest days) on record?
- **Columns**: `event_type`, `rank`, `date`, `location`, `magnitude`, `unit`.
- **SQL Techniques**:
  - Window ranking `DENSE_RANK() OVER (ORDER BY temp_max DESC)` for hottest, `temp_min ASC` for coldest, and `rainfall DESC` for wettest.
  - Unioned and filtered to Top $N$ (`TOP_N_DEFAULT = 10`).

### 4.7 `heatwave_spells`: Heatwave Spells (Gaps-and-Islands)
- **Question**: When did prolonged heatwaves ($\ge 3$ consecutive days above station 95th percentile) occur, and how long did they last?
- **Columns**: `location`, `spell_start`, `spell_end`, `length_days`, `peak_temp_max`.
- **SQL Techniques**:
  - Classic SQL Gaps-and-Islands pattern:
    $$\text{spell\_grp} = \text{date} - \text{ROW\_NUMBER}() \text{ OVER (PARTITION BY location ORDER BY date)}$$
  - When observations on consecutive days qualify, $\text{date} - \text{row\_number}$ remains invariant.
  - Grouping by `location` and `spell_grp` identifies continuous unbroken episodes.
  - Filtered by `HAVING COUNT(*) >= 3`.

### 4.8 `correlations`: Atmospheric Variable Correlation Matrix
- **Question**: How do atmospheric variables co-vary (e.g. temperature vs humidity, pressure vs wind speed)?
- **Columns**: `location`, `var_x`, `var_y`, `r`, `n_pairs`.
- **SQL Techniques**:
  - Native Spark aggregate `CORR(var_x, var_y)` computing sample Pearson correlation coefficient:
    $$r = \frac{\sum (x - \bar{x})(y - \bar{y})}{\sqrt{\sum (x - \bar{x})^2 \sum (y - \bar{y})^2}}$$
  - Modes:
    - `raw`: Pairwise correlation on raw daily observations.
    - `anomaly`: Deseasonalized correlation computed after subtracting the station's monthly mean ($\text{var} - \mu_{\text{loc, month}}$), isolating high-frequency atmospheric dynamics from seasonal co-variation.

---

## 5. Executive KPIs

### 5.1 `kpi_summary`: High-Level Climate KPI Dashboard
- **Question**: What are the top-level metrics summarizing the entire dataset?
- **Columns**: `total_records`, `locations`, `first_date`, `last_date`, `avg_temp`, `max_temp`, `max_temp_location`, `max_temp_date`, `min_temp`, `min_temp_location`, `min_temp_date`, `total_rainfall_mm`, `avg_humidity`, `avg_wind_speed`, `avg_pressure`, `wettest_year`, `hottest_year`, `interpolated_value_count`.
- **SQL Techniques**:
  - Global aggregations combining records, temporal extent, extreme values with station/date coordinates, and data quality metrics.

---

## 6. Meteorological Thresholds & Standards

All thresholds follow international and national meteorological standards:
- **Rainy Day**: $\ge 2.5\text{ mm}$ (IMD definition).
- **Heavy Rain**: $64.5\text{ mm} \le \text{Rain} < 115.6\text{ mm}$ (IMD).
- **Very Heavy Rain**: $115.6\text{ mm} \le \text{Rain} < 204.5\text{ mm}$ (IMD).
- **Extremely Heavy Rain**: $\ge 204.5\text{ mm}$ (IMD).
- **Extreme Hot Day**: $\ge 95\text{th percentile}$ of historical station maximum temperatures.
- **Extreme Cold Day**: $\le 5\text{th percentile}$ of historical station minimum temperatures.
- **Extreme Wet Day**: $\ge 95\text{th percentile}$ of historical station rainy days ($\ge 2.5\text{ mm}$).
- **Heatwave Spell**: $\ge 3$ consecutive extreme hot days.
- **Unusual Precipitation**: $Z \ge 2.0$ standard deviations above multi-year monthly mean (minimum 8 years baseline).
- **Yearly Completeness**: $\ge 95\%$ valid daily observations in a calendar year.

---

## 7. Limitations & Scientific Caveats

1. **Temporal Horizon**: 16 years (2010–2025) of daily observations provides excellent synoptic and inter-annual variability analysis, but is shorter than the 30-year standard climate normal defined by the World Meteorological Organization (WMO).
2. **Correlation vs. Causation**: Significant Pearson correlations (e.g. between temperature and humidity) describe empirical atmospheric co-variation and synoptic monsoon dynamics, not direct causal mechanisms.
3. **Approximation Algorithms**: Extreme thresholds are computed using Spark SQL's `percentile_approx`, which uses an approximate Greenwald-Khanna streaming algorithm. The accuracy parameter is set to $10,000$ to ensure threshold precision within $\pm 0.1^\circ\text{C}$.
4. **Data Imputation**: Filled values in `weather_clean` originate from linear interpolation of short gaps ($\le 3$ calendar days). They represent statistically robust estimates for time-series continuity, not physical sensor measurements.
