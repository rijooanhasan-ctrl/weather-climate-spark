# Dashboard Screenshots Reference Guide

> **Note**: Headless automated browser capture is not configured in the offline environment; **screenshots to be captured manually** using a browser at `http://localhost:8501`.

Please capture and place the following 8 screenshot artifacts in this directory for documentation and evaluation:

| Filename | Section / Workflow | Description & Elements to Include |
| :--- | :--- | :--- |
| `01_overview.png` | **Overview** | Header banner, dataset status strip, 10 warm-themed KPI cards, and Key Findings card. |
| `02_temperature_chart.png` | **Temperature** | Annual mean temperature time series (`temp_yearly`) with trendlines and warm template styling. |
| `03_rainfall_heatmap.png` | **Rainfall** | Monthly rainfall climatology heatmap (`rain_monthly`) showing monsoon seasonality across stations. |
| `04_climate_correlation.png` | **Climate patterns** | Multivariate correlation matrix (`correlations`) with zero-centered diverging warm colorscale. |
| `05_data_quality.png` | **Data quality** | Dataset profiling metrics, completeness percentages, and physical range validation tables. |
| `06_cleaning_report.png` | **Cleaning report** | 6-step cleaning pipeline execution audit table and gap-interpolation metrics. |
| `07_spark_sql.png` | **Spark SQL** | Interactive Spark SQL query catalog, espresso-themed code blocks, and execution statistics. |
| `08_upload_flow.png` | **Upload CSV Flow** | Sidebar CSV file uploader, column mapping selectboxes, and gap-filling checkbox. |

### Visual Checklist:
- [ ] Warm light palette active (page `#F7F3EB`, cards `#FFFFFF`, border `#E5DDD0`).
- [ ] Navigation pill tabs active (selected pill in dark espresso `#1F1815` with bold white text).
- [ ] 10 KPI cards with SVG icons, colored units, and status dots.
- [ ] Espresso code blocks (`#1A1411`) for Spark SQL query inspect windows.

