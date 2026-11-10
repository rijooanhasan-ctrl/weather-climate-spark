# Weather and Climate Pattern Discovery using Spark SQL

A Big Data analytics project that uses **Apache Spark SQL** and **PySpark** as the
primary computation engine to discover weather and climate patterns from large CSV
datasets.  An interactive **Streamlit + Plotly** dashboard surfaces the insights.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Core engine | Apache Spark 3.5 / Spark SQL |
| Python wrapper | PySpark 3.5.3 |
| Supporting data lib | Pandas 2.2 |
| Dashboard | Streamlit 1.40 |
| Visualisation | Plotly 5.24 |
| Language | Python 3.11+ |
| Version control | Git / GitHub |

---

## Prerequisites

1. **Python 3.11 or 3.12** – <https://www.python.org/downloads/>
2. **Java 11 or 17** (required by Spark) –
   - Windows: install [Eclipse Temurin 17](https://adoptium.net/) and set
     `JAVA_HOME` to the JDK root (e.g. `C:\Program Files\Eclipse Adoptium\jdk-17...`).
   - macOS: `brew install --cask temurin@17`
   - Linux: `sudo apt install openjdk-17-jdk`

---

## Setup

```bash
# 1. Clone the repository
git clone <repo-url>
cd weather-climate-spark

# 2. Create a virtual environment
python -m venv .venv

# 3. Activate it
#    Windows PowerShell:
.venv\Scripts\Activate.ps1
#    macOS / Linux:
source .venv/bin/activate

# 4. Install dependencies
pip install -r requirements.txt

# 5. Verify the environment
python scripts/verify_environment.py

# 6. Run tests
pytest -v

# 7. Launch the dashboard (placeholder)
streamlit run app.py
```

---

## Project Structure

```
weather-climate-spark/
├── app.py                  Streamlit dashboard entry point
├── requirements.txt        Pinned Python dependencies
├── README.md               This file
├── .gitignore
├── config/
│   ├── settings.py         Project-wide constants and paths
│   └── schema.py           Canonical Spark SQL StructType schema & aliases
├── data/
│   ├── raw/                Drop CSV source files here (india_weather_daily.csv tracked)
│   └── processed/          Spark-processed outputs (not committed)
├── src/
│   ├── __init__.py
│   ├── spark_session.py    SparkSession factory with auto-JAVA_HOME detection
│   ├── utils.py            Cross-platform Spark path resolution
│   └── queries/            Spark SQL query modules (Phase 3+)
├── tests/
│   ├── conftest.py         Shared session-scoped Spark fixture
│   ├── test_environment.py Pytest environment checks
│   ├── test_schema.py      Schema, column alias, and mapping unit tests
│   └── test_dataset.py     Dataset integrity and plausibility tests
├── scripts/
│   ├── verify_environment.py    One-shot environment verifier
│   ├── diagnose_spark_features.py Diagnostic runner for PySpark capabilities
│   ├── download_weather_data.py  Acquires real ERA5 daily observations
│   └── profile_dataset.py        Spark SQL structural and statistical profiler
├── docs/
│   ├── DATASET.md          Provenance, schema dictionary, and atmospheric notes
│   ├── dataset_profile.txt Generated output from Spark SQL profiling
│   └── screenshots/        Dashboard screenshots for the report
└── notebooks/              Exploratory Jupyter notebooks
```

---

## Dataset

The primary dataset covers **16 years (2010-01-01 to 2025-12-31)** of daily meteorological observations across 8 diverse geographical centers in India: Bengaluru, Mumbai, Delhi, Chennai, Kolkata, Hyderabad, Jaipur, and Shillong (46,752 records total). Data originates from the **Open-Meteo Historical Weather API (ERA5 Reanalysis)** and is licensed under Creative Commons Attribution 4.0 International (CC BY 4.0).

### Download the Dataset
```bash
python scripts/download_weather_data.py
```
Options:
* `--output`: Target CSV path (defaults to `data/raw/india_weather_daily.csv`)
* `--start`: Start date YYYY-MM-DD (default: `2010-01-01`)
* `--end`: End date YYYY-MM-DD (default: `2025-12-31`)
* `--force`: Overwrite existing output file

### Profile the Dataset with Spark SQL
```bash
python scripts/profile_dataset.py
```
This loads the dataset into a Spark SQL temporary view (`weather_raw`) under strict `FAILFAST` mode with `STANDARD_SCHEMA`, validates column order, checks null counts, validates primary key uniqueness (`date`, `location`), checks physical plausibility ranges, and saves the full report to `docs/dataset_profile.txt`.

---

## Project Engineering Rules

Based on rigorous diagnostic testing in the Windows + Python 3.13 + PySpark 3.5.3 environment:

### What Works Reliably (Supported)
* **`spark.sql()`**: All Spark SQL statements (SELECT, GROUP BY, aggregates, window functions, CTEs, CASE statements).
* **`spark.read.csv()`**: Ingestion of CSV files using explicit `StructType` schemas and `to_spark_path()`.
* **`.toPandas()`**: Converting small, aggregated Spark SQL query results into Pandas DataFrames for Streamlit and Plotly visualizations.

### What Fails on this Machine (Prohibited)
* **RDD operations**: `sparkContext.parallelize()`, `rdd.map()`, etc. fail due to Windows loopback socket communication issues (`WinError 10038` / `EOFException`).
* **Python UDFs & pandas_udf**: Any Python worker sub-process execution.
* **`spark.createDataFrame()` from Python lists or Pandas DataFrames**: Spawns Python worker processes that crash.

### Golden Rule of Data Ingestion & Computation
1. Data enters Apache Spark **exclusively** through `spark.read.csv()` with canonical explicit schema.
2. All analytical transformations and business logic **must be written in Spark SQL**.
3. Small, final aggregated metric tables are retrieved via `.toPandas()` for front-end rendering.
4. **Never generate synthetic or mock weather data anywhere in the project.**

---

## Phases

| Phase | Description | Status |
|---|---|---|
| 1 | Environment setup & project skeleton | ✅ Complete |
| 1B | JAVA_HOME auto-detection & diagnostics | ✅ Complete |
| 2 | Dataset acquisition, standard schema & Spark SQL profiling | ✅ Complete |
| 3 | Spark SQL analysis & pattern discovery | 🔜 Planned |
| 4 | Streamlit dashboard & visualizations | 🔜 Planned |

---

## License

MIT – see `LICENSE` for details.

