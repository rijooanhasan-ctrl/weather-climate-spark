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
│   └── settings.py         Project-wide constants and paths
├── data/
│   ├── raw/                Drop CSV source files here (not committed)
│   └── processed/          Spark-processed outputs (not committed)
├── src/
│   ├── spark_session.py    SparkSession factory
│   └── queries/            Spark SQL query modules (Phase 2+)
├── tests/
│   └── test_environment.py Pytest environment checks
├── scripts/
│   └── verify_environment.py  One-shot environment verifier
├── notebooks/              Exploratory Jupyter notebooks
└── docs/screenshots/       Dashboard screenshots for the report
```

---

## Phases

| Phase | Description | Status |
|---|---|---|
| 1 | Environment setup & project skeleton | ✅ Complete |
| 2 | Data ingestion & schema definition | 🔜 Planned |
| 3 | Spark SQL analysis & pattern discovery | 🔜 Planned |
| 4 | Streamlit dashboard | 🔜 Planned |

---

## License

MIT – see `LICENSE` for details.

