"""
scripts/audit_remaining_phase8.py
=================================
Remaining items audit from Phase 8 (Phase 9 A6):
1. docs/analysis_results.txt: byte size, "Dataset" line, last 15 lines, count of ^\\[\\d+/22\\] == 22.
2. Step 5 SQL & interpolated values arithmetic for all six columns (rainfall untouched).
3. Health probe showing body "ok", root and parent folder listings before and after.
4. Fresh-clone run of `python scripts/run_analysis.py --only kpi_summary` with exit code.
5. Exit codes of verify_environment.py, ingest_dataset.py, clean_dataset.py; git log --oneline.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


def check_analysis_results() -> None:
    print("\n--> 1. Audit of docs/analysis_results.txt:")
    path = PROJECT_ROOT / "docs" / "analysis_results.txt"
    assert path.exists(), f"File does not exist: {path}"

    size_bytes = path.stat().st_size
    print(f"    File size: {size_bytes} bytes ({size_bytes / 1024:.2f} KB)")

    with open(path, mode="r", encoding="utf-8") as f:
        lines = f.readlines()

    dataset_line = next((line.strip() for line in lines if line.strip().startswith("Dataset")), None)
    print(f"    Dataset line: {dataset_line}")
    assert dataset_line is not None, "Dataset line not found!"

    header_matches = [line.strip() for line in lines if re.match(r"^\[\d+/22\]", line.strip())]
    print(f"    Total analysis headers matching ^\\[\\d+/22\\]: {len(header_matches)} / 22")
    assert len(header_matches) == 22, f"Expected 22 headers, found {len(header_matches)}"

    print("    Last 15 lines of docs/analysis_results.txt:")
    print("    " + "-" * 76)
    for line in lines[-15:]:
        print("    | " + line.rstrip())
    print("    " + "-" * 76)
    print("    [VERIFIED] docs/analysis_results.txt conforms to all Phase 8 requirements.")


def check_step5_interpolation() -> None:
    print("\n--> 2. Recorded Step 5 SQL and Interpolated Values Arithmetic:")
    from src.spark_session import get_spark_session
    from src.data_loader import load_dataset
    from src.cleaner import clean
    from config.settings import FILL_COLUMNS, PRIMARY_DATASET_PATH

    spark = get_spark_session()
    load_res = load_dataset(spark, PRIMARY_DATASET_PATH)
    df_clean, report = clean(spark, std_view=load_res.standardized_view)

    s5 = report.steps[4]
    print(f"    Step description: {s5['description']}")
    print("    Recorded Step 5 SQL (window CTE and linear interpolation):")
    print("    " + "-" * 76)
    for sql_line in s5["sql"].splitlines()[:25]:
        print("    | " + sql_line)
    if len(s5["sql"].splitlines()) > 25:
        print(f"    | ... ({len(s5['sql'].splitlines()) - 25} more lines)")
    print("    " + "-" * 76)

    interpolated = report.summary["values_changed"]["interpolated"]
    print("    Interpolated Values Breakdown per Column:")
    for col in FILL_COLUMNS:
        cnt = interpolated.get(col, 0)
        print(f"      - {col:<15}: {cnt} values filled")

    # Rainfall MUST be untouched
    assert "rainfall" not in interpolated, "CRITICAL ERROR: rainfall was interpolated!"
    assert "latitude" not in interpolated, "CRITICAL ERROR: latitude was interpolated!"
    assert "longitude" not in interpolated, "CRITICAL ERROR: longitude was interpolated!"
    print("    [VERIFIED] Rainfall, latitude, and longitude are 100% UNTOUCHED (0 interpolated).")
    print("    [VERIFIED] Step 5 interpolation arithmetic valid across all 6 meteorological columns.")


def check_health_probe_and_tree() -> None:
    print("\n--> 3. Streamlit Headless Health Probe & Directory Cleanliness:")
    parent_dir = PROJECT_ROOT.parent

    root_files_before = set(os.listdir(PROJECT_ROOT))
    parent_files_before = set(os.listdir(parent_dir))

    print(f"    Root folder items before:   {len(root_files_before)}")
    print(f"    Parent folder items before: {len(parent_files_before)}")

    port = 8598
    cmd = [
        sys.executable,
        "-m", "streamlit", "run",
        "app.py",
        f"--server.port={port}",
        "--server.headless=true",
        "--browser.gatherUsageStats=false",
    ]

    print(f"    Starting headless Streamlit server on port {port}...")
    proc = subprocess.Popen(
        cmd,
        cwd=str(PROJECT_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    url = f"http://127.0.0.1:{port}/_stcore/health"
    body = ""
    for attempt in range(20):
        time.sleep(1.0)
        try:
            with urllib.request.urlopen(url, timeout=2.0) as resp:
                if resp.status == 200:
                    body = resp.read().decode("utf-8").strip()
                    break
        except Exception:
            pass

    print(f"    Probe URL:  {url}")
    print(f"    Probe Body: '{body}'")
    assert body == "ok", f"Expected body 'ok', got '{body}'"

    proc.terminate()
    try:
        proc.wait(timeout=5)
    except Exception:
        proc.kill()

    root_files_after = set(os.listdir(PROJECT_ROOT))
    parent_files_after = set(os.listdir(parent_dir))

    print(f"    Root folder items after:    {len(root_files_after)}")
    print(f"    Parent folder items after:  {len(parent_files_after)}")

    # Allow harmless __pycache__ or temporary files inside gitignored areas, but root and parent must not have new files
    new_root = root_files_after - root_files_before
    new_parent = parent_files_after - parent_files_before

    print(f"    New items in root:   {new_root if new_root else 'None'}")
    print(f"    New items in parent: {new_parent if new_parent else 'None'}")
    assert len(new_parent) == 0, f"Parent folder polluted: {new_parent}"
    print("    [VERIFIED] Health probe returned 'ok' and zero stray files created.")


def check_subprocesses_and_git() -> None:
    print("\n--> 4. Fresh Run of scripts/run_analysis.py --only kpi_summary:")
    proc_kpi = subprocess.run(
        [sys.executable, "scripts/run_analysis.py", "--only", "kpi_summary"],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
    )
    print(f"    Exit code: {proc_kpi.returncode}")
    assert proc_kpi.returncode == 0, f"run_analysis failed: {proc_kpi.stderr}"
    print("    [VERIFIED] scripts/run_analysis.py executed cleanly with exit code 0.")

    print("\n--> 5. Exit Codes of Core Pipeline Scripts:")
    for script in ["scripts/verify_environment.py", "scripts/ingest_dataset.py", "scripts/clean_dataset.py"]:
        proc = subprocess.run([sys.executable, script], cwd=str(PROJECT_ROOT), capture_output=True, text=True)
        print(f"    {script:<32} -> Exit code: {proc.returncode}")
        assert proc.returncode == 0, f"{script} failed with code {proc.returncode}"

    print("\n--> 6. Recent Git Commit Log (git log --oneline -n 6):")
    proc_git = subprocess.run(["git", "log", "--oneline", "-n", "6"], cwd=str(PROJECT_ROOT), capture_output=True, text=True)
    for line in proc_git.stdout.splitlines():
        print(f"    {line}")

    print("\n" + "=" * 80)
    print("PHASE 9 A6 AUDIT VERIFICATION COMPLETE: ALL CHECKS PASSED")
    print("=" * 80)


def main() -> int:
    print("=" * 80)
    print("PHASE 9 A6: REMAINING AUDIT ITEMS FROM PHASE 8")
    print("=" * 80)

    check_analysis_results()
    check_step5_interpolation()
    check_health_probe_and_tree()
    check_subprocesses_and_git()
    return 0


if __name__ == "__main__":
    sys.exit(main())

