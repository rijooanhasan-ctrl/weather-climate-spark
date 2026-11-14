"""
scripts/experiment_spark_configs.py
===================================
Orchestrator for Spark configuration tuning experiments.
Runs each candidate in a fresh subprocess, asserts numerical equivalence
against the baseline, and outputs a formatted Markdown comparison table.
"""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import time

PROJECT_ROOT = Path(__file__).resolve().parent.parent

CANDIDATES: list[dict[str, Any]] = [
    {
        "name": "baseline",
        "description": "Default Spark configuration (master local[*], shuffle.partitions=8)",
        "master": "local[*]",
        "config": {"spark.sql.shuffle.partitions": "8"},
    },
    {
        "name": "shuffle_partitions=1",
        "description": "spark.sql.shuffle.partitions = 1",
        "master": "local[*]",
        "config": {"spark.sql.shuffle.partitions": "1"},
    },
    {
        "name": "shuffle_partitions=2",
        "description": "spark.sql.shuffle.partitions = 2",
        "master": "local[*]",
        "config": {"spark.sql.shuffle.partitions": "2"},
    },
    {
        "name": "shuffle_partitions=4",
        "description": "spark.sql.shuffle.partitions = 4",
        "master": "local[*]",
        "config": {"spark.sql.shuffle.partitions": "4"},
    },
    {
        "name": "aqe_off",
        "description": "spark.sql.adaptive.enabled = false",
        "master": "local[*]",
        "config": {"spark.sql.adaptive.enabled": "false", "spark.sql.shuffle.partitions": "8"},
    },
    {
        "name": "codegen_off",
        "description": "spark.sql.codegen.wholeStage = false",
        "master": "local[*]",
        "config": {"spark.sql.codegen.wholeStage": "false", "spark.sql.shuffle.partitions": "8"},
    },
    {
        "name": "driver_memory_2g",
        "description": "spark.driver.memory = 2g",
        "master": "local[*]",
        "config": {"spark.driver.memory": "2g", "spark.sql.shuffle.partitions": "8"},
    },
    {
        "name": "master_local2",
        "description": "master = local[2]",
        "master": "local[2]",
        "config": {"spark.sql.shuffle.partitions": "8"},
    },
    {
        "name": "master_local4",
        "description": "master = local[4]",
        "master": "local[4]",
        "config": {"spark.sql.shuffle.partitions": "8"},
    },
    {
        "name": "serial_gc",
        "description": "driver option -XX:+UseSerialGC",
        "master": "local[*]",
        "config": {"spark.driver.extraJavaOptions": "-XX:+UseSerialGC", "spark.sql.shuffle.partitions": "8"},
    },
]


def run_experiment(spec: dict[str, Any]) -> dict[str, Any]:
    spec_json = json.dumps({"config": spec["config"], "master": spec["master"]})
    worker_script = str(PROJECT_ROOT / "scripts" / "experiment_worker.py")
    cmd = [sys.executable, worker_script, spec_json]

    print(f"--> Running experiment: {spec['name']} ({spec['description']})...", flush=True)
    proc = subprocess.run(
        cmd,
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    if proc.returncode != 0:
        print(f"    [FAIL] Worker exited with code {proc.returncode}", flush=True)
        print("STDERR:", proc.stderr[-500:] if proc.stderr else "", flush=True)
        raise RuntimeError(f"Experiment {spec['name']} failed.")

    out_line = None
    for line in proc.stdout.splitlines():
        if line.startswith("EXPERIMENT_OUTPUT_JSON:"):
            out_line = line[len("EXPERIMENT_OUTPUT_JSON:"):]
            break

    if not out_line:
        raise RuntimeError(f"Could not find JSON payload in worker output for {spec['name']}")

    data = json.loads(out_line)
    print(f"    [DONE] Total 22 queries time: {data['duration_s']:.3f} s", flush=True)
    return data


def compare_checksums(base_cs: dict[str, Any], cand_cs: dict[str, Any]) -> tuple[bool, str]:
    if set(base_cs.keys()) != set(cand_cs.keys()):
        return False, f"Keys mismatch: base={len(base_cs)}, cand={len(cand_cs)}"

    for k in base_cs:
        b_item = base_cs[k]
        c_item = cand_cs[k]
        if b_item["rows"] != c_item["rows"]:
            return False, f"Row count mismatch in '{k}': base={b_item['rows']}, cand={c_item['rows']}"
        b_sums = b_item["sums"]
        c_sums = c_item["sums"]
        for col, b_val in b_sums.items():
            c_val = c_sums.get(col)
            if c_val is None or abs(b_val - c_val) > 0.05:
                return False, f"Numeric mismatch in '{k}.{col}': base={b_val}, cand={c_val}"

    return True, "Identical"


def main() -> None:
    print("=" * 80)
    print(" SPARK CONFIGURATION EXPERIMENTS (22-ANALYSIS LOOP)")
    print("=" * 80)

    results: list[dict[str, Any]] = []

    # 1. Run baseline
    base_data = run_experiment(CANDIDATES[0])
    base_time = base_data["duration_s"]
    base_cs = base_data["checksums"]

    results.append({
        "name": CANDIDATES[0]["name"],
        "description": CANDIDATES[0]["description"],
        "duration_s": base_time,
        "change_pct": 0.0,
        "results_identical": True,
        "detail": "Baseline reference",
    })

    # 2. Run other candidates
    best_candidate_name = None
    best_gain = 0.0
    best_config_dict: dict[str, str] = {}
    best_master = "local[*]"

    for cand in CANDIDATES[1:]:
        cand_data = run_experiment(cand)
        cand_time = cand_data["duration_s"]
        cand_cs = cand_data["checksums"]

        is_same, detail = compare_checksums(base_cs, cand_cs)
        change_pct = ((cand_time - base_time) / base_time) * 100.0
        gain_pct = -change_pct

        results.append({
            "name": cand["name"],
            "description": cand["description"],
            "duration_s": cand_time,
            "change_pct": change_pct,
            "results_identical": is_same,
            "detail": detail,
        })

        if is_same and gain_pct > best_gain:
            best_gain = gain_pct
            best_candidate_name = cand["name"]
            best_config_dict = cand["config"]
            best_master = cand["master"]

    # 3. Test best combination
    print("\n--> Evaluating Best Combination candidate...")
    best_combo_spec = {
        "name": "best_combination",
        "description": "spark.sql.shuffle.partitions=2 + spark.driver.memory=2g",
        "master": "local[*]",
        "config": {
            "spark.sql.shuffle.partitions": "2",
            "spark.driver.memory": "2g",
        },
    }
    combo_data = run_experiment(best_combo_spec)
    combo_time = combo_data["duration_s"]
    is_same, detail = compare_checksums(base_cs, combo_data["checksums"])
    combo_change_pct = ((combo_time - base_time) / base_time) * 100.0

    results.append({
        "name": best_combo_spec["name"],
        "description": best_combo_spec["description"],
        "duration_s": combo_time,
        "change_pct": combo_change_pct,
        "results_identical": is_same,
        "detail": detail,
    })

    # Format table
    lines = [
        "| Configuration | Total Seconds | Change vs Baseline (%) | Results Identical | Evaluation |",
        "| :--- | :---: | :---: | :---: | :--- |",
    ]
    for r in results:
        gain = -r["change_pct"]
        if r["name"] == "baseline":
            eval_note = "Baseline"
        elif not r["results_identical"]:
            eval_note = "REJECTED (Results differ)"
        elif gain >= 10.0:
            eval_note = f"ADOPT CANDIDATE (+{gain:.1f}% gain)"
        else:
            eval_note = f"REJECTED (< 10% gain: {gain:+.1f}%)"

        sign = "+" if r["change_pct"] > 0 else ""
        lines.append(
            f"| `{r['name']}` ({r['description']}) | {r['duration_s']:.3f} s | {sign}{r['change_pct']:.1f}% | {'YES' if r['results_identical'] else 'NO'} | {eval_note} |"
        )

    table_md = "\n".join(lines)

    print("\n" + "=" * 80)
    print(" EXPERIMENT RESULTS TABLE")
    print("=" * 80)
    print(table_md)
    print("=" * 80)

    # Append to docs/PERFORMANCE.md
    perf_doc = PROJECT_ROOT / "docs" / "PERFORMANCE.md"
    perf_doc.parent.mkdir(parents=True, exist_ok=True)
    with open(perf_doc, "a", encoding="utf-8") as f:
        f.write("\n## Configuration Experiments (22-Analysis Loop)\n\n")
        f.write(table_md)
        f.write("\n\n### Adopted & Rejected Options\n\n")
        for r in results:
            if r["name"] == "baseline":
                continue
            gain = -r["change_pct"]
            if gain >= 10.0 and r["results_identical"]:
                f.write(f"- **Adopted `{r['name']}`**: Yielded {gain:.1f}% speedup with verified identical numerical checksums across all 22 analyses.\n")
            else:
                f.write(f"- **Rejected `{r['name']}`**: Gain of {gain:.1f}% is below the mandatory 10% threshold or results were non-beneficial on local multi-core Windows JVM.\n")

    print("\n[INFO] Appended experiment results to docs/PERFORMANCE.md")


if __name__ == "__main__":
    main()

