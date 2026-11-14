"""
scripts/retest_codegen_config.py
================================
Config re-test 3x loops (Phase 9 A5):
Run 22-analysis loop 3 times each in fresh subprocesses for:
1. Baseline: default Spark configuration
2. codegen_off: spark.sql.codegen.wholeStage=false

Compute and print all 6 totals and medians.
Verify numerical equivalence across all runs.
Check if median gain >= 10%.
Re-display Phase 8 A2 experiment table.
"""

from __future__ import annotations

import json
import statistics
import subprocess
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent

CANDIDATES = {
    "baseline": {
        "name": "baseline",
        "description": "Default Spark configuration (master local[*], shuffle.partitions=8)",
        "master": "local[*]",
        "config": {"spark.sql.shuffle.partitions": "8"},
    },
    "codegen_off": {
        "name": "codegen_off",
        "description": "spark.sql.codegen.wholeStage = false",
        "master": "local[*]",
        "config": {"spark.sql.codegen.wholeStage": "false", "spark.sql.shuffle.partitions": "8"},
    },
}


def run_single_experiment(spec: dict[str, Any], run_idx: int) -> dict[str, Any]:
    spec_json = json.dumps({"config": spec["config"], "master": spec["master"]})
    worker_script = str(PROJECT_ROOT / "scripts" / "experiment_worker.py")
    cmd = [sys.executable, worker_script, spec_json]

    print(f"  [Run {run_idx}/3] Launching fresh subprocess for {spec['name']}...", flush=True)
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
        if proc.stderr:
            print("STDERR tail:\n", "\n".join(proc.stderr.splitlines()[-10:]), flush=True)
        raise RuntimeError(f"Experiment {spec['name']} run {run_idx} failed.")

    out_line = None
    for line in proc.stdout.splitlines():
        if line.startswith("EXPERIMENT_OUTPUT_JSON:"):
            out_line = line[len("EXPERIMENT_OUTPUT_JSON:"):]
            break

    if not out_line:
        raise RuntimeError(f"Could not find JSON payload in worker output for {spec['name']} run {run_idx}")

    data = json.loads(out_line)
    print(f"    -> Finished in {data['duration_s']:.3f} s", flush=True)
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


def main() -> int:
    print("=" * 80)
    print("PHASE 9 A5: 3x LOOP CONFIGURATION RE-TEST (BASELINE vs CODEGEN_OFF)")
    print("=" * 80)

    # 1. Run baseline 3 times
    print("\n--> 1. Executing 3 Subprocess Runs of BASELINE:")
    base_runs: list[dict[str, Any]] = []
    for r in range(1, 4):
        base_runs.append(run_single_experiment(CANDIDATES["baseline"], r))

    # 2. Run codegen_off 3 times
    print("\n--> 2. Executing 3 Subprocess Runs of CODEGEN_OFF (wholeStage=false):")
    codegen_runs: list[dict[str, Any]] = []
    for r in range(1, 4):
        codegen_runs.append(run_single_experiment(CANDIDATES["codegen_off"], r))

    # 3. Checksums verification
    print("\n--> 3. Numerical Equivalence Verification vs Baseline:")
    ref_cs = base_runs[0]["checksums"]
    for i, b_res in enumerate(base_runs[1:], start=2):
        identical, msg = compare_checksums(ref_cs, b_res["checksums"])
        print(f"    Baseline Run 1 vs Run {i}: {msg}")
        assert identical, f"Baseline run {i} differed: {msg}"

    for i, c_res in enumerate(codegen_runs, start=1):
        identical, msg = compare_checksums(ref_cs, c_res["checksums"])
        print(f"    Baseline Run 1 vs Codegen_Off Run {i}: {msg}")
        assert identical, f"Codegen_Off run {i} differed: {msg}"

    # 4. Timings and medians
    base_times = [r["duration_s"] for r in base_runs]
    codegen_times = [r["duration_s"] for r in codegen_runs]

    base_median = statistics.median(base_times)
    codegen_median = statistics.median(codegen_times)

    gain_pct = ((base_median - codegen_median) / base_median) * 100.0

    print("\n--> 4. Six Total Times & Medians:")
    print("-" * 80)
    print(f"{'Configuration':<25} | {'Run 1 (s)':<11} | {'Run 2 (s)':<11} | {'Run 3 (s)':<11} | {'Median (s)'}")
    print("-" * 80)
    print(f"{'Baseline':<25} | {base_times[0]:<11.3f} | {base_times[1]:<11.3f} | {base_times[2]:<11.3f} | {base_median:.3f}")
    print(f"{'codegen_off':<25} | {codegen_times[0]:<11.3f} | {codegen_times[1]:<11.3f} | {codegen_times[2]:<11.3f} | {codegen_median:.3f}")
    print("-" * 80)
    print(f"    Median Gain: {gain_pct:+.2f}%")

    if gain_pct >= 10.0:
        decision = "KEEP CODEGEN OFF (Gain >= 10% verified with identical numerical results)"
    else:
        decision = "REVERT (Gain < 10%, keep default Spark codegen configuration)"
    print(f"    Decision: {decision}")

    # 5. Re-display Phase 8 A2 experiment table
    print("\n--> 5. Re-Display Phase 8 A2 Configuration Experiment Table:")
    print("""
| Configuration | Total Seconds | Change vs Baseline (%) | Results Identical | Evaluation |
| :--- | :---: | :---: | :---: | :--- |
| `baseline` (master local[*], shuffle.partitions=8) | 26.788 s | 0.0% | YES | Baseline |
| `shuffle_partitions=1` | 24.804 s | -7.4% | NO | REJECTED (Results differ) |
| `shuffle_partitions=2` | 27.970 s | +4.4% | NO | REJECTED (Results differ) |
| `shuffle_partitions=4` | 28.178 s | +5.2% | NO | REJECTED (Results differ) |
| `aqe_off` (spark.sql.adaptive.enabled = false) | 27.257 s | +1.8% | NO | REJECTED (Results differ) |
| `codegen_off` (spark.sql.codegen.wholeStage = false) | 23.757 s | -11.3% | YES | ADOPT CANDIDATE (+11.3% gain) |
| `driver_memory_2g` (spark.driver.memory = 2g) | 26.018 s | -2.9% | YES | REJECTED (< 10% gain) |
| `master_local2` (master = local[2]) | 26.050 s | -2.8% | NO | REJECTED (Results differ) |
| `master_local4` (master = local[4]) | 25.725 s | -4.0% | YES | REJECTED (< 10% gain) |
| `serial_gc` (-XX:+UseSerialGC) | 24.825 s | -7.3% | YES | REJECTED (< 10% gain) |
| `best_combination` (shuffle=2 + mem=2g) | 24.332 s | -9.2% | NO | REJECTED (Results differ) |
""")

    print("=" * 80)
    print("PHASE 9 A5: CONFIG RE-TEST COMPLETE")
    print("=" * 80)
    return 0


if __name__ == "__main__":
    sys.exit(main())

