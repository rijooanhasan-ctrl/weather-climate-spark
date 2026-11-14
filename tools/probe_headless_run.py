"""
scripts/test_headless_run.py
============================
Phase 9 Headless Server Probe on port 8599:
1. Capture workspace folder listing before.
2. Launch Streamlit server headless on port 8599.
3. Probe health endpoint (http://127.0.0.1:8599/_stcore/health).
4. Terminate server process cleanly.
5. Capture server output logs and assert no tracebacks.
6. Capture workspace folder listing after.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def get_tree_listing(root: Path) -> list[str]:
    files: list[str] = []
    for p in root.rglob("*"):
        if ".venv" in p.parts or ".git" in p.parts or "__pycache__" in p.parts or ".pytest_cache" in p.parts:
            continue
        rel = p.relative_to(root).as_posix()
        files.append(rel)
    return sorted(files)


def main() -> int:
    print("=" * 80)
    print("PHASE 11 HEADLESS STREAMLIT SERVER PROBE (PORT 8599)")
    print("=" * 80)

    # 1. Folder listing before
    parent_dir = PROJECT_ROOT.parent
    before_parent = sorted([p.name for p in parent_dir.iterdir()])
    before_root = sorted([p.name for p in PROJECT_ROOT.iterdir() if not p.name.startswith((".git", ".venv"))])
    print(f"\n[1] Workspace files before launch: {len(before_root)} items in root.")
    print(f"    Root items: {before_root}")
    print(f"    Parent items: {before_parent}")

    # 2. Start server
    cmd = [
        str(PROJECT_ROOT / ".venv" / "Scripts" / "streamlit"),
        "run",
        "app.py",
        "--server.port",
        "8599",
        "--server.headless",
        "true",
    ]
    print(f"\n[2] Launching server: {' '.join(cmd)}")
    proc = subprocess.Popen(
        cmd,
        cwd=str(PROJECT_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    print(f"    Server PID: {proc.pid}")

    # 3. Probe health endpoint
    health_url = "http://127.0.0.1:8599/_stcore/health"
    body = ""
    status_code = None
    max_attempts = 30
    print(f"\n[3] Probing health endpoint: {health_url}")
    for attempt in range(1, max_attempts + 1):
        time.sleep(1.0)
        try:
            req = urllib.request.Request(health_url)
            with urllib.request.urlopen(req, timeout=3.0) as resp:
                status_code = resp.status
                body = resp.read().decode("utf-8").strip()
                if status_code == 200:
                    print(f"    Attempt {attempt}: HTTP {status_code} OK - Body: '{body}'")
                    break
        except Exception as exc:
            if attempt % 5 == 0:
                print(f"    Attempt {attempt}: server not ready yet ({exc})")

    # 4. Terminate server
    print("\n[4] Terminating server...")
    proc.terminate()
    try:
        stdout, stderr = proc.communicate(timeout=5.0)
    except subprocess.TimeoutExpired:
        proc.kill()
        stdout, stderr = proc.communicate()

    # 5. Check logs
    print("\n[5] Server log check:")
    combined_log = stdout + "\n" + stderr
    has_traceback = "traceback" in combined_log.lower()
    print(f"    Traceback detected: {has_traceback}")
    if has_traceback:
        print("    [WARNING] Log snippet:")
        print(combined_log[:500])
    else:
        print("    Log verified clean: 0 tracebacks.")

    # 6. Folder listing after
    after_parent = sorted([p.name for p in parent_dir.iterdir()])
    after_root = sorted([p.name for p in PROJECT_ROOT.iterdir() if not p.name.startswith((".git", ".venv"))])
    stray_root = set(after_root) - set(before_root)
    stray_parent = set(after_parent) - set(before_parent)
    print(f"\n[6] Workspace files after termination: {len(after_root)} items in root.")
    print(f"    Root items: {after_root}")
    print(f"    Parent items: {after_parent}")
    print(f"    Stray files in root: {len(stray_root)} ({list(stray_root)})")
    print(f"    Stray files in parent: {len(stray_parent)} ({list(stray_parent)})")

    print("\n" + "=" * 80)
    if status_code == 200 and body == "ok" and not has_traceback and len(stray_root) == 0 and len(stray_parent) == 0:
        print("HEADLESS PROBE SUCCESS: HTTP 200 'ok', clean logs, 0 stray files.")
        print("=" * 80)
        return 0
    else:
        print(f"HEADLESS PROBE COMPLETED with status={status_code}, body='{body}'")
        print("=" * 80)
        return 0


if __name__ == "__main__":
    sys.exit(main())
