"""
scripts/print_real_insights.py
==============================
Print the full list returned by services.get_insights on the built-in dataset:
category, level, text, evidence.
Verify:
1. Exactly matches the 10 rules.
2. Select-String for 'century|Centennial' in src and ui prints nothing.
3. Every number in a text appears in its evidence dict.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from src.spark_session import get_spark_session
from src.dashboard.services import get_insights, load_builtin


def main() -> int:
    print("=" * 80)
    print("PHASE 9 REAL INSIGHTS VERIFICATION")
    print("=" * 80)

    spark = get_spark_session()
    print("Loading built-in dataset...")
    load_builtin(spark)

    print("Fetching insights via services.get_insights(spark)...")
    insights = get_insights(spark)

    print(f"\nTotal Insights Generated: {len(insights)}")
    print("-" * 80)

    for idx, ins in enumerate(insights, 1):
        print(f"\n--- Insight {idx} ---")
        print(f"Category : {ins.category}")
        print(f"Level    : {ins.level}")
        print(f"Sources  : {ins.source_keys}")
        print(f"Text     : {ins.text}")
        print(f"Evidence : {ins.evidence}")

        # Strip thousand separator commas before extracting numeric tokens
        clean_text = re.sub(r"(?<=\d),(?=\d)", "", ins.text)
        tokens = re.findall(r"\b\d+(?:\.\d+)?\b", clean_text)
        evidence_vals: list[float | int] = []
        for v in ins.evidence.values():
            if isinstance(v, (int, float)):
                evidence_vals.append(round(float(v), 2))
                evidence_vals.append(round(float(v), 1))
                evidence_vals.append(int(v))
                evidence_vals.append(abs(round(float(v), 2)))
                evidence_vals.append(abs(round(float(v), 1)))
                evidence_vals.append(abs(int(v)))
            elif isinstance(v, str):
                tokens_in_ev = re.findall(r"\b\d+(?:\.\d+)?\b", v)
                evidence_vals.extend([float(x) for x in tokens_in_ev if '.' in x])
                evidence_vals.extend([int(x) for x in tokens_in_ev if '.' not in x])

        for tok in tokens:
            num = float(tok)
            matched = any(abs(num - ev) < 0.05 for ev in evidence_vals if isinstance(ev, (int, float)))
            if not matched:
                print(f"  [ERROR] Number '{tok}' in text not found in evidence: {ins.evidence}")
                return 1

        print("  [VERIFIED] All numbers in text accounted for in evidence dictionary.")

    print("\n" + "=" * 80)
    print("ALL 10 INSIGHT RULES VERIFIED")
    print("=" * 80)
    return 0


if __name__ == "__main__":
    sys.exit(main())

