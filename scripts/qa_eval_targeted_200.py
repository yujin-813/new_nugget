#!/usr/bin/env python3
"""Targeted QA regression (200) for donation_name/donation_click alignment."""

import json
import os
import random
import sys
from datetime import datetime
from typing import Any, Dict, List

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from scripts.qa_eval_500 import _run_ga4, _evaluate


def _build_cases() -> List[Dict[str, Any]]:
    cases: List[Dict[str, Any]] = []
    suffixes = ["", " 알려줘", " 부탁해", " 자세히", " 빠르게"]
    stems = [
        "주로 어떤 donation을 클릭했어",
        "donation_click의 donation_name 보여줘",
        "후원 클릭의 후원명 분포는?",
        "donation_click 후원명 TOP10",
        "후원명으로 donation_click 묶어서",
        "클릭 많이 된 donation_name 순위",
        "후원 클릭 이름별 집계",
        "donation_name 기준으로 donation_click",
    ]
    for i in range(200):
        q = f"{stems[i % len(stems)]}{suffixes[(i // len(stems)) % len(suffixes)]}"
        cases.append({
            "question": q,
            "expected_route": "ga4",
            "expected_intent": "breakdown",
            "expected_date_mode": "none",
            "expected_metrics": ["eventCount"],
            "expected_dimensions": ["customEvent:donation_name"],
            "previous_state": None,
        })
    return cases


def run():
    random.seed(7)
    report_dir = os.path.join(PROJECT_ROOT, "reports")
    os.makedirs(report_dir, exist_ok=True)

    rows = []
    for idx, case in enumerate(_build_cases(), 1):
        gr = _run_ga4(case["question"], case["previous_state"])
        result = {
            "actual_route": "ga4",
            "actual_intent": gr.get("intent"),
            "actual_date_range": gr.get("date_range") or {},
            "actual_metrics": gr.get("metrics") or [],
            "actual_dimensions": gr.get("dimensions") or [],
            "system_response_json": {"route": "ga4", "response": gr.get("response") or {}},
        }
        score = _evaluate(case, result)
        rows.append({
            "id": idx,
            "question": case["question"],
            "expected_route": case["expected_route"],
            "previous_state": case["previous_state"],
            "system_response_json": result["system_response_json"],
            "expected_intent": case["expected_intent"],
            "expected_date_mode": case["expected_date_mode"],
            "expected_metrics": case["expected_metrics"],
            "expected_dimensions": case["expected_dimensions"],
            "actual_route": result["actual_route"],
            "actual_intent": result["actual_intent"],
            "actual_date_range": result["actual_date_range"],
            "actual_metrics": result["actual_metrics"],
            "actual_dimensions": result["actual_dimensions"],
            "score": score,
        })

    jsonl_path = os.path.join(report_dir, "qa_eval_targeted_200_results.jsonl")
    with open(jsonl_path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    total = len(rows)
    avg = sum(r["score"]["total"] for r in rows) / total if total else 0.0
    pass9 = sum(1 for r in rows if r["score"]["total"] >= 9)
    pass7 = sum(1 for r in rows if r["score"]["total"] >= 7)
    worst = sorted(rows, key=lambda x: x["score"]["total"])[:20]

    md_path = os.path.join(report_dir, "qa_eval_targeted_200_report.md")
    lines: List[str] = []
    lines.append("# Targeted QA Report (200)")
    lines.append(f"- Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"- Total: {total}")
    lines.append(f"- Avg total: {avg:.2f} / 10")
    lines.append(f"- Pass(>=9): {pass9}/{total} ({(pass9/total*100):.1f}%)")
    lines.append(f"- Pass(>=7): {pass7}/{total} ({(pass7/total*100):.1f}%)")
    lines.append("")
    lines.append("## Worst 20")
    for w in worst:
        lines.append(f"- [{w['id']}] score={w['score']['total']} intent={w['actual_intent']} q={w['question']}")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"RESULTS_JSONL: {jsonl_path}")
    print(f"REPORT_MD: {md_path}")
    print(f"TOTAL: {total}")
    print(f"AVG_TOTAL: {avg:.2f}")
    print(f"PASS9: {pass9}/{total}")
    print(f"PASS7: {pass7}/{total}")


if __name__ == "__main__":
    run()

