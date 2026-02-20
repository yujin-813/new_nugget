#!/usr/bin/env python3
"""Run regression checks for bad-labeled FILE-route questions.

Usage:
  python3 scripts/file_bad_regression.py --limit 100
"""

import argparse
import json
import os
import sqlite3
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
if PROJECT_ROOT not in os.sys.path:
    os.sys.path.insert(0, PROJECT_ROOT)

from db_manager import DB_PATH
from file_engine import file_engine


BAD_FALLBACK_TOKENS = [
    "파일 분석 결과를 확인해주세요.",
    "파일을 찾을 수 없습니다.",
    "파일 분석 오류",
]


def load_bad_file_cases(limit: int):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        """
        SELECT il.id, il.question, cc.file_path
        FROM interaction_logs il
        LEFT JOIN conversation_context cc
          ON cc.conversation_id = il.conversation_id
        WHERE il.feedback_label = 'bad'
          AND il.route = 'file'
          AND il.question IS NOT NULL
          AND TRIM(il.question) != ''
        ORDER BY il.id DESC
        LIMIT ?
        """,
        (int(limit),),
    )
    rows = c.fetchall() or []
    conn.close()
    return rows


def evaluate_case(case_id: int, question: str, file_path: str):
    if not file_path or not os.path.exists(file_path):
        return {
            "id": case_id,
            "question": question,
            "file_path": file_path,
            "passed": False,
            "reason": "file_not_found",
            "message": "",
        }

    res = file_engine.process(question, file_path, conversation_id=None, prev_source=None)
    msg = str(res.get("message", ""))
    intent_plan = res.get("intent_plan") or {}

    failed = any(tok in msg for tok in BAD_FALLBACK_TOKENS)
    if not intent_plan:
        failed = True
    if not msg.strip():
        failed = True

    return {
        "id": case_id,
        "question": question,
        "file_path": file_path,
        "passed": not failed,
        "reason": "" if not failed else "fallback_or_empty",
        "message": msg,
        "intent_plan": intent_plan,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=100)
    args = ap.parse_args()

    cases = load_bad_file_cases(args.limit)
    results = [evaluate_case(int(cid), str(q), str(fp) if fp else "") for cid, q, fp in cases]

    total = len(results)
    passed = sum(1 for r in results if r["passed"])
    failed = total - passed
    pass_rate = (passed / total * 100.0) if total else 0.0

    report_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "reports")
    os.makedirs(report_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = os.path.join(report_dir, f"file_bad_regression_{ts}.json")
    md_path = os.path.join(report_dir, f"file_bad_regression_{ts}.md")

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "generated_at": ts,
                "total": total,
                "passed": passed,
                "failed": failed,
                "pass_rate": round(pass_rate, 2),
                "results": results,
            },
            f,
            ensure_ascii=False,
            indent=2,
        )

    lines = [
        "# File Bad Regression Report",
        "",
        f"- generated_at: {ts}",
        f"- total: {total}",
        f"- passed: {passed}",
        f"- failed: {failed}",
        f"- pass_rate: {pass_rate:.2f}%",
        "",
        "## Failed Cases",
        "",
    ]
    fail_rows = [r for r in results if not r["passed"]]
    if not fail_rows:
        lines.append("None")
    else:
        for r in fail_rows[:100]:
            lines.append(f"- id={r['id']} | q={r['question']} | reason={r['reason']}")
            lines.append(f"  - file={r['file_path']}")
            lines.append(f"  - message={r['message'][:180]}")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"total={total} passed={passed} failed={failed} pass_rate={pass_rate:.2f}%")
    print(f"json={json_path}")
    print(f"md={md_path}")


if __name__ == "__main__":
    main()
