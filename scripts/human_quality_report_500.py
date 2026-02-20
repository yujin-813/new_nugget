#!/usr/bin/env python3
"""Run 500 human-like QA checks and export a markdown report."""

import os
import re
import sys
from datetime import datetime
from itertools import cycle
from typing import Dict, List, Tuple, Any

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from scripts.mass_response_regression import _run_one


def build_500_questions() -> List[Tuple[str, str]]:
    items: List[Tuple[str, str]] = []
    p = ["", " 알려줘", " 부탁해", " 확인해줘", " 자세히"]

    # 1) trend daily date required
    for s in p:
        items.append((f"지난주 사용자 추이 알려줘{s}", "trend_daily"))
        items.append((f"지난주 활성 사용자 일별 추이 보여줘{s}", "trend_daily"))

    # 2) week vs prior week
    for s in p:
        items.append((f"지난주와 그 전주 사용자는 어때?{s}", "week_compare_users"))
        items.append((f"지난주 대비 전주 사용자 비교해줘{s}", "week_compare_users"))

    # 3) total + item revenue
    for s in p:
        items.append((f"총 매출과 상품별 매출은?{s}", "total_and_item_revenue"))

    # 4) item type + reason share
    for s in p:
        items.append((f"상품별 매출의 상품 유형은 어때?{s}", "item_type"))
        items.append((f"왜 국내사업후원이 가장 많지? 몇프로야?{s}", "reason_share"))

    # 5) source of purchasers
    for s in p:
        items.append((f"어느 소스에서 온 사용자들이야? 구매한 사람들이?{s}", "source_purchasers"))
        items.append((f"구매한 사용자들의 소스야?{s}", "source_purchasers"))

    # 6) inbound path + not set exclude
    for s in p:
        items.append((f"유입경로{s}", "inbound_path"))
        items.append((f"유입경로 not set제외하고 알려줘{s}", "inbound_path_exclude_notset"))

    # 7) donation click count + donation_name
    for s in p:
        items.append((f"이벤트 중 donation click은 얼마나 일어났어?{s}", "donation_click_count"))
        items.append((f"주로 어떤 donation을 클릭했어?{s}", "donation_name_click"))

    # 8) conversion rate
    for s in p:
        items.append((f"전환율은 얼마나 되?{s}", "conversion_rate"))

    # 9) gnb menu
    for s in p:
        items.append((f"gnb 중 어떤 메뉴를 가장 많이 클릭했어?{s}", "gnb_menu"))

    # 10) monthly insight
    for s in p:
        items.append((f"데이터중 지난달 중요한 것들은 무엇이야?{s}", "monthly_insight"))

    base = list(items)
    suffixes = cycle(["", " 지금", " 오늘", " 기준", " 빠르게"])
    i = 0
    while len(items) < 500:
        q, k = base[i % len(base)]
        items.append((f"{q}{next(suffixes)}", k))
        i += 1
    return items[:500]


def score_case(kind: str, q: str, msg: str) -> Tuple[int, str]:
    m = (msg or "").lower()
    if not m.strip():
        return 0, "empty"
    if "질문 의도는 이해했지만" in m or "매칭 가능한 지표를 찾지 못" in m or "0개 블록 분석 완료" in m:
        return 10, "fallback"

    if kind == "trend_daily":
        has_date = bool(re.search(r"\d{4}-\d{2}-\d{2}", msg))
        has_trend = ("추이" in msg) or ("시점" in msg)
        if has_date and has_trend:
            return 100, "ok"
        if has_trend:
            return 60, "missing_daily_dates"
        return 30, "not_trend"

    if kind == "week_compare_users":
        has_user = ("사용자" in msg or "활성 사용자" in msg)
        has_compare = ("비교" in msg or "대비" in msg or "전주" in msg or "지난주" in msg)
        if has_user and has_compare:
            return 100, "ok"
        if has_user:
            return 60, "missing_compare"
        return 25, "wrong_axis"

    if kind == "total_and_item_revenue":
        ok = ("구매 수익" in msg or "매출" in msg) and ("상품" in msg or "항목 이름" in msg)
        return (100, "ok") if ok else (35, "missing_total_or_item")

    if kind == "item_type":
        ok = ("상품 카테고리" in msg or "유형" in msg or "itemcategory" in m)
        return (100, "ok") if ok else (40, "missing_item_type")

    if kind == "reason_share":
        has_share = ("%" in msg or "비중" in msg)
        has_reason = ("원인" in msg or "집중 구조" in msg or "왜" in q)
        if has_share and has_reason:
            return 100, "ok"
        if has_share:
            return 70, "missing_reason"
        return 30, "missing_share"

    if kind == "source_purchasers":
        has_source = ("소스/매체" in msg or "source" in m)
        buyer_like = ("구매자" in msg or "명" in msg)
        wrong_money = ("원" in msg and "명" not in msg)
        if has_source and buyer_like and not wrong_money:
            return 100, "ok"
        if has_source and wrong_money:
            return 30, "money_instead_of_buyers"
        return 40, "missing_source_or_buyer"

    if kind == "inbound_path":
        return (100, "ok") if ("유입 경로" in msg or "pathname" in m) else (40, "missing_path")

    if kind == "inbound_path_exclude_notset":
        if ("not set" in m) or ("(not set)" in m):
            return 30, "notset_not_excluded"
        return (100, "ok") if ("유입 경로" in msg or "pathname" in m) else (40, "missing_path")

    if kind == "donation_click_count":
        ok = ("이벤트 수" in msg or "회" in msg) and (("donation_click" in m) or ("후원명" in msg))
        return (100, "ok") if ok else (30, "wrong_event_or_metric")

    if kind == "donation_name_click":
        ok = ("후원명" in msg or "donation_name" in m)
        return (100, "ok") if ok else (40, "missing_donation_name_axis")

    if kind == "conversion_rate":
        if "%" in msg and ("전환" in msg or "율" in msg):
            return 100, "ok"
        return 20, "not_rate_answer"

    if kind == "gnb_menu":
        ok = ("메뉴명" in msg or "menu_name" in m)
        return (100, "ok") if ok else (40, "missing_menu_axis")

    if kind == "monthly_insight":
        has_month = ("지난달" in q or "월" in msg or "2026-" in msg)
        has_insight = ("중요" in msg or "요약" in msg or "상위" in msg)
        if has_month and has_insight:
            return 100, "ok"
        if has_month:
            return 60, "weak_insight"
        return 30, "wrong_period"

    return 50, "unknown_case"


def main():
    cases = build_500_questions()
    stats: Dict[str, Dict[str, int]] = {}
    rows: List[Dict[str, Any]] = []

    for i, (q, kind) in enumerate(cases, 1):
        stats.setdefault(kind, {"count": 0, "sum": 0, "pass80": 0})
        res, _ = _run_one(q, None)
        msg = str(res.get("message", ""))
        score, reason = score_case(kind, q, msg)
        stats[kind]["count"] += 1
        stats[kind]["sum"] += score
        if score >= 80:
            stats[kind]["pass80"] += 1
        rows.append({
            "idx": i, "kind": kind, "question": q, "answer": msg, "score": score, "reason": reason
        })

    total = len(rows)
    avg = sum(r["score"] for r in rows) / total if total else 0.0
    pass80 = sum(1 for r in rows if r["score"] >= 80)
    pass60 = sum(1 for r in rows if r["score"] >= 60)

    # worst 20
    worst = sorted(rows, key=lambda x: x["score"])[:20]

    report_dir = os.path.join(PROJECT_ROOT, "reports")
    os.makedirs(report_dir, exist_ok=True)
    out_path = os.path.join(report_dir, "human_quality_report_500.md")

    lines: List[str] = []
    lines.append(f"# Human QA Report (500)\n")
    lines.append(f"- Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"- Total cases: {total}")
    lines.append(f"- Average score: {avg:.1f}/100")
    lines.append(f"- Pass(>=80): {pass80}/{total} ({(pass80/total*100):.1f}%)")
    lines.append(f"- Pass(>=60): {pass60}/{total} ({(pass60/total*100):.1f}%)")
    lines.append("")
    lines.append("## Scenario Summary")
    lines.append("| Scenario | Count | Avg Score | Pass>=80 |")
    lines.append("|---|---:|---:|---:|")
    for kind in sorted(stats.keys()):
        c = stats[kind]["count"]
        s = stats[kind]["sum"]
        p = stats[kind]["pass80"]
        lines.append(f"| {kind} | {c} | {s/c:.1f} | {p}/{c} |")

    lines.append("")
    lines.append("## Worst 20 Samples")
    for w in worst:
        lines.append(f"### [{w['idx']}] {w['kind']} | score={w['score']} | reason={w['reason']}")
        lines.append(f"- Q: {w['question']}")
        lines.append(f"- A: {w['answer'][:500].replace(chr(10), ' / ')}")
        lines.append("")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"REPORT_PATH: {out_path}")
    print(f"TOTAL: {total}")
    print(f"AVG: {avg:.1f}")
    print(f"PASS80: {pass80}/{total}")
    print(f"PASS60: {pass60}/{total}")


if __name__ == "__main__":
    main()
