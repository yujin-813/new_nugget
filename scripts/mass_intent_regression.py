#!/usr/bin/env python3
"""Generate 1000 human-like Korean queries and score intent/slot matching quality.

Usage:
  python3 scripts/mass_intent_regression.py
"""

import os
import sys
from itertools import cycle
from typing import Dict, List, Tuple, Any

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from candidate_extractor import CandidateExtractor
from planner import GA4Planner


def _first_block(question: str, last_state: Dict[str, Any] = None):
    ex = CandidateExtractor()
    pl = GA4Planner()
    ext = ex.extract(question=question, last_state=last_state, date_context=None, semantic=None)
    plan = pl.build_plan(
        property_id="mass-regression",
        question=question,
        intent=ext["intent"],
        metric_candidates=ext["metric_candidates"],
        dimension_candidates=ext["dimension_candidates"],
        date_range=ext["date_range"],
        modifiers=ext["modifiers"],
        last_state=last_state,
    )
    if not plan.blocks:
        return ext, None
    return ext, plan.blocks[0]


def _ok_contains(values: List[str], expected: List[str]) -> bool:
    value_set = set(values)
    return all(e in value_set for e in expected)


def _validate_total_revenue(ext, block):
    if not block:
        return False, "no_block"
    metrics = [m["name"] for m in block.metrics]
    return ("purchaseRevenue" in metrics), f"metrics={metrics}"


def _validate_type_followup(ext, block):
    if not block:
        return False, "no_block"
    dims = [d["name"] for d in block.dimensions]
    return ("customEvent:is_regular_donation" in dims), f"dims={dims}"


def _validate_first_buyer_rate(ext, block):
    if not block:
        return False, "no_block"
    metrics = [m["name"] for m in block.metrics]
    return ("firstTimePurchaserRate" in metrics), f"metrics={metrics}"


def _validate_channel_purchasers(ext, block):
    if not block:
        return False, "no_block"
    metrics = [m["name"] for m in block.metrics]
    dims = [d["name"] for d in block.dimensions]
    ok = ("totalPurchasers" in metrics) and ("defaultChannelGroup" in dims)
    return ok, f"metrics={metrics},dims={dims}"


def _validate_donation_click_count(ext, block):
    if not block:
        return False, "no_block"
    metrics = [m["name"] for m in block.metrics]
    event_filter = (block.filters or {}).get("event_filter")
    ok = ("eventCount" in metrics) and (event_filter == "donation_click")
    return ok, f"metrics={metrics},event_filter={event_filter}"


def _validate_donation_name_breakdown(ext, block):
    if not block:
        return False, "no_block"
    dims = [d["name"] for d in block.dimensions]
    metrics = [m["name"] for m in block.metrics]
    ok = ("customEvent:donation_name" in dims) and ("eventCount" in metrics or "purchaseRevenue" in metrics)
    return ok, f"metrics={metrics},dims={dims}"


def _validate_menu_name(ext, block):
    if not block:
        return False, "no_block"
    dims = [d["name"] for d in block.dimensions]
    metrics = [m["name"] for m in block.metrics]
    ok = ("customEvent:menu_name" in dims) and ("eventCount" in metrics)
    return ok, f"metrics={metrics},dims={dims}"


def _validate_source_medium(ext, block):
    if not block:
        return False, "no_block"
    dims = [d["name"] for d in block.dimensions]
    return ("sourceMedium" in dims), f"dims={dims}"


def _validate_item_category(ext, block):
    if not block:
        return False, "no_block"
    dims = [d["name"] for d in block.dimensions]
    return ("itemCategory" in dims), f"dims={dims}"


def _build_questions(target_count: int = 1000) -> List[Tuple[str, str, Dict[str, Any]]]:
    # (question, scenario_key, last_state)
    out: List[Tuple[str, str, Dict[str, Any]]] = []

    politeness = ["", " 알려줘", " 부탁해", " 확인해줘", " 볼 수 있어?"]
    type_words = ["유형", "타입", "종류"]
    channel_words = ["채널", "유입 채널", "채널 그룹"]
    click_words = ["클릭", "눌림", "탭"]

    # 1. 총 매출
    for p in politeness:
        out.append((f"매출은 어때?{p}", "total_revenue", None))
        out.append((f"총 매출 얼마야?{p}", "total_revenue", None))

    # 2. 유형 follow-up (직전 매출 문맥)
    for t in type_words:
        for p in politeness:
            out.append((
                f"어떤 {t}에서 많이 일어났어?{p}",
                "type_followup",
                {"metrics": [{"name": "purchaseRevenue"}], "dimensions": [], "start_date": "2026-02-09", "end_date": "2026-02-15", "intent": "metric_single"},
            ))

    # 3. 첫 후원자 비율
    for p in politeness:
        out.append((f"첫 후원자는 전체 후원자의 몇퍼센트야?{p}", "first_buyer_rate", None))
        out.append((f"최초 구매자 비율은?{p}", "first_buyer_rate", None))
        out.append((f"신규 후원자 비율 알려줘{p}", "first_buyer_rate", None))

    # 4. 채널별 구매자수
    for c in channel_words:
        for p in politeness:
            out.append((f"{c}별 구매자수는 어떻게 달라?{p}", "channel_purchasers", None))
            out.append((f"매출 일으킨 사용자는 어느 {c}이 많아?{p}", "channel_purchasers", None))

    # 5. donation_click 건수
    for cw in click_words:
        for p in politeness:
            out.append((f"지난주 donation_click 이벤트 {cw}수 얼마나 돼?{p}", "donation_click_count", None))
            out.append((f"후원 클릭은 얼마나 일어났어?{p}", "donation_click_count", None))

    # 6. donation_name breakdown
    for p in politeness:
        out.append((f"donation_click의 donation_name 전체 보여줘{p}", "donation_name_breakdown", None))
        out.append((f"후원명으로 묶어서 보여줘{p}", "donation_name_breakdown", {"metrics": [{"name": "eventCount"}], "dimensions": [{"name": "customEvent:donation_name"}], "start_date": "2026-02-09", "end_date": "2026-02-15", "intent": "breakdown"}))

    # 7. menu_name
    for p in politeness:
        out.append((f"gnb에서 가장 많이 클릭한 메뉴명 뭐야?{p}", "menu_name", None))
        out.append((f"gnb_click menu_name 보여줘{p}", "menu_name", None))

    # 8. source/medium
    for p in politeness:
        out.append((f"display의 소스 매체는 어떻게 돼?{p}", "source_medium", None))
        out.append((f"광고 소스/매체별 사용자 알려줘{p}", "source_medium", None))

    # 9. item category
    for p in politeness:
        out.append((f"상품 카테고리별 매출 확인해줘{p}", "item_category", None))
        out.append((f"국내아동결연 같은 상품유형은 어떻게 돼?{p}", "item_category", None))

    # Repeat and lightly mutate until 1000
    base = list(out)
    fillers = cycle(["", " 지금", " 오늘", " 기준", " 빠르게", " 정확히"])
    i = 0
    while len(out) < target_count:
        q, key, st = base[i % len(base)]
        suffix = next(fillers)
        out.append((f"{q}{suffix}", key, st))
        i += 1

    return out[:target_count]


def run():
    validators = {
        "total_revenue": _validate_total_revenue,
        "type_followup": _validate_type_followup,
        "first_buyer_rate": _validate_first_buyer_rate,
        "channel_purchasers": _validate_channel_purchasers,
        "donation_click_count": _validate_donation_click_count,
        "donation_name_breakdown": _validate_donation_name_breakdown,
        "menu_name": _validate_menu_name,
        "source_medium": _validate_source_medium,
        "item_category": _validate_item_category,
    }

    questions = _build_questions(1000)
    failures = []
    per_key = {k: {"ok": 0, "fail": 0} for k in validators.keys()}

    for idx, (q, key, last_state) in enumerate(questions, 1):
        ext, block = _first_block(q, last_state=last_state)
        ok, reason = validators[key](ext, block)
        if ok:
            per_key[key]["ok"] += 1
        else:
            per_key[key]["fail"] += 1
            failures.append({
                "index": idx,
                "key": key,
                "question": q,
                "reason": reason,
                "intent": ext.get("intent"),
                "top_metric": (ext.get("metric_candidates") or [{}])[0].get("name") if ext.get("metric_candidates") else None,
                "top_dim": (ext.get("dimension_candidates") or [{}])[0].get("name") if ext.get("dimension_candidates") else None,
            })

    total = len(questions)
    failed = len(failures)
    passed = total - failed
    pass_rate = (passed / total * 100.0) if total else 0.0

    print(f"TOTAL: {total}")
    print(f"PASSED: {passed}")
    print(f"FAILED: {failed}")
    print(f"PASS_RATE: {pass_rate:.2f}%")
    print("")
    print("BY_SCENARIO:")
    for k in sorted(per_key.keys()):
        s = per_key[k]
        total_k = s["ok"] + s["fail"]
        rate_k = (s["ok"] / total_k * 100.0) if total_k else 0.0
        print(f"- {k}: {s['ok']}/{total_k} ({rate_k:.1f}%)")

    if failures:
        print("")
        print("SAMPLE_FAILURES:")
        for f in failures[:30]:
            print(f"[{f['index']}] {f['key']} :: {f['question']}")
            print(f"  reason={f['reason']} intent={f['intent']} top_metric={f['top_metric']} top_dim={f['top_dim']}")

        raise SystemExit(1)


if __name__ == "__main__":
    run()

