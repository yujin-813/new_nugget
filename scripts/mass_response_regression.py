#!/usr/bin/env python3
"""Response-quality regression on 1000 generated queries.

Checks not only intent/plan but final user-facing message quality.
"""

import os
import re
import sys
from itertools import cycle
from typing import Any, Dict, List, Tuple

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from candidate_extractor import CandidateExtractor
from planner import GA4Planner
from response_adapter import adapt_pipeline_response_to_legacy


def _build_questions(target_count: int = 1000) -> List[Tuple[str, str, Dict[str, Any]]]:
    out: List[Tuple[str, str, Dict[str, Any]]] = []
    politeness = ["", " 알려줘", " 부탁해", " 확인해줘", " 볼 수 있어?"]
    type_words = ["유형", "타입", "종류"]
    channel_words = ["채널", "유입 채널", "채널 그룹"]
    click_words = ["클릭", "눌림", "탭"]

    for p in politeness:
        out.append((f"매출은 어때?{p}", "total_revenue", None))
        out.append((f"총 매출 얼마야?{p}", "total_revenue", None))

    for t in type_words:
        for p in politeness:
            out.append((
                f"어떤 {t}에서 많이 일어났어?{p}",
                "type_followup",
                {"metrics": [{"name": "purchaseRevenue"}], "dimensions": [], "start_date": "2026-02-09", "end_date": "2026-02-15", "intent": "metric_single"},
            ))

    for p in politeness:
        out.append((f"첫 후원자는 전체 후원자의 몇퍼센트야?{p}", "first_buyer_rate", None))
        out.append((f"최초 구매자 비율은?{p}", "first_buyer_rate", None))
        out.append((f"신규 후원자 비율 알려줘{p}", "first_buyer_rate", None))

    for c in channel_words:
        for p in politeness:
            out.append((f"{c}별 구매자수는 어떻게 달라?{p}", "channel_purchasers", None))
            out.append((f"매출 일으킨 사용자는 어느 {c}이 많아?{p}", "channel_purchasers", None))

    for cw in click_words:
        for p in politeness:
            out.append((f"지난주 donation_click 이벤트 {cw}수 얼마나 돼?{p}", "donation_click_count", None))
            out.append((f"후원 클릭은 얼마나 일어났어?{p}", "donation_click_count", None))

    for p in politeness:
        out.append((f"donation_click의 donation_name 전체 보여줘{p}", "donation_name_breakdown", None))
        out.append((f"후원명으로 묶어서 보여줘{p}", "donation_name_breakdown", {"metrics": [{"name": "eventCount"}], "dimensions": [{"name": "customEvent:donation_name"}], "start_date": "2026-02-09", "end_date": "2026-02-15", "intent": "breakdown"}))

    for p in politeness:
        out.append((f"gnb에서 가장 많이 클릭한 메뉴명 뭐야?{p}", "menu_name", None))
        out.append((f"gnb_click menu_name 보여줘{p}", "menu_name", None))

    for p in politeness:
        out.append((f"display의 소스 매체는 어떻게 돼?{p}", "source_medium", None))
        out.append((f"광고 소스/매체별 사용자 알려줘{p}", "source_medium", None))

    for p in politeness:
        out.append((f"상품 카테고리별 매출 확인해줘{p}", "item_category", None))
        out.append((f"국내아동결연 같은 상품유형은 어떻게 돼?{p}", "item_category", None))

    base = list(out)
    fillers = cycle(["", " 지금", " 오늘", " 기준", " 빠르게", " 정확히"])
    i = 0
    while len(out) < target_count:
        q, key, st = base[i % len(base)]
        out.append((f"{q}{next(fillers)}", key, st))
        i += 1
    return out[:target_count]


def _metric_value(metric_name: str, i: int) -> Any:
    m = (metric_name or "").lower()
    if "rate" in m or "ratio" in m:
        return round(0.1 + (i * 0.07), 3)
    if "revenue" in m or "amount" in m:
        return 30000000 - (i * 1500000)
    if any(k in m for k in ["user", "purchaser", "buyer"]):
        return 1200 - (i * 120)
    return 5000 - (i * 500)


def _dim_values(dim_name: str) -> List[str]:
    mapping = {
        "date": ["2026-02-09", "2026-02-10", "2026-02-11", "2026-02-12", "2026-02-13", "2026-02-14", "2026-02-15"],
        "week": ["2026W05", "2026W06"],
        "yearMonth": ["202601", "202602"],
        "defaultChannelGroup": ["Direct", "Display", "Organic Search", "Referral", "Paid Search"],
        "sourceMedium": ["google / cpc", "naver / organic", "(direct) / (none)", "meta / paid_social"],
        "customEvent:is_regular_donation": ["Y", "N"],
        "customEvent:donation_name": ["정기후원", "일시후원", "천원의 힘", "그린노블클럽"],
        "customEvent:menu_name": ["후원하기", "캠페인", "참여", "소개"],
        "itemCategory": ["국내사업후원", "국내아동결연", "해외사업후원", "해외아동결연"],
        "eventName": ["donation_click", "purchase"],
        "itemName": ["[정기후원] 국내사업후원", "[일시후원] 국내사업후원", "[정기후원] 국내아동결연"],
        "country": ["South Korea", "Japan", "United States"],
    }
    return mapping.get(dim_name, ["A", "B", "C"])


def _make_block_data(block):
    metrics = [m.get("name") for m in (block.metrics or []) if m.get("name")]
    dims = [d.get("name") for d in (block.dimensions or []) if d.get("name")]
    btype = block.block_type

    if btype == "total" or not dims:
        return {m: _metric_value(m, 0) for m in metrics}

    dim = dims[0]
    values = _dim_values(dim)
    rows = []
    for i, dv in enumerate(values[:10]):
        row = {dim: dv}
        for m in metrics:
            row[m] = _metric_value(m, i)
        rows.append(row)
    return rows


def _run_one(question: str, last_state: Dict[str, Any] = None) -> Tuple[Dict[str, Any], str]:
    ex = CandidateExtractor()
    pl = GA4Planner()
    ext = ex.extract(question=question, last_state=last_state, date_context=None, semantic=None)
    plan = pl.build_plan(
        property_id="resp-regression",
        question=question,
        intent=ext["intent"],
        metric_candidates=ext["metric_candidates"],
        dimension_candidates=ext["dimension_candidates"],
        date_range=ext["date_range"],
        modifiers=ext["modifiers"],
        last_state=last_state,
    )
    blocks = []
    for b in plan.blocks:
        blocks.append({
            "type": b.block_type,
            "title": b.title or "분석",
            "data": _make_block_data(b),
        })
    adapted = adapt_pipeline_response_to_legacy({"blocks": blocks, "status": "ok"}, question=question, user_name="DataNugget")
    return adapted, (plan.blocks[0].metrics[0]["name"] if plan.blocks and plan.blocks[0].metrics else "")


def _is_bad_message(msg: str) -> bool:
    if not msg or not msg.strip():
        return True
    bad_tokens = [
        "0개 블록 분석 완료",
        "질문에서 매칭 가능한 지표를 찾지 못했습니다",
        "질문 의도는 이해했지만 현재 데이터에서 조건에 맞는 항목을 찾지 못했습니다",
    ]
    return any(t in msg for t in bad_tokens)


def _validate_message(key: str, q: str, msg: str) -> Tuple[bool, str]:
    if _is_bad_message(msg):
        return False, "fallback_or_empty"

    if key == "total_revenue":
        if ("원" not in msg) or ("매출" not in msg and "수익" not in msg):
            return False, "no_revenue_signal"
    elif key == "type_followup":
        if all(t not in msg for t in ["정기후원 여부", "Y", "N", "유형"]):
            return False, "no_type_signal"
    elif key == "first_buyer_rate":
        if "%" not in msg:
            return False, "no_percent"
    elif key == "channel_purchasers":
        if "기본 채널 그룹" not in msg and "채널" not in msg:
            return False, "no_channel_signal"
        if ("명" not in msg) and ("구매자" not in msg):
            return False, "no_buyer_count_signal"
        if "원" in msg and ("구매자수" in q or "매출 일으킨 사용자" in q):
            return False, "money_instead_of_buyer"
    elif key == "donation_click_count":
        if ("donation_click" not in msg and "후원 클릭" not in msg) or ("회" not in msg and "이벤트 수" not in msg):
            return False, "no_click_count_signal"
    elif key == "donation_name_breakdown":
        if "후원명" not in msg and "donation_name" not in msg:
            return False, "no_donation_name_signal"
    elif key == "menu_name":
        if "메뉴명" not in msg and "menu_name" not in msg:
            return False, "no_menu_name_signal"
    elif key == "source_medium":
        if "소스/매체" not in msg and "sourceMedium" not in msg and "소스 매체" not in msg:
            return False, "no_source_medium_signal"
    elif key == "item_category":
        if "상품 카테고리" not in msg and "itemCategory" not in msg:
            return False, "no_item_category_signal"

    return True, "ok"


def run():
    qs = _build_questions(1000)
    failures = []
    stat: Dict[str, Dict[str, int]] = {}

    for i, (q, key, last_state) in enumerate(qs, 1):
        stat.setdefault(key, {"ok": 0, "fail": 0})
        adapted, top_metric = _run_one(q, last_state=last_state)
        msg = str(adapted.get("message", ""))
        ok, reason = _validate_message(key, q, msg)
        if ok:
            stat[key]["ok"] += 1
        else:
            stat[key]["fail"] += 1
            failures.append({
                "index": i,
                "key": key,
                "question": q,
                "reason": reason,
                "metric": top_metric,
                "message": msg[:220],
            })

    total = len(qs)
    failed = len(failures)
    passed = total - failed
    rate = (passed / total * 100.0) if total else 0.0

    print(f"TOTAL: {total}")
    print(f"PASSED: {passed}")
    print(f"FAILED: {failed}")
    print(f"PASS_RATE: {rate:.2f}%")
    print("")
    print("BY_SCENARIO:")
    for k in sorted(stat.keys()):
        ok = stat[k]["ok"]
        t = ok + stat[k]["fail"]
        print(f"- {k}: {ok}/{t} ({(ok/t*100 if t else 0):.1f}%)")

    if failures:
        print("")
        print("SAMPLE_FAILURES:")
        for f in failures[:20]:
            print(f"[{f['index']}] {f['key']} :: {f['question']}")
            print(f"  reason={f['reason']} metric={f['metric']} msg={f['message']}")
        raise SystemExit(1)


if __name__ == "__main__":
    run()
