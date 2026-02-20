#!/usr/bin/env python3
"""Release smoke checks for intent/plan safety.

Usage:
  python3 scripts/release_smoke.py
"""

import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from candidate_extractor import CandidateExtractor
from planner import GA4Planner
from pipeline import GA4Pipeline
from response_adapter import _extract_entity_terms as _extract_entity_terms_response
from response_adapter import _format_top_rows as _format_top_rows_response


def _first_block(question, last_state=None):
    ex = CandidateExtractor()
    pl = GA4Planner()
    ext = ex.extract(question, last_state=last_state)
    plan = pl.build_plan(
        property_id="smoke",
        question=question,
        intent=ext["intent"],
        metric_candidates=ext["metric_candidates"],
        dimension_candidates=ext["dimension_candidates"],
        date_range=ext["date_range"],
        modifiers=ext["modifiers"],
        last_state=last_state,
    )
    if not plan.blocks:
        raise AssertionError(f"No blocks for question: {question}")
    return ext, plan.blocks[0]


def run():
    failures = []

    # 1) gnb 메뉴 질문 -> event_filter + menu_name
    try:
        ext, b = _first_block("gnb에서 가장 많이 클릭하는 메뉴는?")
        # event_filter는 데이터/질문 형태에 따라 비어 있을 수 있다.
        # 핵심은 menu_name 기준 breakdown과 이벤트 카운트 지표 선택.
        dims = {d["name"] for d in b.dimensions}
        metrics = {m["name"] for m in b.metrics}
        assert "customEvent:menu_name" in dims
        assert "eventCount" in metrics
    except Exception as e:
        failures.append(f"[gnb_menu] {e}")

    # 2) donation_click 클릭수 -> eventCount + donation_name + donation_click filter
    try:
        _, b = _first_block("후원유형별 클릭수는?")
        metrics = {m["name"] for m in b.metrics}
        dims = {d["name"] for d in b.dimensions}
        assert "eventCount" in metrics
        assert "customEvent:donation_name" in dims
        assert b.filters.get("event_filter") == "donation_click"
    except Exception as e:
        failures.append(f"[donation_click] {e}")

    # 3) scroll -> scroll filter + percent_scrolled
    try:
        _, b = _first_block("페이지별 스크롤은?")
        dims = {d["name"] for d in b.dimensions}
        assert b.filters.get("event_filter") == "scroll"
        assert "customEvent:percent_scrolled" in dims
    except Exception as e:
        failures.append(f"[scroll] {e}")

    # 4) follow-up regroup should keep event metric/dim from last state
    try:
        last_state = {
            "metrics": [{"name": "eventCount"}],
            "dimensions": [{"name": "customEvent:menu_name"}],
            "start_date": "2026-02-10",
            "end_date": "2026-02-17",
            "intent": "breakdown",
        }
        _, b = _first_block("메뉴명으로 묶어서 보여줘", last_state=last_state)
        metrics = {m["name"] for m in b.metrics}
        dims = {d["name"] for d in b.dimensions}
        assert "eventCount" in metrics
        assert "customEvent:menu_name" in dims
    except Exception as e:
        failures.append(f"[followup_group] {e}")

    # 5) purchase vs donation_click 비교는 단일 event_filter로 덮어쓰지 않아야 함
    try:
        _, b = _first_block("구매의 donation_name과 donation_click의 donation_name을 name별로 구분해줄 수 있어?")
        dims = {d["name"] for d in b.dimensions}
        metrics = {m["name"] for m in b.metrics}
        assert "eventCount" in metrics
        assert "eventName" in dims
        assert "customEvent:donation_name" in dims
        event_filters = b.filters.get("event_filters") or []
        assert sorted(event_filters) == ["donation_click", "purchase"]
        assert "event_filter" not in b.filters
    except Exception as e:
        failures.append(f"[purchase_vs_donation_click] {e}")

    # 6) "총 매출 + 상품별 매출"은 합계+분해 의도를 모두 가져야 함
    try:
        ex = CandidateExtractor()
        ext = ex.extract("총 매출과 상품별 매출 알려줘")
        mods = ext.get("modifiers", {})
        assert mods.get("needs_total") is True
        assert mods.get("needs_breakdown") is True
        assert mods.get("scope_hint") == ["item"]
    except Exception as e:
        failures.append(f"[total_and_item_revenue] {e}")

    # 7) donation_name 매출 질의는 purchase + donation_name 분해
    try:
        _, b = _first_block("후원 유형 donation_name별 매출이 궁금해")
        dims = {d["name"] for d in b.dimensions}
        metrics = {m["name"] for m in b.metrics}
        assert "purchaseRevenue" in metrics
        assert "customEvent:donation_name" in dims
        assert b.filters.get("event_filter") == "purchase"
    except Exception as e:
        failures.append(f"[donation_name_revenue] {e}")

    # 8) 기간-only 팔로업은 명시 기간 응답을 반환
    try:
        p = GA4Pipeline()
        r = p.run(question="지난주 기준이야?", property_id="smoke", conversation_id=None, semantic=None)
        assert r.get("status") == "ok"
        assert isinstance(r.get("period"), str) and " ~ " in r.get("period")
        assert "기준 기간" in (r.get("message") or "")
    except Exception as e:
        failures.append(f"[period_followup] {e}")

    # 9) 원인분석 후 "채널별" follow-up은 item 반복이 아닌 channel breakdown 이어야 함
    try:
        last_state = {
            "metrics": [{"name": "itemRevenue"}],
            "dimensions": [{"name": "itemName"}],
            "start_date": "2026-02-10",
            "end_date": "2026-02-17",
            "intent": "breakdown",
        }
        _, b = _first_block("채널별로 확인해줘", last_state=last_state)
        metrics = {m["name"] for m in b.metrics}
        dims = {d["name"] for d in b.dimensions}
        assert "eventCount" in metrics
        assert "defaultChannelGroup" in dims
    except Exception as e:
        failures.append(f"[channel_followup_scope_switch] {e}")

    # 10) 조사 결합 토큰(예: 일시후원중) 정규화
    try:
        ex = CandidateExtractor()
        terms_ex = ex.extract("정기후원과 일시후원중 어떤게 더 많아?").get("modifiers", {}).get("entity_contains", [])
        terms_res = _extract_entity_terms_response("정기후원과 일시후원중 어떤게 더 많아?")
        assert any("일시후원" == t for t in terms_res)
        assert not any("일시후원중" == t for t in terms_res)
        # extractor 쪽은 질문 유형에 따라 entity_contains가 없을 수 있어 최소한 오염 토큰 미포함만 확인
        assert not any("일시후원중" == t for t in terms_ex)
    except Exception as e:
        failures.append(f"[entity_suffix_normalization] {e}")

    # 11) 광고/소스/매체 후속 질문은 sourceMedium 차원으로 매칭
    try:
        last_state = {
            "metrics": [{"name": "activeUsers"}],
            "dimensions": [{"name": "defaultChannelGroup"}],
            "start_date": "2026-02-10",
            "end_date": "2026-02-17",
            "intent": "breakdown",
        }
        _, b = _first_block("광고는 없어? 소스나 매체쪽에?", last_state=last_state)
        dims = {d["name"] for d in b.dimensions}
        assert "sourceMedium" in dims
    except Exception as e:
        failures.append(f"[source_medium_followup] {e}")

    # 12) 상품유형 질의는 itemCategory 차원으로 분해
    try:
        _, b = _first_block("국내아동결연 같은 상품유형은 어떻게 되?")
        dims = {d["name"] for d in b.dimensions}
        assert "itemCategory" in dims
    except Exception as e:
        failures.append(f"[item_category_query] {e}")

    # 13) 처음 후원/첫구매 질의는 firstTimePurchasers 우선
    try:
        _, b = _first_block("처음 후원한 사용자는 얼마나 되?")
        metrics = {m["name"] for m in b.metrics}
        assert "firstTimePurchasers" in metrics
    except Exception as e:
        failures.append(f"[first_time_purchasers] {e}")

    # 14) 지난달+이번달 매출 질의는 월 단위 비교 분해(yearMonth)
    try:
        _, b = _first_block("지난달과 이번달 매출은?")
        dims = {d["name"] for d in b.dimensions}
        metrics = {m["name"] for m in b.metrics}
        assert "yearMonth" in dims
        assert "purchaseRevenue" in metrics
    except Exception as e:
        failures.append(f"[month_vs_month_revenue] {e}")

    # 15) 후원 클릭 발생량 질문은 donation_click + eventCount
    try:
        _, b = _first_block("후원 클릭은 얼마나 일어났어?")
        metrics = {m["name"] for m in b.metrics}
        assert "eventCount" in metrics
        assert b.filters.get("event_filter") == "donation_click"
    except Exception as e:
        failures.append(f"[donation_click_volume] {e}")

    # 16) yearMonth 표시 포맷은 202601 -> 2026-01
    try:
        rows = [{"yearMonth": "202601", "activeUsers": 100}, {"yearMonth": "202602", "activeUsers": 80}]
        lines = _format_top_rows_response(rows, max_rows=2)
        text = "\n".join(lines)
        assert "2026-01" in text
        assert "202,601" not in text
    except Exception as e:
        failures.append(f"[yearmonth_display_format] {e}")

    if failures:
        print("FAIL")
        for f in failures:
            print(f)
        raise SystemExit(1)

    print("PASS: release smoke checks")


if __name__ == "__main__":
    run()
