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

    if failures:
        print("FAIL")
        for f in failures:
            print(f)
        raise SystemExit(1)

    print("PASS: release smoke checks")


if __name__ == "__main__":
    run()
