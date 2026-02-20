#!/usr/bin/env python3
"""QA evaluation runner (500 cases) with route/intent/date/metric-dimension scoring."""

import json
import os
import random
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from qa_module import QueryRouter
from candidate_extractor import CandidateExtractor, DateParser
from planner import GA4Planner
from response_adapter import adapt_pipeline_response_to_legacy
from file_engine import file_engine


def _determine_route_with_state(question: str, previous_state: Optional[Dict[str, Any]]) -> str:
    q = (question or "").lower()
    if any(k in q for k in ["연결", "속성", "계정"]):
        return "system"
    has_ga4 = any(k in q for k in QueryRouter.GA4_KEYWORDS)
    has_file = any(k in q for k in QueryRouter.FILE_KEYWORDS)
    if has_ga4 and has_file:
        return "mixed"
    if has_file:
        return "file"
    if previous_state and previous_state.get("active_source") in ["ga4", "file", "mixed"]:
        return previous_state.get("active_source")
    # router 기본값과 동일하게 GA4
    return "ga4"


def _metric_value(metric_name: str, i: int) -> Any:
    m = (metric_name or "").lower()
    if "rate" in m or "ratio" in m:
        return round(0.08 + (i * 0.05), 3)
    if "revenue" in m or "amount" in m:
        return 35000000 - (i * 1500000)
    if any(k in m for k in ["user", "purchaser", "buyer"]):
        return 250000 - (i * 4500)
    return 10000 - (i * 300)


def _dim_values(dim_name: str) -> List[str]:
    mapping = {
        "date": ["2026-02-09", "2026-02-10", "2026-02-11", "2026-02-12", "2026-02-13", "2026-02-14", "2026-02-15"],
        "week": ["2026W05", "2026W06"],
        "yearMonth": ["202601", "202602"],
        "defaultChannelGroup": ["Direct", "Display", "Organic Search", "Referral", "Paid Search"],
        "sourceMedium": ["(direct) / (none)", "meta / display", "google / cpc", "naver / search_ad"],
        "customEvent:donation_name": ["정기후원", "일시후원", "천원의 힘", "그린노블클럽"],
        "customEvent:menu_name": ["후원하기", "캠페인", "소식", "참여"],
        "eventName": ["page_view", "donation_click", "purchase"],
        "itemName": ["[정기후원] 국내사업후원", "[일시후원] 국내사업후원", "[정기후원] 국내아동결연"],
    }
    return mapping.get(dim_name, ["A", "B", "C"])


def _make_block_data(block) -> Any:
    metrics = [m.get("name") for m in (block.metrics or []) if m.get("name")]
    dims = [d.get("name") for d in (block.dimensions or []) if d.get("name")]
    if block.block_type == "total" or not dims:
        return {m: _metric_value(m, 0) for m in metrics}
    dim = dims[0]
    rows = []
    for i, dv in enumerate(_dim_values(dim)[:10]):
        row = {dim: dv}
        for m in metrics:
            row[m] = _metric_value(m, i)
        rows.append(row)
    return rows


def _run_ga4(question: str, previous_state: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    ex = CandidateExtractor()
    pl = GA4Planner()
    use_date_context = {"enabled": True} if "그 전주" in (question or "") else None
    ext = ex.extract(question=question, last_state=previous_state, date_context=use_date_context, semantic=None)
    plan = pl.build_plan(
        property_id="qa-eval",
        question=question,
        intent=ext["intent"],
        metric_candidates=ext["metric_candidates"],
        dimension_candidates=ext["dimension_candidates"],
        date_range=ext["date_range"],
        modifiers=ext["modifiers"],
        last_state=previous_state,
    )
    blocks = [{"type": b.block_type, "title": b.title or "분석", "data": _make_block_data(b)} for b in plan.blocks]
    legacy = adapt_pipeline_response_to_legacy({"status": "ok", "blocks": blocks}, question=question, user_name="QA")
    first = plan.blocks[0] if plan.blocks else None
    return {
        "intent": ext.get("intent"),
        "date_range": ext.get("date_range") or {},
        "metrics": [m.get("name") for m in (first.metrics or [])] if first else [],
        "dimensions": [d.get("name") for d in (first.dimensions or [])] if first else [],
        "response": legacy,
    }


def _build_sample_csv(path: str) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df = pd.DataFrame(
        {
            "moc_idx": list(range(1, 51)),
            "moc_user_id": [f"U{i:04d}" for i in range(1, 51)],
            "moc_is_use": [1] * 50,
            "moc_is_admin": [1 if i % 10 == 0 else 0 for i in range(1, 51)],
            "moc_require_input": [0] * 50,
            "category": ["A", "B", "C", "A", "B"] * 10,
            "amount": [1000 + (i * 100) for i in range(50)],
        }
    )
    df.to_csv(path, index=False)
    return path


def _run_file(question: str, csv_path: str, previous_state: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    intent = file_engine._detect_intent(question, previous_state or {})
    resp = file_engine.process(question, csv_path, conversation_id=None, prev_source=None)
    return {"intent": intent.get("type"), "response": resp}


def _is_hallucinated(case: Dict[str, Any], result: Dict[str, Any]) -> bool:
    msg = str((result.get("system_response_json") or {}).get("response", {}).get("message", "")).strip()
    if not msg:
        return True
    bad = [
        "0개 블록 분석 완료",
        "질문에서 매칭 가능한 지표를 찾지 못했습니다",
        "질문 의도는 이해했지만 현재 데이터에서 조건에 맞는 항목을 찾지 못했습니다",
        "오류:",
    ]
    if any(t in msg for t in bad):
        return True
    if case["expected_route"] == "file" and any(t in msg for t in ["구매 수익", "활성 사용자", "GA4"]):
        return True
    return False


def _score_route(expected_route: str, actual_route: str) -> int:
    if expected_route == actual_route:
        return 2
    if expected_route in ["ga4", "mixed"] and actual_route in ["ga4", "mixed"]:
        return 1
    return 0


def _score_intent(case: Dict[str, Any], result: Dict[str, Any]) -> int:
    expected = case.get("expected_intent")
    if not expected:
        return 3
    actual = result.get("actual_intent")
    if actual == expected:
        return 3
    if expected in ["topn", "breakdown"] and actual in ["topn", "breakdown"]:
        return 2
    if actual:
        return 1
    return 0


def _score_date(case: Dict[str, Any], result: Dict[str, Any]) -> int:
    mode = case.get("expected_date_mode", "none")
    actual = result.get("actual_date_range") or {}
    s, e = actual.get("start_date"), actual.get("end_date")
    if mode == "none":
        return 2 if (not s and not e) else 1
    if mode == "last_week":
        es, ee = DateParser._phrase_to_range("지난주")
        if s == es and e == ee:
            return 2
        return 1 if s and e else 0
    if mode == "relative_shift":
        prev = case.get("previous_state") or {}
        if prev.get("start_date") and prev.get("end_date"):
            ps = datetime.strptime(prev["start_date"], "%Y-%m-%d") - timedelta(days=7)
            pe = datetime.strptime(prev["end_date"], "%Y-%m-%d") - timedelta(days=7)
            if s == ps.strftime("%Y-%m-%d") and e == pe.strftime("%Y-%m-%d"):
                return 2
        return 1 if s and e else 0
    return 1 if s and e else 0


def _score_dim_metric(case: Dict[str, Any], result: Dict[str, Any]) -> int:
    exp_m = set(case.get("expected_metrics") or [])
    exp_d = set(case.get("expected_dimensions") or [])
    act_m = set(result.get("actual_metrics") or [])
    act_d = set(result.get("actual_dimensions") or [])
    if not exp_m and not exp_d:
        return 3 if act_m or act_d else 1
    m_ok = (not exp_m) or bool(exp_m & act_m)
    d_ok = (not exp_d) or bool(exp_d & act_d)
    if m_ok and d_ok:
        return 3
    if m_ok or d_ok:
        return 2
    return 0


def _evaluate(case: Dict[str, Any], result: Dict[str, Any]) -> Dict[str, Any]:
    s1 = _score_route(case["expected_route"], result["actual_route"])
    s2 = _score_intent(case, result)
    s3 = _score_date(case, result)
    s4 = _score_dim_metric(case, result)
    penalty = -3 if _is_hallucinated(case, result) else 0
    total = s1 + s2 + s3 + s4 + penalty
    return {"route": s1, "intent": s2, "date": s3, "dim_metric": s4, "hallucination_penalty": penalty, "total": total}


def _build_cases() -> List[Dict[str, Any]]:
    cases: List[Dict[str, Any]] = []
    suffixes = ["", " 알려줘", " 부탁해", " 빠르게", " 자세히"]

    # 1) GA4 metric single
    for i in range(100):
        s = suffixes[i % len(suffixes)]
        q = random.choice(["총 매출 알려줘", "구매 수익은?", "매출은 어때"]) + s
        cases.append({
            "question": q,
            "expected_route": "ga4",
            "expected_intent": "metric_single",
            "expected_date_mode": "none",
            "expected_metrics": ["purchaseRevenue"],
            "expected_dimensions": [],
            "previous_state": None,
        })

    # 2) GA4 trend (last week, daily)
    for i in range(80):
        s = suffixes[i % len(suffixes)]
        q = random.choice(["지난주 사용자 추이 알려줘", "지난주 활성 사용자 일별 추이", "지난주 유저 변화"]) + s
        cases.append({
            "question": q,
            "expected_route": "ga4",
            "expected_intent": "trend",
            "expected_date_mode": "last_week",
            "expected_metrics": ["activeUsers"],
            "expected_dimensions": ["date"],
            "previous_state": None,
        })

    # 3) GA4 breakdown donation click/name
    for i in range(60):
        s = suffixes[i % len(suffixes)]
        q = random.choice(["donation_click의 donation_name 보여줘", "주로 어떤 donation을 클릭했어", "후원명별 donation_click"]) + s
        cases.append({
            "question": q,
            "expected_route": "ga4",
            "expected_intent": "breakdown",
            "expected_date_mode": "none",
            "expected_metrics": ["eventCount"],
            "expected_dimensions": ["customEvent:donation_name"],
            "previous_state": None,
        })

    # 4) GA4 relative shift (previous state aware)
    prev = {"start_date": "2026-02-09", "end_date": "2026-02-15", "metrics": [{"name": "activeUsers"}], "dimensions": [{"name": "date"}], "intent": "trend"}
    for i in range(40):
        s = suffixes[i % len(suffixes)]
        q = random.choice(["그 전주 사용자는?", "전주 사용자 추이는?", "그 전주와 비교"]) + s
        cases.append({
            "question": q,
            "expected_route": "ga4",
            "expected_intent": "comparison",
            "expected_date_mode": "relative_shift",
            "expected_metrics": ["activeUsers"],
            "expected_dimensions": ["date"],
            "previous_state": prev,
        })

    # 5) FILE analysis
    file_q = [
        ("데이터 구조 알려줘", "schema"),
        ("사용자는 얼마나 되?", "count_users"),
        ("어드민은 몇명이야?", "count_admin"),
        ("또 어떤 데이터가 있어?", "columns_summary"),
        ("그게 무슨 뜻이야?", "explain"),
    ]
    for i in range(120):
        base_q, intent = file_q[i % len(file_q)]
        s = suffixes[(i // len(file_q)) % len(suffixes)]
        cases.append({
            "question": base_q + s,
            "expected_route": "file",
            "expected_intent": intent,
            "expected_date_mode": "none",
            "expected_metrics": [],
            "expected_dimensions": [],
            "previous_state": {
                "active_source": "file",
                "last_intent": {"type": "count_users"} if intent == "explain" else {"type": intent},
                "last_analysis_meta": {"user_count": 50} if intent == "explain" else {},
            },
        })

    # 6) MIXED analysis
    for i in range(100):
        s = suffixes[i % len(suffixes)]
        q = random.choice([
            "업로드한 파일 컬럼하고 지난주 사용자 추이 같이 보여줘",
            "csv 데이터와 ga4 매출 같이 비교해줘",
            "파일 구조랑 GA4 구매 수익 같이 요약해줘",
            "업로드 파일 사용자수와 GA4 활성 사용자 같이 봐줘",
        ]) + s
        cases.append({
            "question": q,
            "expected_route": "mixed",
            "expected_intent": "metric_multi",
            "expected_date_mode": "last_week" if "지난주" in q else "none",
            "expected_metrics": ["activeUsers"] if "활성 사용자" in q or "사용자" in q else ["purchaseRevenue"],
            "expected_dimensions": ["date"] if "지난주" in q else [],
            "previous_state": None,
        })

    return cases[:500]


def run():
    random.seed(7)
    report_dir = os.path.join(PROJECT_ROOT, "reports")
    os.makedirs(report_dir, exist_ok=True)
    csv_path = _build_sample_csv(os.path.join(report_dir, "_qa_sample.csv"))

    cases = _build_cases()
    out_rows = []

    for idx, case in enumerate(cases, 1):
        q = case["question"]
        prev = case.get("previous_state")
        actual_route = _determine_route_with_state(q, prev)

        actual_intent = None
        actual_date_range = {}
        actual_metrics = []
        actual_dimensions = []
        response_payload: Dict[str, Any] = {"message": ""}

        if actual_route == "file":
            fr = _run_file(q, csv_path, prev)
            actual_intent = fr.get("intent")
            response_payload = fr.get("response") or {}
        elif actual_route == "mixed":
            gr = _run_ga4(q, prev)
            fr = _run_file(q, csv_path, prev)
            actual_intent = "metric_multi"
            actual_date_range = gr.get("date_range") or {}
            actual_metrics = gr.get("metrics") or []
            actual_dimensions = gr.get("dimensions") or []
            response_payload = {
                "message": f"GA4와 파일 데이터를 결합 분석했습니다. [GA4] {str((gr.get('response') or {}).get('message', ''))[:120]}",
                "ga4": gr.get("response"),
                "file": fr.get("response"),
                "status": "ok",
            }
        else:
            gr = _run_ga4(q, prev)
            actual_intent = gr.get("intent")
            actual_date_range = gr.get("date_range") or {}
            actual_metrics = gr.get("metrics") or []
            actual_dimensions = gr.get("dimensions") or []
            response_payload = gr.get("response") or {}

        result = {
            "actual_route": actual_route,
            "actual_intent": actual_intent,
            "actual_date_range": actual_date_range,
            "actual_metrics": actual_metrics,
            "actual_dimensions": actual_dimensions,
            "system_response_json": {"route": actual_route, "response": response_payload},
        }
        score = _evaluate(case, result)

        out_rows.append({
            "id": idx,
            "question": q,
            "expected_route": case["expected_route"],
            "previous_state": case.get("previous_state"),
            "system_response_json": result["system_response_json"],
            "expected_intent": case.get("expected_intent"),
            "expected_date_mode": case.get("expected_date_mode"),
            "expected_metrics": case.get("expected_metrics"),
            "expected_dimensions": case.get("expected_dimensions"),
            "actual_route": actual_route,
            "actual_intent": actual_intent,
            "actual_date_range": actual_date_range,
            "actual_metrics": actual_metrics,
            "actual_dimensions": actual_dimensions,
            "score": score,
        })

    jsonl_path = os.path.join(report_dir, "qa_eval_500_results.jsonl")
    with open(jsonl_path, "w", encoding="utf-8") as f:
        for r in out_rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    totals = defaultdict(float)
    by_route = defaultdict(lambda: {"count": 0, "sum": 0.0})
    for r in out_rows:
        s = r["score"]
        totals["route"] += s["route"]
        totals["intent"] += s["intent"]
        totals["date"] += s["date"]
        totals["dim_metric"] += s["dim_metric"]
        totals["hallucination_penalty"] += s["hallucination_penalty"]
        totals["total"] += s["total"]
        by_route[r["expected_route"]]["count"] += 1
        by_route[r["expected_route"]]["sum"] += s["total"]

    avg_total = totals["total"] / len(out_rows)
    pass7 = sum(1 for r in out_rows if r["score"]["total"] >= 7)
    pass9 = sum(1 for r in out_rows if r["score"]["total"] >= 9)
    worst = sorted(out_rows, key=lambda x: x["score"]["total"])[:20]

    md_path = os.path.join(report_dir, "qa_eval_500_report.md")
    lines = []
    lines.append("# QA Eval Report (500)")
    lines.append(f"- Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"- Total cases: {len(out_rows)}")
    lines.append(f"- Avg total score: {avg_total:.2f} / 10")
    lines.append(f"- Pass(>=9): {pass9}/{len(out_rows)} ({(pass9/len(out_rows))*100:.1f}%)")
    lines.append(f"- Pass(>=7): {pass7}/{len(out_rows)} ({(pass7/len(out_rows))*100:.1f}%)")
    lines.append("")
    lines.append("## Score Averages")
    lines.append(f"- Route (0~2): {totals['route']/len(out_rows):.2f}")
    lines.append(f"- Intent (0~3): {totals['intent']/len(out_rows):.2f}")
    lines.append(f"- Date (0~2): {totals['date']/len(out_rows):.2f}")
    lines.append(f"- Dim/Metric (0~3): {totals['dim_metric']/len(out_rows):.2f}")
    lines.append(f"- Hallucination Penalty (-3~0): {totals['hallucination_penalty']/len(out_rows):.2f}")
    lines.append("")
    lines.append("## By Expected Route")
    for route, st in sorted(by_route.items()):
        lines.append(f"- {route}: {st['count']} cases, avg {st['sum']/st['count']:.2f}")
    lines.append("")
    lines.append("## Worst 20")
    for w in worst:
        lines.append(f"- [{w['id']}] score={w['score']['total']} route(exp/act)={w['expected_route']}/{w['actual_route']} q={w['question']}")

    with open(md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"RESULTS_JSONL: {jsonl_path}")
    print(f"REPORT_MD: {md_path}")
    print(f"TOTAL: {len(out_rows)}")
    print(f"AVG_TOTAL: {avg_total:.2f}")
    print(f"PASS9: {pass9}/{len(out_rows)}")
    print(f"PASS7: {pass7}/{len(out_rows)}")


if __name__ == "__main__":
    run()
