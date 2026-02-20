import logging
from datetime import timedelta
from flask import Flask, request, jsonify, session, redirect, url_for, send_from_directory, Response
from werkzeug.utils import secure_filename
from werkzeug.middleware.proxy_fix import ProxyFix
from oauthlib.oauth2 import WebApplicationClient
import google.auth.transport.requests
import google.oauth2.credentials
import googleapiclient.discovery
import requests
import os
import json
import logging
import pandas as pd
import re
from html import unescape
from urllib.parse import unquote
from urllib.parse import urlparse, urlunparse
from dotenv import load_dotenv
from typing import Any, Dict, List, Tuple
from qa_module import handle_question, generate_unique_id
import base64
import urllib.parse
from db_manager import DBManager
from file_engine import file_engine
from db_manager import DBManager
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric
DBManager.init_db()
import math
from semantic_matcher import SemanticMatcher
from ga4_metadata import GA4_METRICS, GA4_DIMENSIONS

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv()
semantic = SemanticMatcher(os.path.join(BASE_DIR, "vectorizer.pkl"))
semantic.build_metric_index(GA4_METRICS)
semantic.build_dimension_index(GA4_DIMENSIONS)


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on", "y"}


def _load_google_web_creds() -> dict:
    """
    Load Google OAuth web credentials from env or file.
    Priority:
      1) GOOGLE_OAUTH_CLIENT_JSON (raw JSON string)
      2) GOOGLE_OAUTH_CLIENT_JSON_BASE64 (base64 encoded JSON)
      3) GOOGLE_CLIENT_SECRET_PATH (default: client_secret.json)
    """
    raw = os.getenv("GOOGLE_OAUTH_CLIENT_JSON", "").strip()
    if raw:
        data = json.loads(raw)
        return data.get("web", data)

    b64 = os.getenv("GOOGLE_OAUTH_CLIENT_JSON_BASE64", "").strip()
    if b64:
        decoded = base64.b64decode(b64.encode("utf-8")).decode("utf-8")
        data = json.loads(decoded)
        return data.get("web", data)

    secret_path = os.getenv("GOOGLE_CLIENT_SECRET_PATH", "client_secret.json")
    with open(secret_path, "r") as f:
        data = json.loads(f.read())
    return data.get("web", data)


def _require_admin_token() -> bool:
    expected = os.getenv("ADMIN_API_TOKEN", "").strip()
    if not expected:
        return False
    auth = request.headers.get("Authorization", "")
    if auth.lower().startswith("bearer "):
        provided = auth.split(" ", 1)[1].strip()
        return provided == expected
    return False

def sanitize(obj):
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, dict):
        return {k: sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize(v) for v in obj]
    return obj


def _is_negative_feedback_text(text: str) -> bool:
    q = (text or "").strip().lower()
    if not q:
        return False
    # 분석 질문에서 자주 쓰는 "이상치"는 피드백이 아님
    if "이상치" in q:
        return False
    # 추천 질문/탐색 질문 문장 패턴은 피드백 아님
    if q.endswith("?") and any(k in q for k in ["해볼까요", "보여줘", "알려줘", "점검", "분석"]):
        return False

    feedback_tokens = [
        "틀렸", "엉망", "잘못", "오답", "아닌데", "아니야",
        "말이 안", "틀린", "다시 해", "맞지 않아",
        "이상해", "이상하네", "이상하다"
    ]
    return any(t in q for t in feedback_tokens)


def _is_feedback_only_text(text: str) -> bool:
    q = (text or "").strip().lower()
    if not q:
        return False
    if not _is_negative_feedback_text(q):
        return False
    analytics_tokens = [
        "매출", "수익", "사용자", "세션", "전환", "이벤트", "클릭", "구매",
        "채널", "소스", "매체", "국가", "기간", "지난주", "지난달", "비교", "추이", "비율"
    ]
    return (len(q) <= 20) and (not any(t in q for t in analytics_tokens))


def _is_valid_http_url(url: str) -> bool:
    try:
        p = urlparse(str(url or "").strip())
        return p.scheme in {"http", "https"} and bool(p.netloc)
    except Exception:
        return False


def _tokenize_text(text: str) -> List[str]:
    if not text:
        return []
    return [t.lower() for t in re.findall(r"[A-Za-z0-9가-힣_]+", str(text)) if len(t) >= 2]


def _jaccard(a: List[str], b: List[str]) -> float:
    sa, sb = set(a or []), set(b or [])
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / max(1, len(sa | sb))


def _intent_signature(text: str) -> Dict[str, bool]:
    t = (text or "").lower()
    return {
        "revenue": any(k in t for k in ["매출", "수익", "revenue", "amount"]),
        "user": any(k in t for k in ["사용자", "유저", "구매자", "후원자", "user", "purchaser"]),
        "event": any(k in t for k in ["이벤트", "클릭", "event", "click"]),
        "channel": any(k in t for k in ["채널", "소스", "매체", "경로", "유입", "source", "medium"]),
        "trend": any(k in t for k in ["추이", "트렌드", "흐름", "일별", "월별", "week", "month", "trend"]),
    }


def _build_reask_suggestions(question: str) -> List[str]:
    sig = _intent_signature(question)
    if sig["revenue"] and sig["channel"]:
        return ["구매 수익을 채널별로 볼까요?", "소스/매체 기준으로 볼까요?", "기간을 지난주로 고정할까요?"]
    if sig["revenue"]:
        return ["총 매출로 볼까요?", "상품별 매출 TOP 10으로 볼까요?", "후원 유형별 매출로 볼까요?"]
    if sig["event"]:
        return ["eventName 기준으로 볼까요?", "donation_click 이벤트만 볼까요?", "이벤트를 기간 비교로 볼까요?"]
    if sig["user"]:
        return ["활성 사용자로 볼까요?", "전체 구매자 수로 볼까요?", "채널별 사용자로 볼까요?"]
    return ["지표명을 포함해 다시 질문해 주세요.", "기간을 함께 지정해볼까요?", "차원(예: 채널/상품)도 함께 지정해볼까요?"]


def _extract_message_from_response(resp: Any) -> str:
    try:
        if not isinstance(resp, dict):
            return ""
        body = resp.get("response") if isinstance(resp.get("response"), dict) else resp
        return str(body.get("message", "")) if isinstance(body, dict) else ""
    except Exception:
        return ""


def _is_no_data_or_no_match_response(resp: Any) -> bool:
    msg = _extract_message_from_response(resp)
    if not msg:
        return False
    bad_patterns = [
        "매칭 가능한 지표를 찾지 못",
        "조건에 맞는 항목을 찾지 못",
        "질문 의도는 이해했지만",
        "0개 블록 분석 완료",
        "데이터를 찾을 수 없",
    ]
    return any(p in msg for p in bad_patterns)


def _is_ga_no_match_response(resp: Any) -> bool:
    if not isinstance(resp, dict):
        return False
    route = str(resp.get("route") or "").lower()
    if route not in {"ga4", "ga4_followup"}:
        return False
    return _is_no_data_or_no_match_response(resp)


def _rewrite_ga_question_for_retry(question: str) -> str:
    q = str(question or "").strip()
    lq = q.lower()
    period = ""
    if "지난주" in lq:
        period = "지난주 "
    elif "지난달" in lq:
        period = "지난달 "
    elif "이번달" in lq or "이번 달" in lq:
        period = "이번달 "
    elif "이번주" in lq or "이번 주" in lq:
        period = "이번주 "

    if "사용자" in lq and any(k in lq for k in ["추이", "트렌드", "흐름"]):
        return f"{period}활성 사용자 추이 알려줘".strip()
    if "사용자" in lq:
        return f"{period}활성 사용자 수 알려줘".strip()
    if any(k in lq for k in ["매출", "수익"]):
        return f"{period}구매 수익 알려줘".strip()
    if "세션" in lq:
        return f"{period}세션 수 알려줘".strip()
    if "이벤트" in lq and "클릭" in lq:
        return f"{period}이벤트 클릭 수 알려줘".strip()
    return q


def _rewrite_followup_with_context(followup: str, last_user_question: str) -> str:
    f = str(followup or "").strip()
    last_q = str(last_user_question or "").strip()
    if not f:
        return f
    if not last_q:
        return f

    if "채널별" in f:
        return f"{last_q}를 채널별로 나눠서 보여줘"
    if "디바이스별" in f:
        return f"{last_q}를 디바이스별로 나눠서 보여줘"
    if "랜딩페이지" in f:
        return f"{last_q}를 랜딩페이지별로 보여줘"
    if "이전 기간과 비교" in f or "증감" in f:
        return f"{last_q}를 이전 기간과 비교해 증감까지 보여줘"
    if "TOP 10" in f or "top 10" in f.lower():
        return f"{last_q}를 TOP 10으로 확장해줘"
    if "원인 분석" in f:
        return f"{last_q}의 원인 분석을 해줘"
    return f


def _normalize_followups(route: str, body: Dict[str, Any], current_question: str, last_user_question: str) -> List[str]:
    raw = body.get("followup_suggestions")
    candidates = [str(x).strip() for x in raw] if isinstance(raw, list) else []

    # 실패/불일치 응답에서는 안전한 재질문 세트로 교체
    if _is_no_data_or_no_match_response({"response": body, "route": route}):
        return _build_reask_suggestions(last_user_question or current_question)

    # 추천이 비어 있으면 현재 질문 기반 기본 추천 제공
    if not candidates:
        return _build_reask_suggestions(current_question)

    out = []
    seen = set()
    for f in candidates:
        if not f:
            continue
        # 숫자 선택형 같은 애매 문구 제거
        if re.match(r"^\s*\d+\s*번?\s*$", f):
            continue
        rewritten = _rewrite_followup_with_context(f, last_user_question or current_question)
        s = re.sub(r"\s+", " ", rewritten).strip()
        if not s:
            continue
        if s not in seen:
            seen.add(s)
            out.append(s)

    # 전부 제거되면 안전 기본값
    if not out:
        out = _build_reask_suggestions(current_question)
    return out[:5]


def _extract_response_body(resp: Any) -> Dict[str, Any]:
    if not isinstance(resp, dict):
        return {}
    if isinstance(resp.get("response"), dict):
        return resp["response"]
    return resp


def _apply_bad_regression_guard(user_id: str, question: str, response: Any) -> Any:
    """
    Guardrail for repeated bad-labeled patterns:
    if intent mismatch is detected and the question is similar to previous bad-labeled questions,
    return a safe clarify-style response instead of likely wrong answer.
    """
    if not isinstance(response, dict):
        return response

    body = _extract_response_body(response)
    message = str(body.get("message", ""))
    if not message:
        return response

    qsig = _intent_signature(question)
    rsig = _intent_signature(message)

    mismatch = False
    if qsig["revenue"] and (rsig["user"] and not rsig["revenue"]):
        mismatch = True
    if qsig["event"] and (not rsig["event"] and rsig["revenue"]):
        mismatch = True
    if qsig["channel"] and (not rsig["channel"] and not rsig["event"]):
        mismatch = True
    if qsig["trend"] and ("기준 기간은" in message and "추이" not in message):
        mismatch = True

    if not mismatch:
        return response

    bad_questions = DBManager.get_recent_bad_questions(user_id=user_id, limit=200)
    q_tokens = _tokenize_text(question)
    max_sim = 0.0
    for bq in bad_questions:
        sim = _jaccard(q_tokens, _tokenize_text(bq))
        if sim > max_sim:
            max_sim = sim

    # bad history가 충분하거나, 유사 bad 질문이 있으면 방어 발동
    if len(bad_questions) < 5 and max_sim < 0.25:
        return response

    safe_msg = (
        "질문 의도(지표/차원)와 현재 응답 후보가 어긋날 가능성이 있어 다시 확인이 필요합니다.\n"
        "원하시는 기준을 한 번만 더 지정해 주세요. 예: `매출 + 채널별`, `donation_click + donation_name`, `지난주 + 사용자 추이`"
    )
    body["message"] = safe_msg
    body["status"] = "clarify"
    body["plot_data"] = []
    body["followup_suggestions"] = _build_reask_suggestions(question)
    body["guardrail"] = {
        "type": "bad_regression_guard",
        "max_bad_similarity": round(float(max_sim), 3),
        "bad_pool_size": len(bad_questions)
    }
    if isinstance(response.get("response"), dict):
        response["response"] = body
    else:
        response = body
    return response


def _html_to_notion_markdown(title: str, html_content: str) -> str:
    text = html_content or ""
    # UI-only blocks should not be exported
    text = re.sub(r'(?is)<div[^>]*class="[^"]*followup-box[^"]*"[^>]*>.*?</div>', "", text)
    text = re.sub(r'(?is)<ol[^>]*class="[^"]*followup-list[^"]*"[^>]*>.*?</ol>', "", text)
    text = re.sub(r'(?is)<ul[^>]*class="[^"]*followup-list[^"]*"[^>]*>.*?</ul>', "", text)
    text = re.sub(r'(?is)<div[^>]*class="[^"]*followup-title[^"]*"[^>]*>.*?</div>', "", text)
    text = re.sub(r'(?is)<div[^>]*class="[^"]*card-actions[^"]*"[^>]*>.*?</div>', "", text)
    text = re.sub(r"(?is)<button[^>]*>.*?</button>", "", text)
    text = re.sub(r"(?is)<small[^>]*>\s*<strong>\s*다음\s*질문\s*추천.*?</small>", "", text)

    # Block-level tags
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"(?is)<h1[^>]*>(.*?)</h1>", r"# \1\n", text)
    text = re.sub(r"(?is)<h2[^>]*>(.*?)</h2>", r"## \1\n", text)
    text = re.sub(r"(?is)<h3[^>]*>(.*?)</h3>", r"### \1\n", text)
    text = re.sub(r"(?is)<li[^>]*>(.*?)</li>", r"- \1\n", text)
    text = re.sub(r"(?is)</p>", "\n\n", text)
    text = re.sub(r"(?is)</div>", "\n", text)

    # Inline tags
    text = re.sub(r"(?is)<strong[^>]*>(.*?)</strong>", r"**\1**", text)
    text = re.sub(r"(?is)<b[^>]*>(.*?)</b>", r"**\1**", text)
    text = re.sub(r"(?is)<em[^>]*>(.*?)</em>", r"*\1*", text)
    text = re.sub(r"(?is)<i[^>]*>(.*?)</i>", r"*\1*", text)
    text = re.sub(r"(?is)<code[^>]*>(.*?)</code>", r"`\1`", text)

    # Remove remaining tags
    text = re.sub(r"(?is)<script[^>]*>.*?</script>", "", text)
    text = re.sub(r"(?is)<style[^>]*>.*?</style>", "", text)
    text = re.sub(r"(?is)<[^>]+>", "", text)
    text = unescape(text)

    # Cleanup
    lines = [ln.rstrip() for ln in text.splitlines()]
    cleaned = []
    prev_blank = False
    for ln in lines:
        blank = (ln.strip() == "")
        if blank and prev_blank:
            continue
        cleaned.append(ln)
        prev_blank = blank

    body = "\n".join(cleaned).strip()
    body = re.sub(
        r"(?ims)^\s*다음\s*질문\s*추천\s*:?\s*$\n(?:\s*\d+\.\s.*(?:\n|$))+",
        "",
        body
    ).strip()

    doc_title = title.strip() if title else "Untitled Report"
    header = f"# {doc_title}\n\n"
    if "report-chart-" in (html_content or ""):
        header += "> 참고: 원본 리포트에 차트 블록이 포함되어 있습니다.\n\n"

    # KPI 추출(숫자 포함 라인)
    kpi_lines = []
    for ln in body.splitlines():
        if re.search(r"\d", ln) and len(ln.strip()) <= 140:
            kpi_lines.append(ln.strip())
        if len(kpi_lines) >= 8:
            break
    if not kpi_lines:
        kpi_lines = ["(핵심 KPI를 여기에 입력)"]

    archive_section = (
        "## 1) 리포트 아카이브\n"
        f"- 작성일: {pd.Timestamp.now().strftime('%Y-%m-%d')}\n"
        "- 작성자: \n"
        "- 데이터 소스: GA4 / File / Mixed\n"
        "- 분석 기간: \n"
        "- 리포트 버전: v1\n\n"
    )

    kpi_section = "## 2) KPI 변화 추적\n" + "\n".join([f"- {k}" for k in kpi_lines]) + "\n\n"

    decision_section = (
        "## 3) 판단 기록 (Decision Log)\n"
        "| 날짜 | 관찰/근거 | 판단 | 영향도 | 담당 |\n"
        "|---|---|---|---|---|\n"
        "|  |  |  |  |  |\n\n"
    )

    action_section = (
        "## 4) 실행 관리 (Action Tracker)\n"
        "| 실행 항목 | 목적 KPI | 오너 | 기한 | 상태 | 결과 |\n"
        "|---|---|---|---|---|---|\n"
        "|  |  |  |  | Todo |  |\n\n"
    )

    analysis_section = "## 5) 원본 분석 내용\n" + (body if body else "(원본 본문 없음)") + "\n"

    return header + archive_section + kpi_section + decision_section + action_section + analysis_section


def _strip_report_html_to_text(html_content: str) -> str:
    text = html_content or ""
    text = re.sub(r'(?is)<div[^>]*class="[^"]*followup-box[^"]*"[^>]*>.*?</div>', "", text)
    text = re.sub(r'(?is)<ol[^>]*class="[^"]*followup-list[^"]*"[^>]*>.*?</ol>', "", text)
    text = re.sub(r'(?is)<ul[^>]*class="[^"]*followup-list[^"]*"[^>]*>.*?</ul>', "", text)
    text = re.sub(r'(?is)<div[^>]*class="[^"]*card-actions[^"]*"[^>]*>.*?</div>', "", text)
    text = re.sub(r"(?is)<button[^>]*>.*?</button>", "", text)
    text = re.sub(r"(?is)<script[^>]*>.*?</script>", "", text)
    text = re.sub(r"(?is)<style[^>]*>.*?</style>", "", text)
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"(?is)</p>", "\n", text)
    text = re.sub(r"(?is)</div>", "\n", text)
    text = re.sub(r"(?is)<[^>]+>", "", text)
    text = unescape(text)
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    return "\n".join(lines)


def _safe_float(value: Any) -> float:
    try:
        s = re.sub(r"[^\d\.\-]", "", str(value))
        if s in ("", "-", ".", "-."):
            return 0.0
        return float(s)
    except Exception:
        return 0.0


def _extract_kpi_name(line: str) -> str:
    cleaned = re.sub(r"^\s*(Bot:|You:)\s*", "", line, flags=re.I).strip()
    cleaned = re.sub(r"^[^\w가-힣]+", "", cleaned).strip()
    line = cleaned or line
    m = re.match(r"^\d+\.\s*([^:|]+)", line)
    if m:
        return re.sub(r"\s+", " ", m.group(1)).strip()
    m = re.match(r"^([가-힣A-Za-z0-9_ /]+)\s*[:：]", line)
    if m:
        return re.sub(r"\s+", " ", m.group(1)).strip()
    return re.sub(r"\s+", " ", line[:30]).strip()


def _extract_pct(line: str) -> float:
    m = re.search(r"(-?\d+(?:\.\d+)?)\s*%", line)
    return float(m.group(1)) if m else 0.0


def _extract_curr_prev(line: str) -> Tuple[float, float]:
    cur = 0.0
    prev = 0.0
    m_cur = re.search(r"(현재|current)\s*[:：]?\s*([0-9,.\-]+)", line, flags=re.I)
    m_prev = re.search(r"(이전|prev|previous)\s*[:：]?\s*([0-9,.\-]+)", line, flags=re.I)
    if m_cur:
        cur = _safe_float(m_cur.group(2))
    if m_prev:
        prev = _safe_float(m_prev.group(2))
    return cur, prev


def _preprocess_report_data(title: str, html_content: str) -> Dict[str, Any]:
    text = _strip_report_html_to_text(html_content)
    lines = [ln for ln in text.splitlines() if ln.strip()]

    kpi_summary = []
    growth_rates = []
    segment_changes = []
    numeric_values = []
    ranked_values = []

    for ln in lines:
        nums = re.findall(r"-?\d[\d,]*(?:\.\d+)?", ln)
        for n in nums:
            numeric_values.append(_safe_float(n))

        if re.search(r"매출|수익|사용자|세션|전환|revenue|users|session|conversion", ln, flags=re.I):
            kpi_summary.append({
                "kpi": _extract_kpi_name(ln),
                "line": ln
            })

        if "%" in ln or re.search(r"증감|대비|change|delta", ln, flags=re.I):
            pct = _extract_pct(ln)
            cur, prev = _extract_curr_prev(ln)
            if pct == 0.0 and cur > 0 and prev > 0:
                pct = ((cur - prev) / prev) * 100 if prev else 0.0
            growth_rates.append({
                "metric": _extract_kpi_name(ln),
                "current": cur if cur > 0 else None,
                "prev": prev if prev > 0 else None,
                "change_pct": round(pct, 1),
                "line": ln
            })

        if re.match(r"^\d+\.\s", ln):
            segment_changes.append({"rank_line": ln})
            # 상위 항목 값 추출 (예: "1. ... 9,076,814원")
            nums = re.findall(r"(-?\d[\d,]*(?:\.\d+)?)", ln)
            if nums:
                ranked_values.append(_safe_float(nums[-1]))

    # 간단 이상치 탐지 (중앙값 기준 3배 이상)
    anomalies = []
    positives = [v for v in numeric_values if v > 0]
    if positives:
        mid = sorted(positives)[len(positives) // 2]
        if mid > 0:
            for v in positives:
                if v >= mid * 3:
                    anomalies.append({"type": "high_spike", "value": v})
                elif v <= mid * 0.3:
                    anomalies.append({"type": "low_drop", "value": v})
            anomalies = anomalies[:5]

    # 비교 기준 추정
    comparison_basis = None
    all_text = "\n".join(lines)
    if "전월" in all_text:
        comparison_basis = "전월 대비"
    elif "전주" in all_text:
        comparison_basis = "전주 대비"
    elif "목표" in all_text:
        comparison_basis = "목표 대비"
    elif any(g.get("prev") for g in growth_rates):
        comparison_basis = "이전값 대비"
    elif ranked_values:
        comparison_basis = "상위/하위 분포 대비"
    else:
        comparison_basis = "평균 대비"

    # 구성비/집중도 계산
    composition = {
        "item_share_pct": [],
        "top3_concentration_pct": 0.0,
        "top5_concentration_pct": 0.0
    }
    total_ranked = sum(v for v in ranked_values if v > 0)
    if total_ranked > 0:
        sorted_vals = [v for v in ranked_values if v > 0]
        composition["top3_concentration_pct"] = round(sum(sorted_vals[:3]) / total_ranked * 100, 1)
        composition["top5_concentration_pct"] = round(sum(sorted_vals[:5]) / total_ranked * 100, 1)
        for idx, v in enumerate(sorted_vals[:10], 1):
            composition["item_share_pct"].append({
                "rank": idx,
                "value": v,
                "share_pct": round(v / total_ranked * 100, 1)
            })

    # 코드 기반 리스크 탐지
    risk_flags = []
    top2_pct = round((sum([v for v in ranked_values[:2] if v > 0]) / total_ranked * 100), 1) if total_ranked > 0 else 0.0
    if top2_pct > 50:
        risk_flags.append({"risk": "상위 2개 항목 매출 비중 > 50%", "level": "high"})
    if re.search(r"\(not set\)", all_text, flags=re.I):
        risk_flags.append({"risk": "(not set) 값 존재 - 데이터 정합성 이슈 가능", "level": "high"})
    if any(g.get("prev") in [None, 0] and g.get("current") not in [None, 0] for g in growth_rates):
        risk_flags.append({"risk": "이전값 0/미존재로 비교 불가 항목 존재", "level": "medium"})

    return {
        "title": title,
        "source_lines": lines[:300],
        "kpi_summary": kpi_summary[:12],
        "growth_rates": growth_rates[:12],
        "segment_change_topn": segment_changes[:10],
        "anomalies": anomalies,
        "comparison_basis": comparison_basis,
        "composition": composition,
        "risk_flags": risk_flags
    }


def _build_report_planner(pre: Dict[str, Any]) -> Dict[str, Any]:
    growth = pre.get("growth_rates", [])
    seg = pre.get("segment_change_topn", [])
    anomalies = pre.get("anomalies", [])

    neg = [g for g in growth if g.get("change_pct", 0) < 0]
    if len(neg) >= 2 or len(anomalies) >= 2:
        analysis_type = "risk"
    elif seg:
        analysis_type = "segment"
    else:
        analysis_type = "trend"

    core_kpis = []
    for k in pre.get("kpi_summary", []):
        name = k.get("kpi", "").strip()
        if name and name not in core_kpis:
            core_kpis.append(name)
        if len(core_kpis) >= 3:
            break
    if not core_kpis:
        core_kpis = ["핵심 KPI 1", "핵심 KPI 2", "핵심 KPI 3"]

    highlights = []
    for g in sorted(growth, key=lambda x: abs(x.get("change_pct", 0)), reverse=True)[:3]:
        sign = "증가" if g.get("change_pct", 0) >= 0 else "감소"
        highlights.append(f"{g.get('metric','지표')} {abs(g.get('change_pct', 0)):.1f}% {sign}")
    if not highlights and pre.get("kpi_summary"):
        highlights = [pre["kpi_summary"][0]["line"][:80]]

    if analysis_type == "risk":
        questions = [
            "감소폭이 가장 큰 KPI는 무엇인가?",
            "어떤 세그먼트/채널이 하락을 주도했는가?",
            "즉시 실행 가능한 리스크 완화 액션은 무엇인가?"
        ]
    elif analysis_type == "segment":
        questions = [
            "증감이 가장 큰 세그먼트는 어디인가?",
            "세그먼트 변화의 원인이 되는 유입/디바이스 요인은 무엇인가?",
            "우선순위가 높은 타겟 액션은 무엇인가?"
        ]
    else:
        questions = [
            "핵심 KPI의 최근 추이는 어떤가?",
            "증감률 기준으로 주목할 지표는 무엇인가?",
            "다음 기간 개선을 위한 액션은 무엇인가?"
        ]

    return {
        "analysis_type": analysis_type,
        "core_kpis": core_kpis[:3],
        "highlight_points": highlights[:3],
        "key_questions": questions[:3],
        "comparison_basis": pre.get("comparison_basis", "평균 대비")
    }


def _build_report_object(pre: Dict[str, Any], planner: Dict[str, Any]) -> Dict[str, Any]:
    growth = pre.get("growth_rates", [])
    analysis_type = planner.get("analysis_type", "trend")
    comp = pre.get("composition", {}) or {}

    executive_summary = list(planner.get("highlight_points", []))[:3]
    if comp.get("top3_concentration_pct", 0) > 0:
        executive_summary.insert(0, f"상위 3개 항목 집중도는 {comp.get('top3_concentration_pct', 0):.1f}%입니다.")
    while len(executive_summary) < 3:
        executive_summary.append("추가 분석 포인트를 확인 중입니다.")
    executive_summary = executive_summary[:3]

    trend_analysis = []
    for g in growth[:5]:
        trend_analysis.append({
            "metric": g.get("metric", "metric"),
            "current": g.get("current"),
            "prev": g.get("prev"),
            "change_pct": g.get("change_pct", 0.0)
        })

    hypotheses = []
    if analysis_type == "risk":
        hypotheses = [
            "유입 품질 저하 또는 캠페인 예산 변화 영향 가능성",
            "랜딩/퍼널 단계 이탈 증가 가능성",
            "세그먼트 믹스 변화로 KPI 왜곡 가능성"
        ]
    elif analysis_type == "segment":
        hypotheses = [
            "특정 세그먼트의 채널 효율 변화 가능성",
            "디바이스/랜딩페이지 조합 차이 영향 가능성",
            "프로모션 노출 편차 영향 가능성"
        ]
    else:
        hypotheses = [
            "시즌성/요일 효과가 KPI에 반영되었을 가능성",
            "상위 채널 기여도 변화 가능성",
            "콘텐츠/상품 믹스 변화 영향 가능성"
        ]

    risks = []
    for rf in pre.get("risk_flags", [])[:3]:
        if isinstance(rf, dict):
            risks.append({"risk": str(rf.get("risk", "")), "level": str(rf.get("level", "medium"))})
    for g in growth:
        pct = g.get("change_pct", 0)
        if pct <= -15:
            risks.append({"risk": f"{g.get('metric','지표')} 하락 지속 가능성", "level": "high"})
        elif pct <= -7:
            risks.append({"risk": f"{g.get('metric','지표')} 변동성 확대 가능성", "level": "medium"})
        if len(risks) >= 3:
            break
    if not risks:
        risks = [{"risk": "단기 급락 리스크는 제한적이나 모니터링 필요", "level": "low"}]

    actions = [
        {"action": "하락 KPI 원인 분해(채널/디바이스/랜딩)", "priority": "high", "owner_suggestion": "Data", "deadline_days": 2},
        {"action": "상위 영향 세그먼트 타겟 재점검", "priority": "high", "owner_suggestion": "Marketing", "deadline_days": 5},
        {"action": "랜딩/퍼널 전환 저하 구간 A/B 테스트", "priority": "medium", "owner_suggestion": "Dev", "deadline_days": 7}
    ]
    if analysis_type == "trend":
        actions[0]["priority"] = "medium"
    actions = actions[:5]

    return {
        "executive_summary": executive_summary,
        "trend_analysis": trend_analysis,
        "hypotheses": hypotheses[:3],
        "risks": risks[:3],
        "actions": actions
    }


def _extract_json_candidate(text: str) -> str:
    t = (text or "").strip()
    if "```" in t:
        t = re.sub(r"```json|```", "", t).strip()
    # 가장 바깥 JSON 후보 추출
    start_obj = t.find("{")
    start_arr = t.find("[")
    starts = [x for x in [start_obj, start_arr] if x >= 0]
    if not starts:
        return t
    start = min(starts)
    end_obj = t.rfind("}")
    end_arr = t.rfind("]")
    end = max(end_obj, end_arr)
    if end >= start:
        return t[start:end + 1]
    return t


def _llm_json_response(system_prompt: str, user_prompt: str, temperature: float = 0.0):
    try:
        import openai
        res = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=temperature
        )
        content = res["choices"][0]["message"]["content"]
        parsed = json.loads(_extract_json_candidate(content))
        return parsed
    except Exception as e:
        logging.warning(f"[Notion LLM JSON] fallback due to: {e}")
        return None


def _llm_planner(pre: Dict[str, Any]) -> Dict[str, Any]:
    schema_hint = {
        "analysis_type": "trend|risk|segment",
        "core_kpis": ["string", "string", "string"],
        "highlight_points": ["string", "string", "string"],
        "key_questions": ["string", "string", "string"]
    }
    system_prompt = (
        "You are a planning model. Return only valid JSON. "
        "No markdown. No explanations. Keep Korean phrasing where possible."
    )
    user_prompt = (
        "다음 전처리 JSON을 보고 분석 계획 JSON을 생성하세요.\n"
        f"필수 스키마:\n{json.dumps(schema_hint, ensure_ascii=False)}\n"
        "제약: 모든 리스트 길이는 정확히 3.\n"
        f"입력:\n{json.dumps(pre, ensure_ascii=False)}"
    )
    out = _llm_json_response(system_prompt, user_prompt, temperature=0.0)
    if not isinstance(out, dict):
        return _build_report_planner(pre)
    # 최소 보정
    out.setdefault("analysis_type", "trend")
    out["analysis_type"] = out["analysis_type"] if out["analysis_type"] in ["trend", "risk", "segment"] else "trend"
    for k in ["core_kpis", "highlight_points", "key_questions"]:
        v = out.get(k, [])
        if not isinstance(v, list):
            v = []
        v = [str(x) for x in v[:3]]
        while len(v) < 3:
            v.append("추가 항목 필요")
        out[k] = v
    return out


def _llm_writer(pre: Dict[str, Any], planner: Dict[str, Any]) -> Dict[str, Any]:
    schema_hint = {
        "executive_summary": ["string", "string", "string"],
        "trend_analysis": [
            {"metric": "string", "current": 0, "prev": 0, "change_pct": 0.0}
        ],
        "hypotheses": ["string", "string", "string"],
        "risks": [{"risk": "string", "level": "high|medium|low"}],
        "actions": [{"action": "string", "priority": "high|medium|low", "owner_suggestion": "string", "deadline_days": 7}]
    }
    system_prompt = (
        "You are a report writer model. Fill data only. Return only valid JSON object."
    )
    user_prompt = (
        "아래 planner + preprocessed를 기반으로 리포트 객체 JSON을 작성하세요.\n"
        f"필수 스키마:\n{json.dumps(schema_hint, ensure_ascii=False)}\n"
        "제약: executive_summary 정확히 3개, hypotheses 정확히 3개, actions 최대 5개, risks 최대 3개.\n"
        f"planner:\n{json.dumps(planner, ensure_ascii=False)}\n"
        f"preprocessed:\n{json.dumps(pre, ensure_ascii=False)}"
    )
    out = _llm_json_response(system_prompt, user_prompt, temperature=0.1)
    if not isinstance(out, dict):
        return _build_report_object(pre, planner)
    return out


def _validate_report_object(obj: Dict[str, Any]) -> Tuple[bool, List[str]]:
    errs = []
    required = ["executive_summary", "trend_analysis", "hypotheses", "risks", "actions"]
    for k in required:
        if k not in obj:
            errs.append(f"missing:{k}")
        elif not isinstance(obj[k], list):
            errs.append(f"type:{k}:list")

    if isinstance(obj.get("executive_summary"), list) and len(obj["executive_summary"]) != 3:
        errs.append("len:executive_summary:3")
    if isinstance(obj.get("actions"), list) and len(obj["actions"]) > 5:
        errs.append("len:actions:max5")
    if isinstance(obj.get("risks"), list) and len(obj["risks"]) > 3:
        errs.append("len:risks:max3")
    return len(errs) == 0, errs


def _repair_report_object(obj: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(obj, dict):
        obj = {}
    obj.setdefault("executive_summary", [])
    obj.setdefault("trend_analysis", [])
    obj.setdefault("hypotheses", [])
    obj.setdefault("risks", [])
    obj.setdefault("actions", [])

    if not isinstance(obj["executive_summary"], list):
        obj["executive_summary"] = []
    while len(obj["executive_summary"]) < 3:
        obj["executive_summary"].append("추가 요약 포인트를 수집 중입니다.")
    obj["executive_summary"] = [str(x) for x in obj["executive_summary"][:3]]

    obj["trend_analysis"] = obj["trend_analysis"] if isinstance(obj["trend_analysis"], list) else []
    obj["hypotheses"] = [str(x) for x in (obj["hypotheses"] if isinstance(obj["hypotheses"], list) else [])][:3]

    if not isinstance(obj["risks"], list):
        obj["risks"] = []
    normalized_risks = []
    for r in obj["risks"][:3]:
        if isinstance(r, dict):
            normalized_risks.append({
                "risk": str(r.get("risk", "리스크 점검 필요")),
                "level": str(r.get("level", "medium"))
            })
        else:
            normalized_risks.append({"risk": str(r), "level": "medium"})
    if not normalized_risks:
        normalized_risks = [{"risk": "리스크 정보 부족", "level": "low"}]
    obj["risks"] = normalized_risks

    if not isinstance(obj["actions"], list):
        obj["actions"] = []
    normalized_actions = []
    for a in obj["actions"][:5]:
        if isinstance(a, dict):
            normalized_actions.append({
                "action": str(a.get("action", "액션 정의 필요")),
                "priority": str(a.get("priority", "medium")),
                "owner_suggestion": str(a.get("owner_suggestion", "TBD")),
                "deadline_days": int(_safe_float(a.get("deadline_days", 7)) or 7)
            })
    if not normalized_actions:
        normalized_actions = [{
            "action": "핵심 KPI 하락 원인 재분석",
            "priority": "high",
            "owner_suggestion": "Data",
            "deadline_days": 3
        }]
    obj["actions"] = normalized_actions
    return obj


def _report_object_to_notion_markdown(
    title: str,
    pre: Dict[str, Any],
    planner: Dict[str, Any],
    report_obj: Dict[str, Any]
) -> str:
    doc_title = title.strip() if title else "Untitled Report"
    today = pd.Timestamp.now().strftime('%Y-%m-%d')

    lines = [f"# {doc_title}", ""]
    lines += [
        "## 1) 리포트 아카이브",
        f"- 작성일: {today}",
        "- 작성자: ",
        "- 데이터 소스: GA4 / File / Mixed",
        "- 분석 기간: ",
        "- 리포트 버전: v2 (planner/writer-json)",
        ""
    ]

    lines += ["## 2) KPI 변화 추적"]
    for g in pre.get("growth_rates", [])[:8]:
        cp = g.get("change_pct", 0.0)
        metric = g.get("metric", "metric")
        lines.append(f"- {metric}: {cp:+.1f}%")
    if len(lines) > 0 and lines[-1] == "## 2) KPI 변화 추적":
        lines.pop()  # 데이터 없으면 섹션 제외
    else:
        lines.append(f"- 비교 기준: {planner.get('comparison_basis', pre.get('comparison_basis', '평균 대비'))}")
    lines.append("")

    # 구성비 섹션
    comp = pre.get("composition", {}) or {}
    top3 = comp.get("top3_concentration_pct", 0.0)
    top5 = comp.get("top5_concentration_pct", 0.0)
    if top3 > 0 or top5 > 0:
        lines += ["## 3) 구성비/집중도"]
        lines.append(f"- 상위 3개 집중도: {top3:.1f}%")
        lines.append(f"- 상위 5개 집중도: {top5:.1f}%")
        item_shares = comp.get("item_share_pct", [])[:5]
        for it in item_shares:
            lines.append(f"- TOP {it.get('rank')}: {it.get('share_pct', 0):.1f}%")
        lines.append("")

    # 리스크 탐지 섹션
    risk_flags = pre.get("risk_flags", [])
    if risk_flags:
        lines += ["## 4) 집중 리스크 탐지(코드 기반)"]
        for r in risk_flags[:5]:
            if isinstance(r, dict):
                lines.append(f"- [{str(r.get('level','medium')).upper()}] {r.get('risk','')}")
        lines.append("")

    lines += [
        "## 5) 판단 기록 (Decision Log)",
        "| 날짜 | 관찰/근거 | 판단 | 영향도 | 담당 |",
        "|---|---|---|---|---|",
        "|  |  |  |  |  |",
        ""
    ]

    lines += [
        "## 6) 실행 관리 (Action Tracker)",
        "| 실행 항목 | 목적 KPI | 오너 | 기한 | 상태 | 결과 |",
        "|---|---|---|---|---|---|",
    ]
    for a in report_obj.get("actions", [])[:5]:
        lines.append(
            f"| {a.get('action','')} | {', '.join(planner.get('core_kpis', [])[:2])} | {a.get('owner_suggestion','')} | D+{a.get('deadline_days',7)} | Todo |  |"
        )
    lines.append("")

    lines += ["## 7) Executive Summary"]
    for s in report_obj.get("executive_summary", [])[:3]:
        lines.append(f"- {s}")
    lines.append("")

    if report_obj.get("trend_analysis"):
        lines += ["## 8) Trend Analysis"]
        lines += ["| Metric | Current | Prev | Change % |", "|---|---:|---:|---:|"]
        for t in report_obj.get("trend_analysis", [])[:10]:
            cur = "" if t.get("current") is None else f"{_safe_float(t.get('current')):,.0f}"
            prev = "" if t.get("prev") is None else f"{_safe_float(t.get('prev')):,.0f}"
            chg = f"{_safe_float(t.get('change_pct')):+.1f}%"
            lines.append(f"| {t.get('metric','')} | {cur} | {prev} | {chg} |")
        lines.append("")

    if report_obj.get("hypotheses"):
        lines += ["## 9) Hypotheses"]
        for h in report_obj.get("hypotheses", [])[:3]:
            lines.append(f"1. {h}")
        lines.append("")

    if report_obj.get("risks"):
        lines += ["## 10) Risks"]
        for r in report_obj.get("risks", [])[:3]:
            lines.append(f"- [{str(r.get('level','medium')).upper()}] {r.get('risk','')}")
        lines.append("")

    lines += ["## 11) Planner 메타"]
    lines.append(f"- 분석 타입: {planner.get('analysis_type','trend')}")
    lines.append(f"- 핵심 KPI: {', '.join(planner.get('core_kpis', []))}")
    lines.append(f"- 비교 기준: {planner.get('comparison_basis', pre.get('comparison_basis', '평균 대비'))}")
    lines.append("- 리포트 질문 3개:")
    for q in planner.get("key_questions", [])[:3]:
        lines.append(f"  - {q}")
    lines.append("")

    if pre.get("source_lines"):
        lines += ["## 12) 원본 분석 스냅샷"]
        for ln in pre.get("source_lines", [])[:30]:
            lines.append(f"- {ln}")
    return "\n".join(lines).strip() + "\n"


def _build_notion_export_payload(title: str, html_content: str) -> Dict[str, Any]:
    pre = _preprocess_report_data(title, html_content)
    planner = _llm_planner(pre)
    report_obj = _llm_writer(pre, planner)
    ok, errs = _validate_report_object(report_obj)
    if not ok:
        report_obj = _repair_report_object(report_obj)
        ok2, errs2 = _validate_report_object(report_obj)
        errs = errs + ([] if ok2 else errs2)

    markdown = _report_object_to_notion_markdown(title, pre, planner, report_obj)
    return {
        "preprocessed": pre,
        "planner": planner,
        "report_object": report_obj,
        "validation_errors": errs,
        "markdown": markdown
    }


def _build_jandi_export_payload(title: str, html_content: str) -> Dict[str, Any]:
    """
    JANDI 공유용 요약 payload 생성.
    - 짧은 실행 요약만 제공 (노션 형식 리포트와 명확히 분리)
    """
    notion_payload = _build_notion_export_payload(title, html_content)
    pre = notion_payload.get("preprocessed", {}) or {}
    planner = notion_payload.get("planner", {}) or {}
    report_obj = notion_payload.get("report_object", {}) or {}

    executive = [str(x) for x in (report_obj.get("executive_summary") or [])[:3]]
    actions = report_obj.get("actions") or []
    risks = report_obj.get("risks") or []
    basis = planner.get("comparison_basis", pre.get("comparison_basis", "평균 대비"))

    lines = [f"[요약] {title}"]
    if basis:
        lines.append(f"- 비교 기준: {basis}")
    for s in executive:
        lines.append(f"- {s}")
    if actions:
        top_action = actions[0]
        lines.append(f"- 액션: {top_action.get('action', '')} ({top_action.get('priority', '')})")
    if risks:
        top_risk = risks[0]
        lines.append(f"- 리스크: {top_risk.get('risk', '')} [{str(top_risk.get('level', '')).upper()}]")

    connect_info = []
    for i, s in enumerate(executive[:2], 1):
        connect_info.append({"title": f"핵심 요약 {i}", "description": s})
    for i, a in enumerate(actions[:1], 1):
        connect_info.append({
            "title": f"실행 {i}",
            "description": f"{a.get('action', '')} | {a.get('owner_suggestion', '')} | D+{a.get('deadline_days', '')}"
        })

    return {
        "title": title,
        "summary_text": "\n".join(lines),
        "jandi_payload": {
            "body": "\n".join(lines),
            "connectColor": "#0F9D58",
            "connectInfo": connect_info[:3]
        },
        # 노션 상세 payload는 별도 엔드포인트(/export_report/notion)에서만 사용
        "notion_payload": {}
    }

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)
flask_env = os.getenv("FLASK_ENV", "development").lower()
secret_key = os.getenv("FLASK_SECRET_KEY", "").strip()
if flask_env == "production" and not secret_key:
    raise RuntimeError("FLASK_SECRET_KEY must be set in production.")
app.secret_key = secret_key or os.urandom(24)
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SECURE"] = _env_bool("SESSION_COOKIE_SECURE", flask_env == "production")
app.config["SESSION_COOKIE_SAMESITE"] = os.getenv("SESSION_COOKIE_SAMESITE", "Lax")
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(hours=int(os.getenv("SESSION_LIFETIME_HOURS", "24")))
app.config["MAX_CONTENT_LENGTH"] = int(os.getenv("MAX_CONTENT_LENGTH_MB", "50")) * 1024 * 1024
UPLOAD_FOLDER = os.getenv("UPLOAD_FOLDER", "uploaded_files")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
try:
    retention_days = int(os.getenv("LEARNING_RETENTION_DAYS", "180"))
    deleted = DBManager.prune_old_interactions(retention_days=max(1, retention_days))
    if deleted:
        logging.info(f"[Startup] Pruned old interaction logs: {deleted} rows")
except Exception as e:
    logging.warning(f"[Startup] Failed to prune old interaction logs: {e}")


@app.before_request
def _set_session_permanent():
    session.permanent = True

# 통합 데이터셋 저장용 변수
integrated_datasets = {}


@app.route('/healthz', methods=['GET'])
def healthz():
    return jsonify({"ok": True, "service": "my_project", "env": flask_env}), 200

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    filename = secure_filename(file.filename)
    filepath = os.path.join(UPLOAD_FOLDER, filename)
    file.save(filepath)
    session['uploaded_file_path'] = filepath  # 세션에 저장
    selected = session.get('selected_datasets') or []
    if filename not in selected:
        selected.append(filename)
        session['selected_datasets'] = selected

    return jsonify({"message": "File uploaded successfully", "file_path": filepath})



if flask_env != "production":
    os.environ.setdefault("OAUTHLIB_INSECURE_TRANSPORT", "1")

google_creds = _load_google_web_creds()
GOOGLE_CLIENT_ID = google_creds["client_id"]
GOOGLE_CLIENT_SECRET = google_creds["client_secret"]
GOOGLE_DISCOVERY_URL = "https://accounts.google.com/.well-known/openid-configuration"

client = WebApplicationClient(GOOGLE_CLIENT_ID)


def _oauth_redirect_uri() -> str:
    """
    OAuth redirect URI resolver.
    Priority:
      1) OAUTH_REDIRECT_URI (explicit full URI)
      2) url_for('callback', _external=True)
    """
    explicit = os.getenv("OAUTH_REDIRECT_URI", "").strip()
    if explicit:
        # allow host-only value and normalize to callback endpoint
        try:
            p = urlparse(explicit)
            if p.scheme and p.netloc:
                path = p.path or ""
                if path in {"", "/"}:
                    p = p._replace(path="/oauth2callback")
                    return urlunparse(p)
        except Exception:
            pass
        if explicit.endswith("/"):
            return explicit.rstrip("/") + "/oauth2callback"
        return explicit
    return url_for('callback', _external=True)


def get_google_provider_cfg():
    return requests.get(GOOGLE_DISCOVERY_URL).json()

def fetch_accounts(credentials):
    analytics = googleapiclient.discovery.build('analyticsadmin', 'v1beta', credentials=credentials)
    accounts = analytics.accounts().list().execute()
    return accounts

def fetch_properties(analytics, account_id):
    properties = analytics.properties().list(filter=f'parent:accounts/{account_id}').execute()
    return properties

@app.route("/")
def index():
    if 'credentials' not in session:
        app.logger.info("No credentials in session. Redirecting to login.")
        return redirect(url_for('login'))
    app.logger.info("Credentials found in session. Serving index page.")
    return send_from_directory('static', 'index.html')

@app.route("/login")
def login():
    google_provider_cfg = get_google_provider_cfg()
    authorization_endpoint = google_provider_cfg["authorization_endpoint"]
    redirect_uri = _oauth_redirect_uri()

    request_uri = client.prepare_request_uri(
        authorization_endpoint,
        redirect_uri=redirect_uri,
        scope=["openid", "email", "profile", "https://www.googleapis.com/auth/analytics.readonly"],
    )
    app.logger.info(f"Redirecting to: {request_uri}")
    return redirect(request_uri)

@app.route("/oauth2callback")
def callback():
    code = request.args.get("code")
    google_provider_cfg = get_google_provider_cfg()
    token_endpoint = google_provider_cfg["token_endpoint"]
    redirect_uri = _oauth_redirect_uri()

    token_url, headers, body = client.prepare_token_request(
        token_endpoint,
        authorization_response=request.url,
        redirect_url=redirect_uri,
        code=code
    )
    app.logger.info(f"Token URL: {token_url}")
    app.logger.info(f"Headers: {headers}")
    app.logger.info(f"Body: {body}")

    token_response = requests.post(
        token_url,
        headers=headers,
        data=body,
        auth=(GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET),
    )
    token_response_data = token_response.json()
    
    if 'error' in token_response_data:
        app.logger.error(f"Error in token response: {token_response_data['error']}")
        return jsonify({"error": "Authentication failed"}), 400

    # 토큰 정보를 클라이언트에 파싱 (add_token 호출 전 필수)
    client.parse_request_body_response(json.dumps(token_response_data))

    # 유저 정보 가져오기 (Email을 user_id로 사용)
    userinfo_endpoint = google_provider_cfg["userinfo_endpoint"]
    uri, headers, body = client.add_token(userinfo_endpoint)
    userinfo_response = requests.get(uri, headers=headers, data=body)
    user_data = userinfo_response.json()
    
    session['user_id'] = user_data["email"]
    session['user_name'] = user_data.get("name", "User")

    session['credentials'] = {
        'token': token_response_data['access_token'],
        'refresh_token': token_response_data.get('refresh_token'),
        'token_uri': token_endpoint,
        'client_id': GOOGLE_CLIENT_ID,
        'client_secret': GOOGLE_CLIENT_SECRET,
        'scopes': ['openid', 'email', 'profile', 'https://www.googleapis.com/auth/analytics.readonly']
    }
    
    # 첫 접속 시 conversation_id 생성
    if 'conversation_id' not in session:
        import uuid
        session['conversation_id'] = str(uuid.uuid4())
        # DB에 초기 레코드 생성
        from db_manager import DBManager
        DBManager.save_conversation_record(
            session['conversation_id'], 
            session['user_id'], 
            session.get("property_id"), 
            session.get("preprocessed_data_path") or session.get("uploaded_file_path")
        )

    app.logger.info(f"User {session['user_id']} logged in. Session: {session['conversation_id']}")
    return redirect(url_for("index"))

def ensure_new_conversation():
    """데이터 구성이 바뀌면 새 대화 세션 발급"""
    import uuid
    from db_manager import DBManager
    old_id = session.get('conversation_id')
    new_id = str(uuid.uuid4())
    session['conversation_id'] = new_id
    
    property_id = session.get("property_id")
    file_path = session.get("preprocessed_data_path") or session.get("uploaded_file_path")
    
    DBManager.save_conversation_record(new_id, session.get('user_id'), property_id, file_path)
    app.logger.info(f"Conversation rotated: {old_id} -> {new_id}")

def refresh_credentials(credentials):
    request_ = google.auth.transport.requests.Request()
    if credentials.expired and credentials.refresh_token:
        credentials.refresh(request_)
    return credentials

def get_traffic_data(dimensions, metrics, start_date, end_date, property_id):
    if 'credentials' not in session:
        raise ValueError("GA4 credentials not found in session")

    credentials = google.oauth2.credentials.Credentials(**session['credentials'])
    credentials = refresh_credentials(credentials)
    session['credentials']['token'] = credentials.token

    client = BetaAnalyticsDataClient(credentials=credentials)
    req = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name=d["name"]) for d in dimensions],
        metrics=[Metric(name=m["name"]) for m in metrics],
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
    )
    resp = client.run_report(req)

    rows = []
    for row in resp.rows:
        item = {}
        for i, d in enumerate(dimensions):
            item[d["name"]] = row.dimension_values[i].value
        for i, m in enumerate(metrics):
            raw = row.metric_values[i].value
            try:
                item[m["name"]] = float(raw)
            except (TypeError, ValueError):
                item[m["name"]] = raw
        rows.append(item)

    if not rows:
        columns = [d["name"] for d in dimensions] + [m["name"] for m in metrics]
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(rows)

@app.route("/list_all")
def list_all():
    try:
        if 'credentials' not in session:
            app.logger.error("No credentials in session. Redirecting to login.")
            return redirect(url_for('login'))

        credentials = google.oauth2.credentials.Credentials(**session['credentials'])
        credentials = refresh_credentials(credentials)

        analytics = googleapiclient.discovery.build('analyticsadmin', 'v1beta', credentials=credentials)
        accounts = fetch_accounts(credentials)
        account_data = [
            {'id': account['name'].split('/')[1], 'name': account['displayName']}
            for account in accounts.get('accounts', [])
        ]

        return jsonify(account_data)
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        return jsonify({"error": f"Failed to fetch data: {e}"}), 500

@app.route("/list_properties")
def list_properties():
    try:
        if 'credentials' not in session:
            app.logger.error("No credentials in session. Redirecting to login.")
            return redirect(url_for('login'))

        account_id = request.args.get('accountId')
        if not account_id:
            return jsonify({"error": "Account ID is required"}), 400

        credentials = google.oauth2.credentials.Credentials(**session['credentials'])
        credentials = refresh_credentials(credentials)

        analytics = googleapiclient.discovery.build('analyticsadmin', 'v1beta', credentials=credentials)
        properties = fetch_properties(analytics, account_id)
        property_data = [
            {'id': prop['name'].split('/')[1], 'name': prop['displayName']}
            for prop in properties.get('properties', [])
        ]

        return jsonify(property_data)
    except Exception as e:
        logging.error(f"Error fetching properties: {e}")
        return jsonify({"error": f"Failed to fetch properties: {e}"}), 500

@app.route("/set_property", methods=["POST"])
def set_property():
    data = request.get_json()

    property_id = data.get("property_id")
    property_name = data.get("property_name")

    if not property_id or not property_name:
        return jsonify({"error": "Property ID and name are required"}), 400

    old_prop = session.get('property_id')
    session['property_id'] = property_id
    session['property_name'] = property_name
    session["active_source"] = "ga4"
    
    if old_prop != property_id:
        ensure_new_conversation()

    # ✅ DB에 컨텍스트 저장
    DBManager.save_conversation_context(session.get('conversation_id'), {
        "active_source": "ga4",
        "property_id": property_id,
        "file_path": session.get("preprocessed_data_path") or session.get("uploaded_file_path")
    })

    return jsonify({
        "success": True, 
        "property_id": property_id, 
        "conversation_id": session.get('conversation_id')
    })


@app.route("/logout")
def logout():
    session.clear()
    app.logger.info("Session cleared. Redirecting to index.")
    return redirect(url_for("index"))
@app.route('/autocomplete')
def autocomplete():
    query = request.args.get('query')
    suggestions = get_suggestions(query)
    return jsonify(suggestions)

def get_suggestions(query):
    all_questions = [
        "총 사용자 수가 얼마나 되나요?",
        "활성 사용자는 얼마인가요?",
        "사용자는 얼마나 들어오나요?",
        "페이지뷰가 얼마나 되나요?",
        "조회수는 얼마나 되나요?",
        "평균 세션시간은 어떻게 되나요?",
        "이탈률이 어떻게 되나요?",
        "가장 인기 있는 페이지는 무엇인가요?",
        "신규 사용자가 몇 명인가요?",
        "가장 많은 트래픽을 보내는 소스는 무엇인가요?",
        "디바이스별 사용자 수는 얼마나 되나요?"
    ]
    return [q for q in all_questions if query.lower() in q.lower()]

#GET요청용 API엔드포인트이다. 브라우저나 프론트에서 GET요청을 보내면 이 함수가 실행된다.
@app.route("/traffic")
def traffic():
    question = request.args.get("question")
    property_id = request.args.get("propertyId")
    
    if not question:
        return jsonify({"error": "Question is required"}), 400
    if not property_id:
        return jsonify({"error": "Property ID is required"}), 400

    try:
        logging.info(f"[Traffic] Question: {question}, Prop: {property_id}")
        response = handle_question(
            question,
            property_id=property_id,
            conversation_id=session.get("conversation_id"),
            user_id=session.get("user_id", "anonymous"),
            semantic=semantic
        )


        # [P0] Sanitize
        return jsonify(sanitize(response))
    except Exception as e:
        logging.error(f"Error occurred: {e}")
        return jsonify({"error": f"Failed to process traffic request: {e}"}), 500

@app.route("/visualize")
def visualize():
    try:
        graph_type = request.args.get("type")
        logging.info(f"[Visualize] Request Type: {graph_type}")

        if not graph_type:
            return jsonify({"error": "Graph type is required"}), 400

        last_response = session.get('last_response')
        if not last_response:
            logging.warning("[Visualize] No last_response in session")
            return jsonify({"error": "No data available for visualization"}), 400

        # [Fix] Handle nested 'response' key from handle_question structure
        if isinstance(last_response, dict) and 'response' in last_response:
            data = last_response['response'].get('plot_data')
        else:
            data = last_response.get('plot_data')

        # [P0] Allow dict or list & Sanitize
        if not isinstance(data, (list, dict)):
            logging.error(f"[Visualize] Invalid plot_data type: {type(data)}")
            return jsonify({"error": "Plot data is not a list or dict"}), 400
        if 'response' in last_response:
            if last_response['response'].get("status") == "clarify":
                return jsonify({"error": "Clarify 상태에서는 시각화할 수 없습니다."}), 400

        # [Fix] Sanitize before sanitize
        data = sanitize(data)
        
        # Ensure list wrapping if dict (for client compatibility if needed, safely)
        # If client expects list, wrap it. ApexCharts often handles both but list is safer for series.
        # But 'data' here might be the full config object or just series?
        # Usually it's the full config {type:..., labels:..., series:...}
        # If it's a dict, sanitize is done.
        
        logging.debug(f"[Visualize] Data: {str(data)[:200]}...") # Log summary
        plot_data = base64.b64encode(json.dumps(data).encode()).decode()
        return jsonify({"plot_data": plot_data})
    except Exception as e:
        logging.error(f"Error occurred: {e}")
        return jsonify({"error": f"Failed to visualize: {e}"}), 500



@app.route("/upload_data", methods=["POST"])
def upload_data():
    file = request.files['file']
    if not file:
        return jsonify({"error": "No file uploaded"}), 400

    df = pd.read_csv(file)
    session['uploaded_data'] = df.to_dict(orient='records')
    return jsonify({"success": True, "data": df.head().to_dict(orient='records')})
@app.route('/preprocess_data_preview', methods=['POST'])
def preprocess_data_preview():
    actions = request.json.get('actions', [])
    file_path = session.get('uploaded_file_path')

    if not file_path or not os.path.exists(file_path):
        return jsonify({"error": "No file uploaded or file not found"}), 400

    try:
        if file_path.endswith('.xlsx'):
            df = pd.read_excel(file_path)
        elif file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        else:
            return jsonify({"error": "Unsupported file type"}), 400

        for action in actions:
            if action['type'] == 'drop_column':
                if 'column' in action and action['column']:
                    df.drop(columns=[action['column']], inplace=True)
                else:
                    return jsonify({"error": "Column name for drop_column cannot be empty"}), 400
            elif action['type'] == 'rename_column':
                if 'old_name' in action and 'new_name' in action:
                    df.rename(columns={action['old_name']: action['new_name']}, inplace=True)
                else:
                    return jsonify({"error": "'old_name' and 'new_name' are required for renaming columns"}), 400
            elif action['type'] == 'filter_rows':
                df = df[df[action['column']].astype(str).str.contains(action['value'], na=False)]

        df.fillna('', inplace=True)
        columns = df.columns.tolist()
        data = df.to_dict(orient='records')

        return jsonify({'columns': columns, 'data': data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/preprocess_data', methods=['POST'])
def preprocess_data():
    actions = request.json.get('actions', [])
    file_path = session.get('uploaded_file_path')

    if not file_path or not os.path.exists(file_path):
        return jsonify({"error": "No file uploaded or file not found"}), 400

    try:
        if file_path.endswith('.xlsx'):
            df = pd.read_excel(file_path)
        elif file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        else:
            return jsonify({"error": "Unsupported file type"}), 400

        for action in actions:
            if action['type'] == 'drop_column':
                df.drop(columns=[action['column']], inplace=True)
            elif action['type'] == 'rename_column':
                if 'old_name' in action and 'new_name' in action:
                    df.rename(columns={action['old_name']: action['new_name']}, inplace=True)
                else:
                    return jsonify({"error": "'old_name' and 'new_name' are required for renaming columns"}), 400
            elif action['type'] == 'filter_rows':
                df = df[df[action['column']].astype(str).str.contains(action['value'], na=False)]

        df.fillna('', inplace=True)
        columns = df.columns.tolist()
        data = df.to_dict(orient='records')

        # 전처리된 데이터를 파일에 저장하고 파일 경로를 세션에 저장
        preprocessed_file_path = os.path.join(UPLOAD_FOLDER, f'preprocessed_{generate_unique_id()}.csv')
        df.to_csv(preprocessed_file_path, index=False)
        # [Fix] Key Consistency: Use 'preprocessed_data_path'
        session['preprocessed_data_path'] = preprocessed_file_path
        logging.info(f"[Preprocess] Saved to {preprocessed_file_path}, Session Key: preprocessed_data_path")

        return jsonify({'columns': columns, 'data': data, 'file_path': preprocessed_file_path})
    except Exception as e:
        logging.error(f"Error during preprocessing: {e}")
        return jsonify({"error": str(e)}), 500



@app.route('/save_preprocessed_data', methods=['POST'])
def save_preprocessed_data():
    dataset_name = request.json.get('dataset_name')
    data = request.json.get('data')

    if not dataset_name:
        return jsonify({"error": "Dataset name is required"}), 400

    if not data:
        return jsonify({"error": "No data to save"}), 400

    preprocessed_file_path = os.path.join(UPLOAD_FOLDER, f'{dataset_name}.csv')

    try:
        df = pd.DataFrame(data)
        df.to_csv(preprocessed_file_path, index=False)
        return jsonify({"message": "Dataset saved successfully", "dataset_name": dataset_name})
    except Exception as e:
        logging.error(f"Error saving preprocessed data: {e}")
        return jsonify({"error": str(e)}), 500



@app.route('/list_datasets', methods=['GET'])
def list_datasets():
    try:
        datasets = []

        # GA4 계정 목록 추가
        if 'credentials' in session:
            credentials = google.oauth2.credentials.Credentials(**session['credentials'])
            credentials = refresh_credentials(credentials)

            analytics = googleapiclient.discovery.build('analyticsadmin', 'v1beta', credentials=credentials)
            accounts = fetch_accounts(credentials)
            for account in accounts.get('accounts', []):
                account_id = account['name'].split('/')[1]
                properties = fetch_properties(analytics, account_id)
                for prop in properties.get('properties', []):
                    datasets.append({'type': 'GA4', 'name': f"{account['displayName']} - {prop['displayName']}", 'id': prop['name'].split('/')[1]})

        # 업로드한 파일 목록 추가
        uploaded_files = [f for f in os.listdir(UPLOAD_FOLDER) if os.path.isfile(os.path.join(UPLOAD_FOLDER, f))]
        for file in uploaded_files:
            datasets.append({'type': 'File', 'name': file, 'id': file})

        return jsonify(datasets)
    except Exception as e:
        logging.error(f"Error listing datasets: {e}")
        return jsonify({"error": f"Failed to list datasets: {e}"}), 500

@app.route('/select_dataset', methods=['POST'])
def select_dataset():
    dataset_names = request.json.get('dataset_names')
    if not dataset_names:
        return jsonify({"error": "Dataset names are required"}), 400

    session['selected_datasets'] = dataset_names
    return jsonify({"success": True, "dataset_names": dataset_names})

@app.route('/fetch_ga4_data', methods=['POST'])
def fetch_ga4_data():
    # GA4 데이터 가져오는 로직 추가
    property_id = session.get('property_id')
    if not property_id:
        return jsonify({"error": "GA4 property ID is not set"}), 400

    try:
        # GA4 데이터 가져오기 로직 구현
        data = get_traffic_data(
            dimensions=[{"name": "date"}], 
            metrics=[{"name": "activeUsers"}], 
            start_date='7daysAgo', 
            end_date='today', 
            property_id=property_id
        )
        dataset_name = f"GA4_{property_id}"
        integrated_datasets[dataset_name] = data
        return jsonify({"message": "GA4 data fetched successfully", "dataset_name": dataset_name})
    except Exception as e:
        logging.error(f"Error fetching GA4 data: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/fetch_other_api_data', methods=['POST'])
def fetch_other_api_data():
    api_endpoint = request.json.get('api_endpoint')
    if not api_endpoint:
        return jsonify({"error": "API endpoint is required"}), 400

    try:
        response = requests.get(api_endpoint)
        response.raise_for_status()
        data = response.json()
        dataset_name = f"API_{generate_unique_id()}"
        integrated_datasets[dataset_name] = pd.DataFrame(data)
        return jsonify({"message": "API data fetched successfully", "dataset_name": dataset_name})
    except Exception as e:
        logging.error(f"Error fetching API data: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/get_uploaded_data', methods=['GET'])
def get_uploaded_data():
    file_path = session.get('uploaded_file_path')
    if not file_path or not os.path.exists(file_path):
        return jsonify({"error": "No file uploaded or file not found"}), 400

    try:
        if file_path.endswith('.xlsx'):
            df = pd.read_excel(file_path)
        elif file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        else:
            return jsonify({"error": "Unsupported file type"}), 400

        df.fillna('', inplace=True)  # NaN 값을 빈 문자열로 대체
        columns = df.columns.tolist()
        data = df.to_dict(orient='records')
        return jsonify({'columns': columns, 'data': data})
    except Exception as e:
        logging.error(f"Error reading uploaded data: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/get_preprocessed_data', methods=['GET'])
def get_preprocessed_data():
    dataset_name = request.args.get('dataset_name')

    if not dataset_name:
        return jsonify({'error': 'Dataset name is required'}), 400

    try:
        dataset_name = unquote(dataset_name)
        dataset_path = os.path.join(UPLOAD_FOLDER, dataset_name)

        if not os.path.isfile(dataset_path):
            return jsonify({'error': 'Dataset not found'}), 404

        df = pd.read_csv(dataset_path)
        df = df.where(pd.notnull(df), None)  # NaN -> None
        data = df.to_dict(orient="records")

        # ✅ 세션에 파일 경로 저장
        old_path = session.get('preprocessed_data_path')
        session['preprocessed_data_path'] = dataset_path
        selected = session.get('selected_datasets') or []
        if dataset_name not in selected:
            selected.append(dataset_name)
            session['selected_datasets'] = selected
        session["active_source"] = "file"
        
        if old_path != dataset_path:
            ensure_new_conversation()

        # ✅ DB에 컨텍스트 저장
        DBManager.save_conversation_context(session.get('conversation_id'), {
            "active_source": "file",
            "property_id": session.get("property_id"),
            "file_path": dataset_path
        })

        return jsonify({'data': data})

    except Exception as e:
        app.logger.error(f'Unexpected error: {e}')
        return jsonify({'error': 'An unexpected error occurred', 'details': str(e)}), 500


@app.route('/ask_question', methods=['POST'])
def ask_question():
    try:
        data = request.get_json()
        question = (data.get('question') or "").strip()
        beginner_mode = bool(data.get("beginner_mode", False))
        session["beginner_mode"] = beginner_mode

        if not question:
            return jsonify({"error": "Question is required"}), 400

        # 추천 질문 번호 선택 지원 (예: "1번", "2")
        is_followup_selected = False
        selected_followup_text = None
        # 단, 소스 선택/파일 전환 확인 대기 중에는 번호를 followup으로 치환하지 않는다.
        has_pending_choice = bool(session.get("pending_source_choice") or session.get("pending_file_switch") or session.get("pending_clarify"))
        if not has_pending_choice:
            m = re.match(r"^\s*(\d+)\s*번?\s*$", question)
            if m:
                idx = int(m.group(1)) - 1
                suggestions = session.get("last_followup_suggestions") or []
                if 0 <= idx < len(suggestions):
                    question = suggestions[idx]
                    is_followup_selected = True
                    selected_followup_text = question
                    logging.info(f"[Ask] Followup option selected: {m.group(1)} -> {question}")
            else:
                suggestions = session.get("last_followup_suggestions") or []
                if question in suggestions:
                    is_followup_selected = True
                    selected_followup_text = question

        property_id = session.get("property_id")
        # 🔥 파일 경로 세션 연동 (전처리 우선)
        file_path = session.get("preprocessed_data_path") or session.get("uploaded_file_path")
        
        user_id = session.get("user_id", "anonymous")
        conversation_id = session.get("conversation_id")

        if not conversation_id:
            import uuid
            session['conversation_id'] = str(uuid.uuid4())
            conversation_id = session['conversation_id']

        # 사용자 부정 피드백(틀림/이상) 자동 수집
        if _is_negative_feedback_text(question):
            prev_question = session.get("last_user_question")
            prev_response = session.get("last_response")
            DBManager.mark_last_interaction_bad(
                conversation_id=conversation_id,
                note=f"auto_feedback: {question}"
            )
            DBManager.log_failure_feedback(
                user_id=user_id,
                conversation_id=conversation_id,
                feedback_text=question,
                target_question=prev_question,
                target_response=prev_response
            )
            # 피드백만 입력된 경우는 저장 확인 후 종료
            if _is_feedback_only_text(question):
                return jsonify({
                    "response": {
                        "message": "피드백을 저장했습니다. 어떤 질문에서 틀렸는지 이어서 알려주시면 바로 보정하겠습니다.",
                        "plot_data": []
                    },
                    "route": "system"
                })

        # 🔥 handle_question에 user_id와 conversation_id 추가 전달
        logging.info(f"[Ask] Question: {question}, Prop: {property_id}, File: {file_path}")
        
        response = handle_question(
            question,
            property_id=property_id,
            file_path=file_path,
            user_id=user_id,
            conversation_id=conversation_id,
            semantic=semantic,
            beginner_mode=bool(session.get("beginner_mode", False))
        )

        # 추천 질문 클릭 시 "매칭 실패"면 컨텍스트를 붙여 1회 자동 재시도
        if is_followup_selected and _is_no_data_or_no_match_response(response):
            prev_question = session.get("last_user_question") or ""
            rewritten = _rewrite_followup_with_context(selected_followup_text or question, prev_question)
            if rewritten and rewritten != question:
                logging.info(f"[Ask] Followup retry with context rewrite: {question} -> {rewritten}")
                response2 = handle_question(
                    rewritten,
                    property_id=property_id,
                    file_path=file_path,
                    user_id=user_id,
                    conversation_id=conversation_id,
                    semantic=semantic,
                    beginner_mode=bool(session.get("beginner_mode", False))
                )
                # 재시도 결과가 더 낫다면 교체
                if not _is_no_data_or_no_match_response(response2):
                    response = response2

        # GA 질문에서 미스매치/무응답일 때 표준 표현으로 1회 자동 재시도
        if _is_ga_no_match_response(response):
            ga_retry_q = _rewrite_ga_question_for_retry(question)
            if ga_retry_q and ga_retry_q != question:
                logging.info(f"[Ask] GA retry with normalized question: {question} -> {ga_retry_q}")
                response2 = handle_question(
                    ga_retry_q,
                    property_id=property_id,
                    file_path=file_path,
                    user_id=user_id,
                    conversation_id=conversation_id,
                    semantic=semantic,
                    beginner_mode=bool(session.get("beginner_mode", False))
                )
                if not _is_ga_no_match_response(response2):
                    response = response2

        response = _apply_bad_regression_guard(user_id=user_id, question=question, response=response)

        
        logging.info(f"[Ask] Response Keys: {response.keys() if isinstance(response, dict) else 'Not Dict'}")

        # 후속 질문 추천을 현재 상태에서 실행 가능한 문장으로 정제
        if isinstance(response, dict):
            r = response.get("route")
            b = response.get("response") if isinstance(response.get("response"), dict) else response
            if isinstance(b, dict):
                last_q_for_followup = session.get("last_user_question") or question
                b["followup_suggestions"] = _normalize_followups(
                    route=str(r or ""),
                    body=b,
                    current_question=question,
                    last_user_question=last_q_for_followup
                )
                if isinstance(response.get("response"), dict):
                    response["response"] = b
                else:
                    response = b

        # [P0] Session Save
        session['last_response'] = response

        # 후속 질문 추천 세션 저장
        followups = []
        route = None
        body = response
        if isinstance(response, dict):
            route = response.get("route")
            body = response.get("response") if isinstance(response.get("response"), dict) else response
            if isinstance(body, dict):
                f = body.get("followup_suggestions")
                if isinstance(f, list):
                    followups = [str(x) for x in f if str(x).strip()]
        session["last_followup_suggestions"] = followups

        # 학습/평가용 인터랙션 로그 저장
        interaction_id = None
        if isinstance(body, dict):
            plot_data = body.get("plot_data")
            has_plot = isinstance(plot_data, dict) and bool(plot_data.get("labels")) and bool(plot_data.get("series"))
            has_raw_data = isinstance(body.get("raw_data"), list) and len(body.get("raw_data")) > 0
            msg = str(body.get("message", ""))
            abstained = any(k in msg for k in ["없습니다", "모릅니다", "알 수 없습니다", "확인할 수 없습니다"])
            interaction_id = DBManager.log_interaction(
                user_id=user_id,
                conversation_id=conversation_id,
                route=route or "unknown",
                question=question,
                response=body,
                has_plot=has_plot,
                has_raw_data=has_raw_data,
                abstained=abstained
            )

        # 다음 피드백 연결을 위해 직전 질의/응답 저장
        session["last_user_question"] = question
        session["last_response"] = response

        # [P0] Sanitize Response (NaN -> None)
        sanitized_response = sanitize(response)
        # 응답에 데이터 소스 라벨 보강 (복수 연결 시에도 어떤 소스를 썼는지 표시)
        try:
            if isinstance(sanitized_response, dict) and isinstance(sanitized_response.get("response"), dict):
                body = sanitized_response["response"]
                r = str(sanitized_response.get("route") or "").lower()
                if not body.get("data_label"):
                    if r == "file":
                        fp = file_path or session.get("preprocessed_data_path") or session.get("uploaded_file_path")
                        body["data_label"] = f"FILE · {os.path.basename(fp)}" if fp else "FILE"
                    elif r in {"ga4", "ga4_followup"}:
                        prop_name = session.get("property_name") or (property_id or session.get("property_id") or "")
                        body["data_label"] = f"GA4 · {prop_name}" if prop_name else "GA4"
                    elif r == "mixed":
                        fp = file_path or session.get("preprocessed_data_path") or session.get("uploaded_file_path")
                        fn = os.path.basename(fp) if fp else "-"
                        prop_name = session.get("property_name") or (property_id or session.get("property_id") or "-")
                        body["data_label"] = f"MIXED · GA4:{prop_name} + FILE:{fn}"
                sanitized_response["response"] = body
        except Exception:
            pass
        if isinstance(sanitized_response, dict):
            sanitized_response["interaction_id"] = interaction_id
        return jsonify(sanitized_response)

    except Exception as e:
        logging.error(f"Error processing question: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/save_report', methods=['POST'])
def save_report():
    try:
        data = request.get_json()
        title = data.get('title', 'Untitled Report')
        content = data.get('content') # JSON structure of the report
        
        user_id = session.get('user_id', 'anonymous')
        conversation_id = session.get('conversation_id')
        
        if not content:
            return jsonify({"error": "Report content is required"}), 400
            
        success = DBManager.save_report(user_id, conversation_id, title, content)
        
        if success:
            return jsonify({"success": True, "message": "Report saved to database"})
        else:
            return jsonify({"error": "Failed to save report to database"}), 500
            
    except Exception as e:
        logging.error(f"Error saving report: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/list_reports', methods=['GET'])
def list_reports():
    try:
        user_id = session.get('user_id', 'anonymous')
        reports = DBManager.get_reports(user_id)
        return jsonify({"success": True, "reports": reports})
    except Exception as e:
        logging.error(f"Error listing reports: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/get_report/<int:report_id>', methods=['GET'])
def get_report(report_id):
    try:
        report = DBManager.get_report_by_id(report_id)
        if report:
            return jsonify({"success": True, "report": report})
        else:
            return jsonify({"error": "Report not found"}), 404
    except Exception as e:
        logging.error(f"Error getting report: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/export_report/notion', methods=['POST'])
def export_report_notion():
    """
    Notion import-friendly Markdown export.
    Input:
      - report_id (optional): load saved report
      - title/content (optional): export current report draft
    """
    try:
        data = request.get_json() or {}
        report_id = data.get("report_id")
        title = data.get("title", "Untitled Report")
        content = data.get("content", "")

        if report_id:
            report = DBManager.get_report_by_id(int(report_id))
            if not report:
                return jsonify({"error": "Report not found"}), 404
            title = report.get("title", title)
            content = report.get("content", content)

        if not content:
            return jsonify({"error": "Report content is required"}), 400
        if not isinstance(content, str):
            content = json.dumps(content, ensure_ascii=False)

        # 1차 구현: 전처리 -> planner -> writer(report object) -> validation -> notion 템플릿 렌더링
        payload = _build_notion_export_payload(title, content)
        markdown = payload["markdown"]
        filename = re.sub(r"[^\w\-. ]", "_", title).strip() or "report"
        filename = f"{filename}.md"

        return jsonify({
            "success": True,
            "format": "notion_markdown",
            "filename": filename,
            "markdown": markdown,
            "report_object": payload.get("report_object", {}),
            "planner": payload.get("planner", {}),
            "preprocessed": payload.get("preprocessed", {}),
            "validation_errors": payload.get("validation_errors", [])
        })
    except Exception as e:
        logging.error(f"Error exporting notion report: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route('/export_report/notion_webhook', methods=['POST'])
def export_report_notion_webhook():
    """
    Send notion-ready payload to user webhook.
    Input:
      - report_id (optional)
      - title/content (optional)
      - webhook_url (required, unless saved preset lookup is used in UI)
      - send (optional, default true)
    """
    try:
        data = request.get_json() or {}
        report_id = data.get("report_id")
        title = data.get("title", "Untitled Report")
        content = data.get("content", "")
        webhook_url = str(data.get("webhook_url") or "").strip()
        send_flag = bool(data.get("send", True))

        if report_id:
            report = DBManager.get_report_by_id(int(report_id))
            if not report:
                return jsonify({"error": "Report not found"}), 404
            title = report.get("title", title)
            content = report.get("content", content)

        if not content:
            return jsonify({"error": "Report content is required"}), 400
        if not isinstance(content, str):
            content = json.dumps(content, ensure_ascii=False)

        notion_payload = _build_notion_export_payload(title, content)
        body = {
            "title": title,
            "markdown": notion_payload.get("markdown", ""),
            "report_object": notion_payload.get("report_object", {}),
            "planner": notion_payload.get("planner", {}),
            "preprocessed": notion_payload.get("preprocessed", {}),
            "validation_errors": notion_payload.get("validation_errors", []),
            "format": "notion_markdown"
        }

        delivered = False
        delivery_error = None
        if send_flag:
            if not webhook_url or not _is_valid_http_url(webhook_url):
                return jsonify({
                    "success": False,
                    "error": "Valid webhook_url is required",
                    "payload": body
                }), 400
            resp = requests.post(webhook_url, json=body, timeout=12)
            delivered = 200 <= resp.status_code < 300
            if not delivered:
                delivery_error = f"Notion webhook failed: {resp.status_code} {resp.text[:300]}"

        return jsonify({
            "success": delivery_error is None,
            "format": "notion_webhook",
            "title": title,
            "payload": body,
            "delivered": delivered,
            "delivery_error": delivery_error
        }), (200 if delivery_error is None else 502)
    except Exception as e:
        logging.error(f"Error exporting notion webhook: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route('/export_report/jandi', methods=['POST'])
def export_report_jandi():
    """
    JANDI 공유용 요약 생성/전송.
    Input:
      - report_id (optional)
      - title/content (optional)
      - webhook_url (optional, 없으면 JANDI_WEBHOOK_URL env 사용)
      - send (optional, default true)
    """
    try:
        data = request.get_json() or {}
        report_id = data.get("report_id")
        title = data.get("title", "Untitled Report")
        content = data.get("content", "")
        webhook_url = (data.get("webhook_url") or os.getenv("JANDI_WEBHOOK_URL", "")).strip()
        send_flag = bool(data.get("send", True))

        if report_id:
            report = DBManager.get_report_by_id(int(report_id))
            if not report:
                return jsonify({"error": "Report not found"}), 404
            title = report.get("title", title)
            content = report.get("content", content)

        if not content:
            return jsonify({"error": "Report content is required"}), 400
        if not isinstance(content, str):
            content = json.dumps(content, ensure_ascii=False)

        payload = _build_jandi_export_payload(title, content)
        jandi_body = payload.get("jandi_payload", {})
        delivered = False
        delivery_error = None

        if send_flag:
            if not webhook_url:
                return jsonify({
                    "success": False,
                    "error": "JANDI webhook URL is required",
                    "summary_text": payload.get("summary_text", ""),
                    "jandi_payload": jandi_body
                }), 400

            resp = requests.post(webhook_url, json=jandi_body, timeout=12)
            delivered = 200 <= resp.status_code < 300
            if not delivered:
                delivery_error = f"JANDI webhook failed: {resp.status_code} {resp.text[:300]}"

        return jsonify({
            "success": delivery_error is None,
            "format": "jandi",
            "title": title,
            "summary_text": payload.get("summary_text", ""),
            "jandi_payload": jandi_body,
            "delivered": delivered,
            "delivery_error": delivery_error
        }), (200 if delivery_error is None else 502)
    except Exception as e:
        logging.error(f"Error exporting jandi report: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route('/webhook_presets', methods=['GET', 'POST'])
def webhook_presets():
    user_id = session.get("user_id", "anonymous")
    try:
        if request.method == 'GET':
            channel = request.args.get("channel", default=None, type=str)
            presets = DBManager.list_webhook_presets(user_id=user_id, channel=channel)
            return jsonify({"success": True, "presets": presets})

        body = request.get_json(silent=True) or {}
        channel = str(body.get("channel") or "").strip().lower()
        name = str(body.get("name") or "").strip()
        url = str(body.get("url") or "").strip()
        if channel not in {"notion", "jandi"}:
            return jsonify({"error": "channel must be notion or jandi"}), 400
        if not name:
            return jsonify({"error": "name is required"}), 400
        if not _is_valid_http_url(url):
            return jsonify({"error": "valid url is required"}), 400
        ok = DBManager.save_webhook_preset(user_id=user_id, channel=channel, name=name, url=url)
        if not ok:
            return jsonify({"error": "failed to save preset"}), 500
        presets = DBManager.list_webhook_presets(user_id=user_id, channel=channel)
        return jsonify({"success": True, "presets": presets})
    except Exception as e:
        logging.error(f"Error handling webhook presets: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route('/webhook_presets/<int:preset_id>', methods=['DELETE'])
def delete_webhook_preset(preset_id):
    user_id = session.get("user_id", "anonymous")
    try:
        ok = DBManager.delete_webhook_preset(user_id=user_id, preset_id=preset_id)
        if not ok:
            return jsonify({"error": "preset not found"}), 404
        return jsonify({"success": True, "deleted_id": int(preset_id)})
    except Exception as e:
        logging.error(f"Error deleting webhook preset: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route('/learning_status', methods=['GET'])
def learning_status():
    try:
        user_id = session.get("user_id", "anonymous")
        days = request.args.get("days", default=30, type=int)
        summary = DBManager.get_learning_status(user_id=user_id, days=max(1, min(days, 365)))
        return jsonify({"success": True, "user_id": user_id, "days": max(1, min(days, 365)), "summary": summary})
    except Exception as e:
        logging.error(f"Error getting learning status: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route('/learning_samples', methods=['GET'])
def learning_samples():
    try:
        user_id = session.get("user_id", "anonymous")
        limit = request.args.get("limit", default=50, type=int)
        samples = DBManager.get_recent_learning_samples(user_id=user_id, limit=limit)
        return jsonify({"success": True, "user_id": user_id, "count": len(samples), "samples": samples})
    except Exception as e:
        logging.error(f"Error getting learning samples: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route('/matching_status', methods=['GET'])
def matching_status():
    try:
        user_id = session.get("user_id", "anonymous")
        days = request.args.get("days", default=30, type=int)
        summary = DBManager.get_matching_status(user_id=user_id, days=max(1, min(days, 365)))
        return jsonify({"success": True, "user_id": user_id, "days": max(1, min(days, 365)), "summary": summary})
    except Exception as e:
        logging.error(f"Error getting matching status: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route('/admin/token_status', methods=['GET'])
def admin_token_status():
    if not _require_admin_token():
        return jsonify({"error": "unauthorized"}), 401
    try:
        status = {
            "flask_env": flask_env,
            "flask_secret_configured": bool(os.getenv("FLASK_SECRET_KEY", "").strip()),
            "openai_api_key_configured": bool(os.getenv("OPENAI_API_KEY", "").strip()),
            "admin_api_token_configured": bool(os.getenv("ADMIN_API_TOKEN", "").strip()),
            "google_client_id_configured": bool(GOOGLE_CLIENT_ID),
            "google_client_secret_configured": bool(GOOGLE_CLIENT_SECRET),
            "session_cookie_secure": bool(app.config.get("SESSION_COOKIE_SECURE")),
            "session_cookie_samesite": app.config.get("SESSION_COOKIE_SAMESITE"),
        }
        return jsonify({"success": True, "status": status})
    except Exception as e:
        logging.error(f"Error getting token status: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route('/admin/export_training_jsonl', methods=['GET'])
def admin_export_training_jsonl():
    if not _require_admin_token():
        return jsonify({"error": "unauthorized"}), 401
    try:
        days = request.args.get("days", default=30, type=int)
        limit = request.args.get("limit", default=int(os.getenv("TRAINING_EXPORT_MAX_ROWS", "5000")), type=int)
        include_abstained = request.args.get("include_abstained", default=0, type=int) == 1
        label_filter = request.args.get("label_filter", default=os.getenv("TRAINING_EXPORT_LABEL_FILTER", "good"), type=str)
        include_unlabeled = request.args.get("include_unlabeled", default=0, type=int) == 1
        user_id = request.args.get("user_id", default=None, type=str)
        examples = DBManager.export_training_examples(
            user_id=user_id,
            days=max(1, min(days, 3650)),
            limit=max(1, min(limit, 100000)),
            include_abstained=include_abstained,
            label_filter=label_filter,
            include_unlabeled=include_unlabeled
        )
        lines = [json.dumps(x, ensure_ascii=False) for x in examples]
        payload = "\n".join(lines) + ("\n" if lines else "")
        filename = f"training_examples_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
        return Response(
            payload,
            mimetype="application/x-ndjson",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except Exception as e:
        logging.error(f"Error exporting training jsonl: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route('/admin/label_status', methods=['GET'])
def admin_label_status():
    if not _require_admin_token():
        return jsonify({"error": "unauthorized"}), 401
    try:
        days = request.args.get("days", default=30, type=int)
        user_id = request.args.get("user_id", default=None, type=str)
        summary = DBManager.get_label_status(user_id=user_id, days=max(1, min(days, 3650)))
        return jsonify({"success": True, "days": max(1, min(days, 3650)), "summary": summary})
    except Exception as e:
        logging.error(f"Error getting label status: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route('/admin/regression_snapshot', methods=['GET'])
def admin_regression_snapshot():
    if not _require_admin_token():
        return jsonify({"error": "unauthorized"}), 401
    try:
        days = request.args.get("days", default=14, type=int)
        limit = request.args.get("limit", default=100, type=int)
        user_id = request.args.get("user_id", default=None, type=str)
        snap = DBManager.get_regression_snapshot(
            user_id=user_id,
            days=max(1, min(days, 3650)),
            limit=max(10, min(limit, 5000))
        )
        return jsonify({"success": True, "snapshot": snap})
    except Exception as e:
        logging.error(f"Error getting regression snapshot: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route('/admin/label_interaction', methods=['POST'])
def admin_label_interaction():
    if not _require_admin_token():
        return jsonify({"error": "unauthorized"}), 401
    try:
        body = request.get_json(silent=True) or {}
        interaction_id = body.get("interaction_id")
        label = body.get("label")
        note = body.get("note", "")
        if not interaction_id or not label:
            return jsonify({"error": "interaction_id and label are required"}), 400
        ok = DBManager.set_interaction_label(interaction_id=interaction_id, label=label, note=note)
        if not ok:
            return jsonify({"error": "failed to set label"}), 400
        return jsonify({"success": True, "interaction_id": int(interaction_id), "label": str(label).lower()})
    except Exception as e:
        logging.error(f"Error labeling interaction: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route('/label_interaction', methods=['POST'])
def label_interaction():
    """User-facing interaction label API (good/bad/unknown/unlabeled)."""
    try:
        body = request.get_json() or {}
        interaction_id = body.get("interaction_id")
        label = body.get("label")
        note = body.get("note")
        if not interaction_id or not label:
            return jsonify({"error": "interaction_id and label are required"}), 400
        ok = DBManager.set_interaction_label(interaction_id=interaction_id, label=label, note=note)
        if not ok:
            return jsonify({"error": "failed to set label"}), 400
        return jsonify({"success": True, "interaction_id": int(interaction_id), "label": str(label).lower()})
    except Exception as e:
        logging.error(f"Error labeling interaction(user): {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route('/admin/prune_learning_data', methods=['POST'])
def admin_prune_learning_data():
    if not _require_admin_token():
        return jsonify({"error": "unauthorized"}), 401
    try:
        body = request.get_json(silent=True) or {}
        retention_days = int(body.get("retention_days") or os.getenv("LEARNING_RETENTION_DAYS", "180"))
        deleted = DBManager.prune_old_interactions(retention_days=max(1, retention_days))
        return jsonify({"success": True, "deleted_rows": deleted, "retention_days": max(1, retention_days)})
    except Exception as e:
        logging.error(f"Error pruning learning data: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

# [PHASE 2] Block Editing API
@app.route('/edit_block', methods=['POST'])
def edit_block():
    """Edit a report block using AI with different modes"""
    try:
        data = request.get_json()
        text = data.get('text', '')
        mode = data.get('mode', 'concise')
        
        if not text:
            return jsonify({"error": "Text is required"}), 400
        
        # Define mode-specific prompts
        mode_prompts = {
            "concise": "다음 텍스트를 간결하게 재작성하세요. 핵심만 남기고 불필요한 문장은 제거하세요. 2-3줄 이내로 작성하세요.",
            "executive": "다음 텍스트를 임원 보고용으로 재작성하세요. 비즈니스 임팩트와 핵심 수치를 강조하세요. 전문적이고 간결하게 작성하세요.",
            "marketing": "다음 텍스트를 마케팅 자료용으로 재작성하세요. 긍정적이고 설득력 있게 작성하세요. 성과를 강조하세요.",
            "data-focused": "다음 텍스트를 데이터 중심으로 재작성하세요. 구체적인 수치와 통계를 강조하세요. 객관적으로 작성하세요."
        }
        
        prompt = mode_prompts.get(mode, mode_prompts["concise"])
        
        # Call LLM
        import openai
        res = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a professional content editor. Follow the instructions precisely."},
                {"role": "user", "content": f"{prompt}\n\n원본 텍스트:\n{text}"}
            ],
            temperature=0.3
        )
        
        edited_text = res['choices'][0]['message']['content'].strip()
        
        return jsonify({
            "success": True,
            "original": text,
            "edited": edited_text,
            "mode": mode
        })
        
    except Exception as e:
        logging.error(f"Error editing block: {e}")
        return jsonify({"error": str(e)}), 500



# [PHASE 3] Report-level Editing API
@app.route('/edit_report', methods=['POST'])
def edit_report():
    """Edit entire report using AI with structured block format (SAFE JSON MERGE VERSION)"""
    try:
        import json
        import re
        import uuid
        import openai

        data = request.get_json()
        blocks = data.get("blocks", [])
        instruction = data.get("instruction", "")

        if not blocks or not instruction:
            return jsonify({"error": "Blocks and instruction are required"}), 400

        # ------------------------------------------------------------------
        # 1) Ensure every block has a stable id (critical for safe merging)
        # ------------------------------------------------------------------
        normalized_blocks = []
        for b in blocks:
            if not isinstance(b, dict):
                continue

            block_id = b.get("id")
            if not block_id:
                block_id = str(uuid.uuid4())

            normalized_blocks.append({
                "id": block_id,
                "html": b.get("html", ""),
                "plotData": b.get("plotData"),
                "chartId": b.get("chartId"),
                "source": b.get("source"),
                "created_at": b.get("created_at")
            })

        if not normalized_blocks:
            return jsonify({"error": "No valid blocks found"}), 400

        # ------------------------------------------------------------------
        # 2) LLM Input should NOT include plotData (token waste + corruption risk)
        #    Only pass id + html to rewrite safely
        # ------------------------------------------------------------------
        llm_context = [
            {
                "id": b["id"],
                "html": b["html"]
            }
            for b in normalized_blocks
        ]

        context_json = json.dumps(llm_context, ensure_ascii=False, indent=2)

        prompt = f"""
다음은 데이터 분석 리포트 블록들입니다. (JSON 배열)

{context_json}

사용자 요청:
{instruction}

요청에 따라 각 블록의 "html"만 수정하세요.

반드시 아래 JSON 형식 그대로 반환하세요:

[
  {{
    "id": "...원본 id 그대로...",
    "html": "...수정된 html..."
  }}
]

주의:
- id는 절대 변경하지 마세요.
- 블록을 삭제하거나 새로 추가하지 마세요.
- JSON 배열만 반환하세요.
- 설명 문장 없이 JSON만 반환하세요.
"""

        res = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": "You are a professional report editor. Return ONLY valid JSON array. Do not add explanations."
                },
                {"role": "user", "content": prompt}
            ],
            temperature=0.2
        )

        response_text = res["choices"][0]["message"]["content"].strip()

        # ------------------------------------------------------------------
        # 3) Remove markdown fences if exists
        # ------------------------------------------------------------------
        if "```" in response_text:
            response_text = re.sub(r"```json|```", "", response_text).strip()

        edited_minimal = json.loads(response_text)

        if not isinstance(edited_minimal, list):
            return jsonify({"error": "LLM output is not a JSON array"}), 500

        # ------------------------------------------------------------------
        # 4) Build edited html map (id -> html)
        # ------------------------------------------------------------------
        edited_html_map = {}
        for item in edited_minimal:
            if not isinstance(item, dict):
                continue
            if "id" not in item or "html" not in item:
                continue
            edited_html_map[item["id"]] = item["html"]

        # ------------------------------------------------------------------
        # 5) Merge: preserve plotData/chartId/source/created_at
        # ------------------------------------------------------------------
        merged_blocks = []
        for b in normalized_blocks:
            block_id = b["id"]

            merged_blocks.append({
                "id": block_id,
                "html": edited_html_map.get(block_id, b.get("html", "")),
                "plotData": b.get("plotData"),
                "chartId": b.get("chartId"),
                "source": b.get("source"),
                "created_at": b.get("created_at")
            })

        return jsonify({
            "success": True,
            "blocks": merged_blocks
        })

    except Exception as e:
        logging.error(f"Error editing report: {e}")
        return jsonify({"error": str(e)}), 500


            
@app.route('/ask_csv', methods=['POST'])
def ask_csv():
    data = request.get_json()
    question = data.get("question")
    dataset_name = data.get("dataset_name")

    if not question or not dataset_name:
        return jsonify({"error": "question and dataset_name required"}), 400

    base_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(base_dir, UPLOAD_FOLDER, dataset_name)
    response = file_engine.process(question, file_path)

    return jsonify({"response": response, "route": "file"})


@app.route('/list_preprocessed_data', methods=['GET'])
def list_preprocessed_data():
    files = os.listdir('uploaded_files')
    return jsonify(files)

if __name__ == "__main__":
    debug_mode = os.getenv("FLASK_DEBUG", "0") == "1"
    port = int(os.getenv("PORT", "5001"))
    app.run(debug=debug_mode, host='0.0.0.0', port=port)
