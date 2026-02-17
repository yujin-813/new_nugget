# response_adapter.py
# Response Format Adapter
"""
새 파이프라인의 응답 형식을 기존 프론트엔드 형식으로 변환
"""

import logging
from typing import Dict, Any, List
import re


def _metric_unit(metric_key: str, category: str) -> str:
    mk = (metric_key or "").lower()
    # 비율형
    if "rate" in mk or "ratio" in mk:
        return "%"
    # 금액형
    if any(k in mk for k in ["revenue", "amount", "adspend", "tax", "shipping", "refund"]):
        return "원"
    # 사용자/구매자 수
    if any(k in mk for k in ["user", "visitor", "purchaser", "buyer"]):
        return "명"
    # 이벤트/세션/거래 건수
    if any(k in mk for k in ["session", "event", "transaction", "purchase"]):
        return "회"
    return ""


def _to_number(value: Any):
    if value is None:
        return None
    try:
        text = str(value).strip()
        text = re.sub(r"[^\d\.\-]", "", text)
        if text in ("", "-", ".", "-."):
            return None
        return float(text)
    except Exception:
        return None


def _format_value(value: Any, unit: str) -> str:
    num = _to_number(value)
    if num is None:
        text = str(value)
    else:
        if unit == "%" and 0 <= num <= 1:
            num *= 100
        if unit == "원":
            text = f"{round(num):,}"
        else:
            text = f"{num:,.0f}" if float(num).is_integer() else f"{num:,.2f}"
    return f"{text}{unit}" if unit else text


def _is_brief_request(question: str) -> bool:
    q = (question or "").lower()
    return any(k in q for k in ["한줄", "요약", "간단", "짧게", "brief"])


def _build_followups(question: str, has_breakdown: bool) -> List[str]:
    q = (question or "").lower()
    followups = []
    compare_tokens = ["비교", "대비", "증감", "차이", "vs"]
    if ("지난주" in q or "이번주" in q or "이번달" in q or "지난달" in q) and not any(t in q for t in compare_tokens):
        followups.append("이전 기간과 비교해 증감도 보여드릴까요?")
    if not has_breakdown:
        followups.append("채널별/디바이스별로 나눠서 볼까요?")
    topn_friendly = any(k in q for k in ["매출", "이벤트", "구매", "상품", "전환"])
    if topn_friendly and "top" not in q and "상위" not in q:
        followups.append("상위 항목 TOP 10으로 확장할까요?")
    if has_breakdown:
        followups.append("상위 항목의 원인 분석까지 이어서 볼까요?")
    return followups[:3]


def _topic_particle(text: str) -> str:
    if not text:
        return "는"
    ch = text[-1]
    code = ord(ch)
    if 0xAC00 <= code <= 0xD7A3:
        return "은" if ((code - 0xAC00) % 28) != 0 else "는"
    return "은"


def _summarize_top_item(row: Dict[str, Any]) -> str:
    if not isinstance(row, dict) or not row:
        return "상위 항목을 확인했습니다."

    dim_key = None
    dim_val = None
    metric_key = None
    metric_val = None

    for k, v in row.items():
        if dim_key is None and _to_number(v) is None and v not in (None, ""):
            dim_key, dim_val = k, v
        if metric_key is None and _to_number(v) is not None:
            metric_key, metric_val = k, v

    if dim_val is not None and metric_val is not None:
        unit = "원" if ("매출" in str(metric_key) or "revenue" in str(metric_key).lower()) else ""
        return f"{dim_val} ({_format_value(metric_val, unit)})"
    if dim_val is not None:
        return str(dim_val)
    return "상위 항목"


def _extract_plot_data(blocks: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not blocks:
        return []

    # 1) breakdown/trend list data -> category chart
    for block in blocks:
        rows = block.get("data")
        if not isinstance(rows, list) or not rows:
            continue
        if not isinstance(rows[0], dict):
            continue

        sample = rows[0]
        keys = list(sample.keys())
        if not keys:
            continue

        label_key = None
        for k in keys:
            if _to_number(sample.get(k)) is None:
                label_key = k
                break
        if label_key is None:
            label_key = keys[0]

        metric_keys = []
        for k in keys:
            numeric_count = 0
            for r in rows[:30]:
                if _to_number(r.get(k)) is not None:
                    numeric_count += 1
            if numeric_count >= max(1, min(len(rows[:30]), 3)):
                metric_keys.append(k)

        metric_keys = [k for k in metric_keys if k != label_key][:2]
        if not metric_keys:
            continue

        labels = [str(r.get(label_key, "")) for r in rows]
        series = []
        for mk in metric_keys:
            data = []
            for r in rows:
                num = _to_number(r.get(mk))
                data.append(num if num is not None else 0.0)
            series.append({"name": mk, "data": data})

        chart_type = "line" if block.get("type") == "trend" else "bar"

        # 추이 차트는 날짜 오름차순으로 정렬
        if chart_type == "line":
            pairs = list(zip(labels, *[s["data"] for s in series]))
            def _date_key(item):
                lb = item[0]
                if re.match(r"^\d{4}-\d{2}-\d{2}$", lb):
                    return lb
                return f"9999-{lb}"
            pairs.sort(key=_date_key)
            labels = [p[0] for p in pairs]
            for idx, s in enumerate(series):
                s["data"] = [p[idx + 1] for p in pairs]

        return {"type": chart_type, "labels": labels, "series": series}

    # 2) total dict data -> single bar chart
    for block in blocks:
        data = block.get("data")
        if not isinstance(data, dict) or not data:
            continue
        labels = []
        values = []
        for k, v in data.items():
            num = _to_number(v)
            if num is None:
                continue
            labels.append(str(k))
            values.append(num)
        if labels:
            return {"type": "bar", "labels": labels, "series": [{"name": "value", "data": values}]}

    return []


def _format_top_rows(data: List[Dict[str, Any]], max_rows: int = 10) -> List[str]:
    if not data:
        return []
    try:
        from ga4_metadata import GA4_METRICS, GA4_DIMENSIONS
    except Exception:
        GA4_METRICS, GA4_DIMENSIONS = {}, {}

    def _label(k: str) -> str:
        if k in GA4_METRICS:
            return GA4_METRICS[k].get("ui_name", k)
        if k in GA4_DIMENSIONS:
            return GA4_DIMENSIONS[k].get("ui_name", k)
        return k

    def _pretty(k: str, v: Any) -> Any:
        unit = ""
        lk = str(k).lower()
        if "rate" in lk or "ratio" in lk or "율" in str(k) or "비율" in str(k):
            unit = "%"
        elif "revenue" in lk or "매출" in str(k):
            unit = "원"
        elif any(t in lk for t in ["user", "purchaser", "buyer", "visitor"]):
            unit = "명"
        elif any(t in lk for t in ["event", "session", "transaction", "purchase"]):
            unit = "회"
        num = _to_number(v)
        if num is None:
            return v
        return _format_value(num, unit)

    rows = []
    has_custom = any(isinstance(r, dict) and any(str(k).startswith("customEvent:") for k in r.keys()) for r in data[:max_rows])
    for i, row in enumerate(data[:max_rows], 1):
        if not isinstance(row, dict):
            continue
        parts = []
        keys = list(row.keys())
        if has_custom:
            custom_keys = [k for k in keys if str(k).startswith("customEvent:")]
            non_custom_keys = [k for k in keys if k not in custom_keys]
            keys = non_custom_keys[:1] + custom_keys + non_custom_keys[1:]
        for k in keys:
            v = row.get(k)
            if isinstance(v, (dict, list)):
                continue
            parts.append(f"{_label(str(k))}: {_pretty(str(k), v)}")
            limit = 4 if has_custom else 2
            if len(parts) >= limit:
                break
        if parts:
            rows.append(f"{i}. " + " | ".join(parts))
    return rows


def _is_blank_like(value: Any) -> bool:
    v = str(value if value is not None else "").strip().lower()
    return v in {"", "(not set)", "not set", "unknown", "none", "null", "(none)"}


def _question_focus_dimension(question: str, rows: List[Dict[str, Any]]) -> str:
    if not rows or not isinstance(rows[0], dict):
        return ""
    keys = set(rows[0].keys())
    q = (question or "").lower()
    if any(k in q for k in ["메뉴", "menu", "gnb", "lnb"]) and "customEvent:menu_name" in keys:
        return "customEvent:menu_name"
    if any(k in q for k in ["후원명", "donation_name", "후원 유형", "후원유형"]) and "customEvent:donation_name" in keys:
        return "customEvent:donation_name"
    if any(k in q for k in ["스크롤", "scroll"]) and "customEvent:percent_scrolled" in keys:
        return "customEvent:percent_scrolled"
    if any(k in q for k in ["버튼", "button"]) and "customEvent:button_name" in keys:
        return "customEvent:button_name"
    return ""


def _clean_display_rows(rows: List[Dict[str, Any]], preferred_dim: str = "") -> List[Dict[str, Any]]:
    if not rows or not isinstance(rows[0], dict):
        return rows
    dim_key = preferred_dim or _select_filter_dimension(rows)
    if not dim_key:
        return rows
    normalized = [r.get(dim_key, "") for r in rows]
    has_real = any(not _is_blank_like(v) for v in normalized)
    if not has_real:
        return []
    cleaned = []
    for r in rows:
        if _is_blank_like(r.get(dim_key, "")):
            continue
        cleaned.append(r)
    return cleaned if cleaned else rows


def _build_data_quality_warning(question: str, rows: List[Dict[str, Any]]) -> str:
    if not rows or not isinstance(rows[0], dict):
        return ""
    focus_dim = _question_focus_dimension(question, rows)
    if not focus_dim:
        return ""
    if focus_dim not in rows[0]:
        return ""
    total = len(rows)
    valid = sum(1 for r in rows if not _is_blank_like(r.get(focus_dim)))
    if valid == 0:
        label = focus_dim.replace("customEvent:", "")
        return f"현재 기간에는 `{label}` 값이 수집되지 않았습니다. GA4 커스텀 정의/이벤트 전송을 점검해 주세요."
    ratio = valid / total if total else 0
    if ratio < 0.3:
        label = focus_dim.replace("customEvent:", "")
        return f"`{label}` 값의 유효 수집 비율이 낮습니다({valid}/{total}). 해석 시 주의가 필요합니다."
    return ""


def _build_dual_entity_compare_message(question: str, rows: List[Dict[str, Any]]) -> str:
    q = (question or "").lower()
    if not any(k in q for k in ["중", "어떤게", "어느", "많아", "더"]):
        return ""
    terms = _extract_entity_terms(question)
    if len(terms) < 2 or not rows:
        return ""
    sample = rows[0] if isinstance(rows[0], dict) else {}
    if not isinstance(sample, dict):
        return ""

    label_key = _select_filter_dimension(rows)
    metric_key = None
    for k, v in sample.items():
        if _to_number(v) is not None:
            metric_key = k
            break
    if not label_key or not metric_key:
        return ""

    totals = {terms[0]: 0.0, terms[1]: 0.0}
    for r in rows:
        label = str(r.get(label_key, ""))
        val = _to_number(r.get(metric_key)) or 0.0
        for t in totals:
            if t in label:
                totals[t] += val
                break
    if totals[terms[0]] == 0 and totals[terms[1]] == 0:
        return ""
    winner = terms[0] if totals[terms[0]] >= totals[terms[1]] else terms[1]
    unit = "원" if "revenue" in str(metric_key).lower() else ""
    return (
        f"{terms[0]} vs {terms[1]} 비교 결과, **{winner}**이(가) 더 큽니다.\n"
        f"- {terms[0]}: **{_format_value(totals[terms[0]], unit)}**\n"
        f"- {terms[1]}: **{_format_value(totals[terms[1]], unit)}**"
    )


def _build_domestic_overseas_message(question: str, rows: List[Dict[str, Any]]) -> str:
    q = (question or "").lower()
    if not ("해외" in q and "국내" in q):
        return ""
    if not rows or not isinstance(rows[0], dict):
        return ""
    country_key = None
    metric_key = None
    for k, v in rows[0].items():
        if country_key is None and "country" in str(k).lower() and _to_number(v) is None:
            country_key = k
        if metric_key is None and _to_number(v) is not None:
            metric_key = k
    if not country_key or not metric_key:
        return ""
    domestic = 0.0
    overseas = 0.0
    for r in rows:
        c = str(r.get(country_key, "")).lower()
        v = _to_number(r.get(metric_key)) or 0.0
        if c in ["south korea", "korea", "대한민국", "한국"]:
            domestic += v
        else:
            overseas += v
    if domestic == 0 and overseas == 0:
        return ""
    total = domestic + overseas
    d_pct = (domestic / total * 100) if total else 0
    o_pct = (overseas / total * 100) if total else 0
    unit = "원" if "revenue" in str(metric_key).lower() else ""
    return (
        "국내(대한민국) vs 해외(기타 국가) 비교입니다.\n"
        f"- 국내: **{_format_value(domestic, unit)}** ({d_pct:.1f}%)\n"
        f"- 해외: **{_format_value(overseas, unit)}** ({o_pct:.1f}%)"
    )


def _build_donation_type_conversion_message(question: str, rows: List[Dict[str, Any]]) -> str:
    q = (question or "").lower()
    if not (any(k in q for k in ["전환", "비율", "율"]) and any(k in q for k in ["클릭", "구매"])):
        return ""
    if not rows or not isinstance(rows[0], dict):
        return ""
    sample = rows[0]
    donation_key = next((k for k in sample.keys() if "is_regular_donation" in str(k).lower()), None)
    event_key = next((k for k in sample.keys() if "eventname" in str(k).lower()), None)
    metric_key = next((k for k, v in sample.items() if _to_number(v) is not None), None)
    if not donation_key or not event_key or not metric_key:
        return ""

    buckets = {}
    for r in rows:
        t = str(r.get(donation_key, ""))
        e = str(r.get(event_key, "")).lower()
        v = _to_number(r.get(metric_key)) or 0.0
        b = buckets.setdefault(t, {"click": 0.0, "purchase": 0.0})
        if "purchase" in e or "구매" in e:
            b["purchase"] += v
        elif "click" in e or "클릭" in e or "select" in e:
            b["click"] += v
    if not buckets:
        return ""
    lines = ["후원 유형별 전환율(구매/클릭)입니다."]
    valid = False
    for t, val in buckets.items():
        click = val["click"]
        purchase = val["purchase"]
        if click > 0:
            rate = purchase / click * 100
            lines.append(f"- {t}: 클릭 {click:,.0f}회, 구매 {purchase:,.0f}회, 전환율 **{rate:.1f}%**")
            valid = True
    return "\n".join(lines) if valid else ""


def _extract_named_ratio_keywords(question: str) -> List[str]:
    q = (question or "").strip()
    if not q:
        return []
    found = re.findall(r"([가-힣A-Za-z0-9_]+후원)", q)
    unique = []
    seen = set()
    for token in found:
        t = token.strip()
        if not t or t in seen:
            continue
        seen.add(t)
        unique.append(t)
    return unique[:3]


def _build_named_ratio_message(question: str, rows: List[Dict[str, Any]]) -> str:
    q = (question or "").lower()
    if not any(k in q for k in ["비중", "구성비", "비율", "점유율"]):
        return ""
    keywords = _extract_named_ratio_keywords(question)
    if len(keywords) < 2:
        return ""
    if not rows or not isinstance(rows[0], dict):
        return ""

    sample = rows[0]
    label_key = None
    metric_key = None
    for k, v in sample.items():
        if label_key is None and _to_number(v) is None:
            label_key = k
        if metric_key is None and _to_number(v) is not None:
            metric_key = k
    if not label_key or not metric_key:
        return ""

    buckets = {k: 0.0 for k in keywords}
    total = 0.0
    for row in rows:
        if not isinstance(row, dict):
            continue
        label = str(row.get(label_key, ""))
        val = _to_number(row.get(metric_key)) or 0.0
        if val < 0:
            continue
        total += val
        for key in keywords:
            if key in label:
                buckets[key] += val
                break

    focus_total = sum(buckets.values())
    if focus_total <= 0:
        return ""

    lines = []
    for key in keywords:
        v = buckets[key]
        share = (v / focus_total * 100) if focus_total else 0
        lines.append(f"- {key}: **{_format_value(v, '원')}** ({share:.1f}%)")
    other = max(total - focus_total, 0.0)
    if other > 0:
        lines.append(f"- 기타: **{_format_value(other, '원')}**")

    return "요청하신 후원 유형 비중입니다.\n" + "\n".join(lines)


def _build_item_profile_message(question: str, rows: List[Dict[str, Any]]) -> str:
    q = (question or "").lower()
    if not any(k in q for k in ["매개변수", "파라미터", "parameter", "상세", "정보", "더 알 수", "is_regular_donation", "donation_name", "country_name", "menu_name", "메뉴명", "메뉴 네임"]):
        return ""
    if not rows or not isinstance(rows[0], dict):
        return ""
    keys = list(rows[0].keys())
    dim_keys = [k for k in keys if _to_number(rows[0].get(k)) is None]
    metric_keys = [k for k in keys if _to_number(rows[0].get(k)) is not None]
    has_custom_params = any(str(k).startswith("customEvent:") for k in dim_keys)
    if "itemName" not in dim_keys and not has_custom_params:
        return ""

    entity_terms = _extract_entity_terms(question or "")
    target = entity_terms[0] if entity_terms else "요청 항목"

    filtered = rows
    if "itemName" in dim_keys:
        tmp = []
        for r in rows:
            try:
                if target in str(r.get("itemName", "")):
                    tmp.append(r)
            except Exception:
                continue
        if tmp:
            filtered = tmp

    lines = [f"**{target}** 관련 항목을 기준으로 확인한 추가 정보입니다."]
    lines.append(f"- 관련 항목 수: **{len(filtered)}개**")

    for dk in ["itemCategory", "itemBrand", "itemVariant"]:
        if dk in dim_keys:
            vals = [str(r.get(dk, "")).strip() for r in filtered if str(r.get(dk, "")).strip()]
            uniq = []
            seen = set()
            for v in vals:
                if v not in seen:
                    seen.add(v)
                    uniq.append(v)
            if uniq:
                lines.append(f"- {dk}: {', '.join(uniq[:5])}")

    custom_dim_keys = [k for k in dim_keys if str(k).startswith("customEvent:")]
    for ck in custom_dim_keys[:8]:
        vals = [str(r.get(ck, "")).strip() for r in filtered if str(r.get(ck, "")).strip()]
        uniq = []
        seen = set()
        for v in vals:
            if v not in seen:
                seen.add(v)
                uniq.append(v)
        if uniq:
            pretty_key = ck.replace("customEvent:", "")
            lines.append(f"- {pretty_key}: {', '.join(uniq[:6])}")

    if metric_keys:
        mk = metric_keys[0]
        total = sum(_to_number(r.get(mk)) or 0 for r in filtered)
        lines.append(f"- {mk} 합계: **{_format_value(total, '원' if 'revenue' in mk.lower() else '')}**")
    return "\n".join(lines)


def _extract_entity_terms(question: str) -> List[str]:
    q = (question or "").strip()
    if not q:
        return []
    candidates = []
    candidates.extend(re.findall(r"[\"']([^\"']{2,40})[\"']", q))
    candidates.extend(re.findall(r"([가-힣A-Za-z0-9_\-/\[\] ]{2,40})\s*(?:에\s*대해|에\s*대해서|관련|기준|만|비중|추이|원인|정보)", q))
    candidates.extend(re.findall(r"([가-힣A-Za-z0-9_\-/\[\]]{2,30})\s*[와과]\s*([가-힣A-Za-z0-9_\-/\[\]]{2,30})", q))
    candidates.extend(re.findall(r"([가-힣A-Za-z0-9_\-/\[\] ]{2,30})\s*,\s*([가-힣A-Za-z0-9_\-/\[\] ]{2,30})\s*같은", q))
    candidates.extend(re.findall(r"([가-힣A-Za-z0-9_]+후원)", q))
    flat = []
    for c in candidates:
        if isinstance(c, tuple):
            flat.extend(list(c))
        else:
            flat.append(c)
    stop = {
        "무엇", "어떤", "더", "알", "수", "있어", "있는", "기준", "관련", "정보",
        "비중", "추이", "원인", "분석", "상세", "매개변수", "파라미터", "항목", "상품", "아이템",
        "후원 이름", "후원명", "donation_name", "이탈", "이탈율", "이탈률", "활성", "신규", "매출", "수익", "세션", "전환"
    }
    uniq = []
    seen = set()
    def _clean_term(term: str) -> str:
        t = re.sub(r"\s+", " ", term).strip()
        while True:
            prev = t
            t = re.sub(r"\s*(관련|기준|정보|상세|매출|전환|추이|원인|분석|채널|캠페인)$", "", t).strip()
            t = re.sub(r"(은|는|이|가|을|를|에|의)$", "", t).strip()
            if t == prev:
                break
        t = re.sub(r"^(어떤|무슨|무엇)\s*", "", t).strip()
        return t
    for raw in flat:
        t = _clean_term(str(raw))
        if len(t) < 2 or t in stop:
            continue
        key = t.lower()
        if key in seen:
            continue
        seen.add(key)
        uniq.append(t)
    return uniq[:4]


def _select_filter_dimension(rows: List[Dict[str, Any]]) -> str:
    if not rows or not isinstance(rows[0], dict):
        return ""
    preferred = [
        "customEvent:menu_name", "customEvent:donation_name", "customEvent:click_text", "itemName", "eventName", "linkText", "defaultChannelGroup", "sourceMedium", "source",
        "medium", "landingPage", "pagePath", "pageTitle", "itemBrand", "itemCategory"
    ]
    keys = list(rows[0].keys())
    for p in preferred:
        if p in keys and _to_number(rows[0].get(p)) is None:
            return p
    for k in keys:
        if _to_number(rows[0].get(k)) is None:
            return k
    return ""


def _filter_rows_by_entity_terms(rows: List[Dict[str, Any]], terms: List[str]) -> List[Dict[str, Any]]:
    if not rows or not terms:
        return rows
    if not isinstance(rows[0], dict):
        return rows
    dim = _select_filter_dimension(rows)
    if not dim:
        return rows
    filtered = []
    for row in rows:
        value = str(row.get(dim, ""))
        if any(term in value for term in terms):
            filtered.append(row)
    return filtered if filtered else rows


def _apply_question_entity_filter(question: str, blocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    terms = _extract_entity_terms(question)
    if not terms:
        return blocks
    updated = []
    for block in blocks:
        rows = block.get("data")
        if isinstance(rows, list) and rows and isinstance(rows[0], dict):
            filtered_rows = _filter_rows_by_entity_terms(rows, terms)
            nb = dict(block)
            nb["data"] = filtered_rows
            updated.append(nb)
        else:
            updated.append(block)
    return updated


def adapt_pipeline_response_to_legacy(
    pipeline_response: Dict[str, Any],
    question: str = "",
    user_name: str = ""
) -> Dict[str, Any]:
    """
    새 파이프라인 응답을 기존 형식으로 변환
    """
    blocks = pipeline_response.get("blocks", [])
    matching_debug = pipeline_response.get("matching_debug", {})
    blocks = _apply_question_entity_filter(question, blocks)
    
    if not blocks:
        fallback = pipeline_response.get("message")
        if fallback:
            if "0개 블록 분석 완료" in str(fallback):
                fallback = "질문 의도는 이해했지만 현재 데이터에서 조건에 맞는 항목을 찾지 못했습니다. 기준(기간/프로그램명/이벤트명)을 조금 넓혀 다시 확인해 주세요."
            return {
                "message": fallback,
                "raw_data": [],
                "structured": {},
                "plot_data": [],
                "followup_suggestions": [],
                "matching_debug": matching_debug
            }
        return {
            "message": "질문 의도는 이해했지만 현재 조건에서 조회된 데이터가 없습니다. 기간이나 지표를 바꿔 다시 질문해 주세요.",
            "raw_data": [],
            "structured": {},
            "plot_data": [],
            "matching_debug": matching_debug,
            "followup_suggestions": [
                "기간을 넓혀서 다시 조회할까요?",
                "지표를 바꿔서 확인해볼까요?",
                "차원별(예: 채널별)로 나눠 볼까요?"
            ]
        }
    
    # 메시지 생성 (직접 답변 + 요약)
    message_parts = []
    quality_warnings = []
    raw_data = []
    structured = {}
    
    from ga4_metadata import GA4_METRICS
    
    # 블록 타입별 그룹화
    total_blocks = [b for b in blocks if b.get("type") == "total"]
    breakdown_blocks = [b for b in blocks if b.get("type") in ["breakdown", "breakdown_topn", "trend"]]
    
    opening = f"{user_name}님, " if user_name else ""
    concise = _is_brief_request(question)

    # Total blocks 처리: 질문에 대한 직접 답변
    for block in total_blocks:
        data = block.get("data", {})
        
        for key, value in data.items():
            metric_info = GA4_METRICS.get(key, {})
            ui_name = metric_info.get("ui_name", key)
            category = metric_info.get("category", "")
            
            unit = _metric_unit(key, category)
            pretty = _format_value(value, unit)
            structured[ui_name] = pretty
            particle = _topic_particle(ui_name)
            message_parts.append(f"{opening}{ui_name}{particle} **{pretty}**입니다.")
            opening = ""
    
    # Breakdown blocks 처리: 상위 항목 핵심 요약
    top_rows_limit = 10
    q_lower = (question or "").lower()
    if any(k in q_lower for k in ["가장", "많이", "어떤"]) and not any(k in q_lower for k in ["top", "상위", "10", "20"]):
        top_rows_limit = 5
    if any(k in q_lower for k in ["전체", "전부", "모든", "전체 항목", "전체 목록"]):
        top_rows_limit = 1000

    for block in breakdown_blocks:
        data = block.get("data")
        title = block.get("title", "상세 분석")
        
        if isinstance(data, list) and data:
            warning = _build_data_quality_warning(question, data)
            if warning:
                quality_warnings.append(warning)
            preferred_dim = _question_focus_dimension(question, data)
            data = _clean_display_rows(data, preferred_dim=preferred_dim)
            if not data:
                continue
            raw_data.extend(data)
            block_type = block.get("type")
            count = len(data)
            first = data[0] if isinstance(data[0], dict) else None
            if block_type == "trend":
                if first:
                    date_key = next((k for k, v in first.items() if isinstance(v, str) and re.match(r"^\d{4}-\d{2}-\d{2}", str(v))), None)
                    if date_key:
                        date_vals = [str(r.get(date_key, "")) for r in data if isinstance(r, dict)]
                        date_vals = [d for d in date_vals if re.match(r"^\d{4}-\d{2}-\d{2}$", d)]
                        date_vals.sort()
                        start = date_vals[0] if date_vals else data[0].get(date_key)
                        end = date_vals[-1] if date_vals else data[-1].get(date_key)
                        message_parts.append(f"{title} 데이터를 **{count}개 시점**으로 확인했습니다. ({start} ~ {end})")
                    else:
                        message_parts.append(f"{title} 데이터를 **{count}개 시점**으로 확인했습니다.")
                else:
                    message_parts.append(f"{title} 데이터를 **{count}개 시점**으로 확인했습니다.")
            else:
                if first:
                    summary = _summarize_top_item(first)
                    message_parts.append(f"{title} 기준 상위 결과는 **{summary}** 입니다. (총 {count}개)")
                    top_lines = _format_top_rows(data, max_rows=min(top_rows_limit, count))
                    if top_lines:
                        message_parts.append("상위 목록:\n" + "\n".join(top_lines))
                else:
                    message_parts.append(f"{title} 기준으로 총 {count}개 항목을 확인했습니다.")

    # 비중/구성비 질문은 명시 항목 비중 요약을 우선 추가
    ratio_msg = _build_named_ratio_message(question, raw_data)
    if ratio_msg:
        message_parts = [ratio_msg] + message_parts
    dual_compare_msg = _build_dual_entity_compare_message(question, raw_data)
    if dual_compare_msg:
        message_parts = [dual_compare_msg] + message_parts
    domestic_overseas_msg = _build_domestic_overseas_message(question, raw_data)
    if domestic_overseas_msg:
        message_parts = [domestic_overseas_msg] + message_parts
    conversion_msg = _build_donation_type_conversion_message(question, raw_data)
    if conversion_msg:
        message_parts = [conversion_msg] + message_parts
    profile_msg = _build_item_profile_message(question, raw_data)
    if profile_msg:
        message_parts = [profile_msg] + message_parts
    
    # 최종 메시지
    if not message_parts:
        if quality_warnings:
            final_message = quality_warnings[0]
        else:
            final_message = "분석이 완료되었습니다."
    else:
        final_message = " ".join(message_parts) if concise else "\n".join(message_parts)
        if quality_warnings:
            final_message = final_message + ("\n" if not concise else " ") + quality_warnings[0]
    
    return {
        "message": final_message,
        "raw_data": raw_data,
        "structured": structured,
        "plot_data": _extract_plot_data(blocks),
        "matching_debug": matching_debug,
        "followup_suggestions": _build_followups(question, has_breakdown=bool(breakdown_blocks))
    }
