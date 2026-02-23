# candidate_extractor.py
# GA4 Candidate Extraction Layer
"""
질문에서 후보(Candidates)만 추출하는 레이어.
결정은 하지 않고, 가능성 있는 모든 후보를 score와 함께 반환한다.

핵심 원칙:
1. 결정 금지 - "이게 맞다"가 아니라 "이것들이 가능하다"
2. Score 기반 - 모든 후보에 신뢰도 점수 부여
3. 다중 후보 - 가능한 모든 후보를 반환 (Planner가 선택)
"""

import re
import os
import logging
from datetime import date, datetime, timedelta
from typing import List, Dict, Optional, Any

from ga4_metadata import GA4_METRICS, GA4_DIMENSIONS
from ml_module import parse_dates

KNOWN_CUSTOM_PARAM_TOKENS = {
    "banner_name", "button_name", "click_button", "click_location", "click_section",
    "click_text",
    "content_category", "content_name", "content_type", "country_name", "detail_category",
    "donation_name", "event_category", "event_label", "is_regular_donation",
    "letter_translation", "main_category", "menu_name", "payment_type", "percent_scrolled",
    "referrer_host", "referrer_pathname", "step", "sub_category",
    "domestic_children_count", "overseas_children_count"
}


def _normalize_text(text: str) -> str:
    """공백/구두점 변형을 흡수한 비교용 문자열"""
    if not text:
        return ""
    lowered = text.lower()
    return re.sub(r"[\s\-_/]+", "", lowered)


def _is_too_short_term(term: str) -> bool:
    """한 글자 용어는 오탐이 매우 높아 substring 매칭에서 제외"""
    return len(_normalize_text(term or "")) <= 1


def _resolve_metric_name(name: str) -> Optional[str]:
    if not name:
        return None
    key = str(name).strip()
    if key in GA4_METRICS:
        return key
    key_norm = _normalize_text(key)
    for metric_name, meta in GA4_METRICS.items():
        if _normalize_text(metric_name) == key_norm:
            return metric_name
        ui_name = meta.get("ui_name", "")
        if ui_name and _normalize_text(ui_name) == key_norm:
            return metric_name
        for alias in meta.get("aliases", []):
            if _normalize_text(alias) == key_norm:
                return metric_name
    return None


def _resolve_dimension_name(name: str) -> Optional[str]:
    if not name:
        return None
    key = str(name).strip()
    if key in GA4_DIMENSIONS:
        return key
    key_norm = _normalize_text(key)
    for dim_name, meta in GA4_DIMENSIONS.items():
        if _normalize_text(dim_name) == key_norm:
            return dim_name
        ui_name = meta.get("ui_name", "")
        if ui_name and _normalize_text(ui_name) == key_norm:
            return dim_name
        for alias in meta.get("aliases", []):
            if _normalize_text(alias) == key_norm:
                return dim_name
    return None


def _extract_entity_terms(question: str) -> List[str]:
    q = (question or "").strip()
    if not q:
        return []

    candidates = []
    # 따옴표 패턴: "브랜드A", 'Campaign X'
    candidates.extend(re.findall(r"[\"']([^\"']{2,40})[\"']", q))
    # "X에 대해/관련/기준/만/비중/추이/원인/정보"
    candidates.extend(re.findall(r"([가-힣A-Za-z0-9_\-/\[\] ]{2,40})\s*(?:에\s*대해|에\s*대해서|관련|기준|만|비중|추이|원인|정보)", q))
    # "A와 B", "A과 B"
    candidates.extend(re.findall(r"([가-힣A-Za-z0-9_\-/\[\]]{2,30})\s*[와과]\s*([가-힣A-Za-z0-9_\-/\[\]]{2,30})", q))
    # "A, B 같은 ..."
    candidates.extend(re.findall(r"([가-힣A-Za-z0-9_\-/\[\] ]{2,30})\s*,\s*([가-힣A-Za-z0-9_\-/\[\] ]{2,30})\s*같은", q))
    # "X 국가별"
    candidates.extend(re.findall(r"([가-힣A-Za-z0-9_\-/\[\] ]{2,40})\s*국가별", q))
    # "X의 ..." 패턴 (예: display의 소스 매체)
    candidates.extend(re.findall(r"([가-힣A-Za-z0-9_\-/\[\]]{2,40})\s*의\s*", q))
    flat = []
    for c in candidates:
        if isinstance(c, tuple):
            flat.extend(list(c))
        else:
            flat.append(c)

    # 기존 후원 패턴은 유지
    flat.extend(re.findall(r"([가-힣A-Za-z0-9_]+후원)", q))
    # 채널 토큰 직접 추출
    for token in ["display", "paid", "organic", "direct", "referral", "unassigned", "cross-network"]:
        if token in q.lower():
            flat.append(token)

    stop = {
        "무엇", "어떤", "더", "알", "수", "있어", "있는", "기준", "관련", "정보",
        "비중", "추이", "원인", "분석", "상세", "매개변수", "파라미터", "항목", "상품", "아이템",
        "후원 이름", "후원명", "donation_name", "이탈", "이탈율", "이탈률", "활성", "신규", "매출", "수익", "세션", "전환",
        "클릭", "구매", "구매로", "판매", "프로그램", "국가",
        "상품별", "아이템별", "제품별", "지난주", "이번주", "지난달", "이번달", "어제", "오늘",
        "첫후원", "첫구매", "처음후원", "처음구매", "구매한", "사용자수", "사용자 수",
        "후원자", "구매자", "유형", "타입", "전체"
    }
    uniq = []
    seen = set()

    def _clean_term(term: str) -> str:
        t = re.sub(r"\s+", " ", term).strip()
        # "X별 ..." 구문은 차원 지정 표현으로 간주하여 엔티티에서 제거
        t = re.sub(r"[A-Za-z0-9_가-힣]+\s*별.*$", "", t).strip()
        # ranking/집계형 문장 정리
        t = re.sub(r"^(가장|최고|최저|상위|하위)\s*", "", t).strip()
        t = re.sub(r"(top\s*\d+|상위\s*\d+|\d+\s*위|\d+\s*[-~]\s*\d+)\s*", "", t, flags=re.IGNORECASE).strip()
        # 의미 없는 접미어/조사를 반복 제거
        while True:
            prev = t
            t = re.sub(r"\s*(관련|기준|정보|상세|매출|전환|추이|원인|분석|채널|캠페인)$", "", t).strip()
            t = re.sub(r"(은|는|이|가|을|를|에|의|중|중에|쪽|쪽에)$", "", t).strip()
            if t == prev:
                break
        t = re.sub(r"^(어떤|무슨|무엇)\s*", "", t).strip()
        return t

    for raw in flat:
        t = _clean_term(str(raw))
        if len(t) < 2:
            continue
        if t in stop:
            continue
        # 지나치게 일반적인 조각 제외
        if t.lower() in {"top", "ga4", "data", "report"}:
            continue
        if len(t.split()) >= 3 and any(k in t for k in ["가장", "상위", "매출", "상품", "사용자"]):
            continue
        # 조건/축 표현 오탐 제거
        if any(noise in t.lower() for noise in ["event", "이벤트", "기준", "purchase", "click", "donation_name"]):
            continue
        key = t.lower()
        if key in seen:
            continue
        seen.add(key)
        uniq.append(t)
    return uniq[:4]


def _extract_event_name_token(question: str) -> str:
    q = (question or "").strip()
    if not q:
        return ""
    q_lower = q.lower()

    # snake_case / event-like token (e.g., gnb_click)
    tokens = re.findall(r"[a-z][a-z0-9]*(?:_[a-z0-9]+)+", q_lower)
    if tokens:
        token = tokens[0]
        if token in KNOWN_CUSTOM_PARAM_TOKENS:
            return ""
        return token

    # "gnb클릭", "menu 클릭" -> gnb_click / menu_click
    m = re.search(r"\b([a-z0-9]+)\s*클릭\b", q_lower)
    if m:
        return f"{m.group(1)}_click"

    # "이벤트 xxx" where xxx is english-ish token
    m2 = re.search(r"이벤트\s*([a-z][a-z0-9_\-]{2,40})", q_lower)
    if m2:
        token = m2.group(1).replace("-", "_")
        if token in KNOWN_CUSTOM_PARAM_TOKENS:
            return ""
        return token

    return ""


# =============================================================================
# Date Parser (기존 유지)
# =============================================================================

class DateParser:
    """날짜 추출 (변경 없음)"""
    
    @staticmethod
    def parse(question, last_state=None, date_context=None):
        delta_dates = {"start_date": None, "end_date": None, "is_relative_shift": False}
        q = question.lower()

        # 0) 지난주 vs 전주 자동 기간 계산 (동일 길이 직전 구간 포함)
        if "지난주" in q and ("전주" in q or "그 전주" in q):
            today = date.today()
            this_monday = today - timedelta(days=today.weekday())
            last_week_start = this_monday - timedelta(days=7)
            last_week_end = last_week_start + timedelta(days=6)
            prev_week_start = last_week_start - timedelta(days=7)
            prev_week_end = last_week_end - timedelta(days=7)
            delta_dates["start_date"] = prev_week_start.strftime("%Y-%m-%d")
            delta_dates["end_date"] = last_week_end.strftime("%Y-%m-%d")
            delta_dates["compare_windows"] = [
                {"label": "prev_week", "start_date": prev_week_start.strftime("%Y-%m-%d"), "end_date": prev_week_end.strftime("%Y-%m-%d")},
                {"label": "last_week", "start_date": last_week_start.strftime("%Y-%m-%d"), "end_date": last_week_end.strftime("%Y-%m-%d")},
            ]
            return delta_dates
        
        # 1. Relative Shift
        if date_context and ("그 전주" in q or ("전주" in q and "지난주" not in q and "이번주" not in q)):
            if last_state and last_state.get("start_date") and last_state.get("end_date"):
                try:
                    ls_start = datetime.strptime(last_state["start_date"], "%Y-%m-%d")
                    ls_end = datetime.strptime(last_state["end_date"], "%Y-%m-%d")
                    
                    delta_dates["start_date"] = (ls_start - timedelta(days=7)).strftime("%Y-%m-%d")
                    delta_dates["end_date"] = (ls_end - timedelta(days=7)).strftime("%Y-%m-%d")
                    delta_dates["is_relative_shift"] = True
                    logging.info(f"[DateParser] Relative shift: {delta_dates['start_date']} ~ {delta_dates['end_date']}")
                    return delta_dates
                except Exception as e:
                    logging.error(f"[DateParser] Relative shift error: {e}")

        # 2. Period phrases
        period_phrases = []
        if "지난주" in q: period_phrases.append("지난주")
        if "이번주" in q: period_phrases.append("이번주")
        if "지난달" in q: period_phrases.append("지난달")
        if "이번달" in q: period_phrases.append("이번달")
        if "어제" in q: period_phrases.append("어제")
        if "오늘" in q: period_phrases.append("오늘")

        if ("지난달" in q and "이번달" in q):
            # 비교 질의: 지난달 1일 ~ 오늘 (yearMonth 분해와 조합)
            today = date.today()
            first_this_month = today.replace(day=1)
            last_month_end = first_this_month - timedelta(days=1)
            last_month_start = last_month_end.replace(day=1)
            delta_dates["start_date"] = last_month_start.strftime("%Y-%m-%d")
            delta_dates["end_date"] = today.strftime("%Y-%m-%d")
            return delta_dates

        if period_phrases:
            phrase = period_phrases[0]
            s_date, e_date = DateParser._phrase_to_range(phrase)
            delta_dates["start_date"] = s_date
            delta_dates["end_date"] = e_date
            return delta_dates

        # 3. Explicit dates
        s, e = parse_dates(question)
        if s and e:
            delta_dates["start_date"], delta_dates["end_date"] = s, e

        return delta_dates

    @staticmethod
    def _phrase_to_range(phrase):
        today = date.today()
        if phrase == "오늘": s = e = today
        elif phrase == "어제": s = e = today - timedelta(days=1)
        elif phrase == "지난주":
            s = today - timedelta(days=today.weekday() + 7)
            e = s + timedelta(days=6)
        elif phrase == "이번주":
            s = today - timedelta(days=today.weekday())
            e = today
        elif phrase == "지난달":
            first_this_month = today.replace(day=1)
            e = first_this_month - timedelta(days=1)
            s = e.replace(day=1)
        elif phrase == "이번달":
            s = today.replace(day=1)
            e = today
        else:
            s = today - timedelta(days=7)
            e = today
        return s.strftime("%Y-%m-%d"), e.strftime("%Y-%m-%d")


# =============================================================================
# Intent Classifier (독립 레이어)
# =============================================================================

class IntentClassifier:
    """
    의도 분류기 - 질문의 의도만 판단
    
    반환값:
    - metric_single: 단일 지표 조회
    - metric_multi: 여러 지표 조회
    - breakdown: 차원별 분석
    - topn: 상위 N개
    - trend: 추이 분석
    - comparison: 비교 분석
    - category_list: 카테고리 목록
    """
    
    @staticmethod
    def classify(question: str) -> str:
        q = question.lower()
        if ("지난주" in q and ("그 전주" in q or "전주" in q)):
            return "comparison"
        if ("지난주" in q and ("그 전주" in q or "전주" in q)) and any(k in q for k in ["사용자", "유저", "세션"]):
            return "comparison"
        
        # 1. Category List (최우선)
        if ("종류" in q and any(k in q for k in ["이벤트", "event", "목록"])) or "무슨 이벤트" in q or "어떤 이벤트" in q:
            return "category_list"
        
        # 2. TopN (명시적 숫자)
        if re.search(r'(top\s*\d+|상위\s*\d+|\d+\s*위|1\s*[-~]\s*\d+|\d+개)', q):
            return "topn"
        if any(k in q for k in ["가장", "최고", "최저", "높은", "낮은"]) and any(k in q for k in ["상품", "매출", "이벤트", "후원"]):
            return "topn"

        # 2.1 전체 항목/목록 후속 조회는 breakdown
        if any(k in q for k in ["전체 항목", "전체 목록", "전체 프로그램", "모든 항목", "전부 보여", "다 보여", "전체 보여", "전체 보여줘", "이것 전체", "이거 전체"]):
            return "breakdown"

        # 2.5 비중/구성비/비율 -> breakdown
        if any(k in q for k in ["비중", "구성비", "비율", "점유율"]):
            return "breakdown"

        # 2.55 비교형 자연어 ("A와 B는 어때?")
        if any(k in q for k in ["어때", "어떤게", "무엇이"]) and any(k in q for k in ["와", "과", "중"]):
            return "breakdown"
        if any(k in q for k in ["유형", "타입", "종류"]) and any(k in q for k in ["어떤", "많이", "가장", "상위"]):
            return "breakdown"

        # 2.6 탐색형 상세질문 -> breakdown
        if any(k in q for k in ["어떤 것을 더 알 수", "무엇을 더 알 수", "상세", "정보", "매개변수", "파라미터"]):
            return "breakdown"

        # donation_click / donation_name 클릭 분포 질문은 breakdown 우선
        if any(k in q for k in ["donation_click", "donation_name"]) and any(k in q for k in ["클릭", "click", "주로 어떤", "순위", "많이"]):
            return "breakdown"
        if ("donation" in q and any(k in q for k in ["클릭", "click"])) and any(k in q for k in ["어떤", "주로", "순위", "top", "상위"]):
            return "breakdown"
        
        # 3. Trend
        if any(k in q for k in ["추이", "흐름", "일별", "변화", "trend", "daily"]):
            return "trend"
        
        # 4. Comparison
        if any(k in q for k in ["전주 대비", "비교", "차이", "증감", "compare", "vs"]):
            return "comparison"
        if q.strip() in ["비교", "비교해", "비교해서", "대비", "증감"]:
            return "comparison"
        
        # 5. Breakdown
        if any(k in q for k in ["별", "기준", "따라", "by "]):
            return "breakdown"

        # 5.1 차원 축(채널/소스/매체 등) 언급은 breakdown
        if any(k in q for k in ["채널", "소스", "매체", "디바이스", "기기", "랜딩", "국가", "카테고리", "유형", "타입", "종류", "메뉴명", "후원명", "광고", "paid", "display"]):
            return "breakdown"
        if len(q.strip()) <= 20 and any(k in q for k in ["name", "이름", "네임"]):
            return "breakdown"
        
        # 6. Multi-metric (여러 지표 언급)
        if any(k in q for k in ["와", "과", ","]) and any(k in q for k in ["사용자", "유저"]) and any(k in q for k in ["구매한", "구매자", "후원자", "구매"]):
            return "metric_multi"
        if any(k in q for k in ["와", "과", ","]) and any(k in q for k in ["구매수", "구매 건수", "구매건수", "트랜잭션"]) and any(k in q for k in ["전체 구매자", "구매자", "후원자"]):
            return "metric_multi"
        metric_count = sum(1 for m_meta in GA4_METRICS.values() 
                          if m_meta.get("ui_name", "").lower() in q)
        if metric_count > 1:
            return "metric_multi"
        
        # Default
        return "metric_single"


# =============================================================================
# Metric Candidate Extractor
# =============================================================================

class MetricCandidateExtractor:
    """
    Metric 후보 추출기
    
    반환 형식:
    [
      {"name": "activeUsers", "score": 0.95, "matched_by": "explicit", "scope": "event"},
      {"name": "sessions", "score": 0.82, "matched_by": "semantic", "scope": "event"}
    ]
    """
    
    @staticmethod
    def extract(question: str, semantic=None) -> List[Dict[str, Any]]:
        """
        질문에서 Metric 후보 추출
        
        Args:
            question: 사용자 질문
            semantic: SemanticMatcher (선택)
        
        Returns:
            후보 리스트 (score 높은 순 정렬)
        """
        q = question.lower()
        candidates = []
        seen = set()  # 🔥 중복 방지용
        is_ranking_query = bool(re.search(r'(top\s*\d+|상위\s*\d+|\d+위|1-\d+|\d+개)', q))
        
        # 1. Explicit matching (substring)
        for metric_name, meta in GA4_METRICS.items():
            score = MetricCandidateExtractor._calculate_explicit_score(q, metric_name, meta)
            
            if score > 0:
                scope = meta.get("scope") or MetricCandidateExtractor._infer_scope_from_category(
                    meta.get("category")
                )
                
                candidates.append({
                    "name": metric_name,
                    "score": score,
                    "matched_by": "explicit",
                    "scope": scope,
                    "priority": meta.get("priority", 0)
                })
                seen.add(metric_name)
        
        # 2. Semantic matching
        if semantic:
            sem_candidates = semantic.match_metric(question)
            for sem in sem_candidates:
                name = sem.get("name")
                confidence = sem.get("confidence", 0)
                
                # 이미 explicit으로 찾은 것은 제외
                if name in seen:
                    continue
                
                if confidence >= 0.25:  # 최소 임계값
                    meta = GA4_METRICS.get(name, {})
                    scope = meta.get("scope") or MetricCandidateExtractor._infer_scope_from_category(
                        meta.get("category")
                    )
                    
                    candidates.append({
                        "name": name,
                        "score": confidence,
                        "matched_by": "semantic",
                        "scope": scope,
                        "priority": meta.get("priority", 0)
                    })
                    seen.add(name)
        
        # 🔥 Boost item-scoped metrics if question contains item keywords
        item_keywords = ["상품", "아이템", "제품", "상품별", "아이템별", "제품별", "항목", "브랜드"]
        if any(kw in question for kw in item_keywords):
            for candidate in candidates:
                if candidate.get("scope") == "item":
                    candidate["score"] = min(candidate["score"] + 0.15, 1.0)
                    logging.info(f"[MetricExtractor] Boosted item-scoped metric: {candidate['name']} -> {candidate['score']:.2f}")

        # TopN + 항목 류 질문에서는 item scope를 추가 가중
        if is_ranking_query and any(kw in question for kw in ["항목", "상품", "아이템", "제품"]):
            for candidate in candidates:
                if candidate.get("scope") == "item":
                    candidate["score"] = min(candidate["score"] + 0.20, 1.0)
                elif candidate.get("scope") == "event":
                    candidate["score"] = max(candidate["score"] - 0.08, 0.0)

        # item 힌트가 있으나 item 후보가 없으면, 컨셉/카테고리 기반으로 item 후보 보강
        has_item_hint = any(kw in question for kw in ["상품", "아이템", "제품", "항목", "후원", "브랜드"])
        has_item_candidate = any(c.get("scope") == "item" for c in candidates)
        if has_item_hint and not has_item_candidate:
            top_concept = None
            top_category = None
            if candidates:
                top_name = candidates[0]["name"]
                top_meta = GA4_METRICS.get(top_name, {})
                top_concept = top_meta.get("concept")
                top_category = top_meta.get("category")

            inferred = []
            for m_name, meta in GA4_METRICS.items():
                m_scope = meta.get("scope") or MetricCandidateExtractor._infer_scope_from_category(meta.get("category"))
                if m_scope != "item":
                    continue
                if top_concept and meta.get("concept") != top_concept:
                    continue
                if (not top_concept) and top_category and meta.get("category") != top_category:
                    continue
                inferred.append((m_name, meta.get("priority", 0)))

            inferred.sort(key=lambda x: x[1], reverse=True)
            for m_name, pr in inferred[:3]:
                if m_name in seen:
                    continue
                candidates.append({
                    "name": m_name,
                    "score": min(0.72 + (pr * 0.02), 0.88),
                    "matched_by": "scope_infer",
                    "scope": "item",
                    "priority": pr
                })
                seen.add(m_name)
                logging.info(f"[MetricExtractor] Inferred item metric candidate: {m_name}")

        # 후원 유형 비중/구성비 질문은 item 매출 지표를 우선 후보로 추가
        donation_keywords = ["후원", "정기후원", "일시후원"]
        ratio_keywords = ["비중", "구성비", "점유율", "나눠줘", "나눠", "비교"]
        if any(k in question for k in donation_keywords) and any(k in question for k in ratio_keywords):
            boosted = []
            for m_name in ["itemRevenue", "grossItemRevenue", "purchaseRevenue"]:
                if m_name in seen:
                    continue
                meta = GA4_METRICS.get(m_name, {})
                scope = meta.get("scope") or MetricCandidateExtractor._infer_scope_from_category(
                    meta.get("category")
                )
                score = 0.94 if m_name in ["itemRevenue", "grossItemRevenue"] else 0.86
                candidates.append({
                    "name": m_name,
                    "score": score,
                    "matched_by": "donation_ratio_rule",
                    "scope": scope,
                    "priority": meta.get("priority", 0)
                })
                seen.add(m_name)
                boosted.append(m_name)
            if boosted:
                logging.info(f"[MetricExtractor] Donation ratio boost: {boosted}")

        # 후원 관련 "많이/가장/어떤게" 질문은 후원 매출/건수 우선
        if any(k in question for k in donation_keywords) and any(k in question for k in ["많이", "가장", "어떤", "상위", "top"]):
            prefer = [("itemRevenue", 0.94), ("purchaseRevenue", 0.90), ("transactions", 0.88)]
            for m_name, sc in prefer:
                if m_name in seen:
                    for c in candidates:
                        if c.get("name") == m_name:
                            c["score"] = max(c.get("score", 0), sc)
                            c["matched_by"] = "donation_volume_rule"
                    continue
                meta = GA4_METRICS.get(m_name, {})
                scope = meta.get("scope") or MetricCandidateExtractor._infer_scope_from_category(meta.get("category"))
                candidates.append({
                    "name": m_name,
                    "score": sc,
                    "matched_by": "donation_volume_rule",
                    "scope": scope,
                    "priority": meta.get("priority", 0)
                })
                seen.add(m_name)

        # 판매/반응 자연어 보강
        if any(k in question for k in ["판매", "팔리", "매출", "수익"]):
            for m_name, sc in [("purchaseRevenue", 0.90), ("itemRevenue", 0.86), ("transactions", 0.84)]:
                if m_name in seen:
                    for c in candidates:
                        if c.get("name") == m_name:
                            c["score"] = max(c.get("score", 0), sc)
                            c["matched_by"] = "sales_semantic_rule"
                    continue
                meta = GA4_METRICS.get(m_name, {})
                scope = meta.get("scope") or MetricCandidateExtractor._infer_scope_from_category(meta.get("category"))
                candidates.append({
                    "name": m_name,
                    "score": sc,
                    "matched_by": "sales_semantic_rule",
                    "scope": scope,
                    "priority": meta.get("priority", 0)
                })
                seen.add(m_name)

        # 유입 축(소스/매체/채널) + 구매/매출 질문은 구매 기여 지표 우선
        has_acq_axis = any(k in q for k in ["소스", "매체", "채널", "유입", "source", "medium"])
        has_purchase_intent = any(k in q for k in ["구매", "매출", "수익", "후원"])
        if has_acq_axis and has_purchase_intent:
            for m_name, sc in [("purchaseRevenue", 0.97), ("totalPurchasers", 0.95), ("transactions", 0.90)]:
                if m_name in seen:
                    for c in candidates:
                        if c.get("name") == m_name:
                            c["score"] = max(c.get("score", 0), sc)
                            c["matched_by"] = "acq_purchase_slot_rule"
                    continue
                meta = GA4_METRICS.get(m_name, {})
                scope = meta.get("scope") or MetricCandidateExtractor._infer_scope_from_category(meta.get("category"))
                candidates.append({
                    "name": m_name,
                    "score": sc,
                    "matched_by": "acq_purchase_slot_rule",
                    "scope": scope,
                    "priority": meta.get("priority", 0)
                })
                seen.add(m_name)

        # "후원 유형별 매출"은 이벤트 파라미터 기반 매출 집계 우선
        if any(k in q for k in ["후원 유형", "후원유형"]) and any(k in q for k in ["매출", "수익", "금액", "revenue"]):
            for m_name, sc in [("purchaseRevenue", 0.96), ("eventCount", 0.86)]:
                if m_name in seen:
                    for c in candidates:
                        if c.get("name") == m_name:
                            c["score"] = max(c.get("score", 0), sc)
                            c["matched_by"] = "donation_type_revenue_rule"
                    continue
                meta = GA4_METRICS.get(m_name, {})
                scope = meta.get("scope") or MetricCandidateExtractor._infer_scope_from_category(meta.get("category"))
                candidates.append({
                    "name": m_name,
                    "score": sc,
                    "matched_by": "donation_type_revenue_rule",
                    "scope": scope,
                    "priority": meta.get("priority", 0)
                })
                seen.add(m_name)

        # 이벤트 종류/목록 질문은 eventCount 우선
        if any(k in q for k in ["이벤트 종류", "이벤트 목록", "무슨 이벤트", "어떤 이벤트"]):
            if "eventCount" in seen:
                for c in candidates:
                    if c.get("name") == "eventCount":
                        c["score"] = max(c.get("score", 0), 0.99)
                        c["matched_by"] = "event_category_list_rule"
            else:
                meta = GA4_METRICS.get("eventCount", {})
                candidates.append({
                    "name": "eventCount",
                    "score": 0.99,
                    "matched_by": "event_category_list_rule",
                    "scope": meta.get("scope") or MetricCandidateExtractor._infer_scope_from_category(meta.get("category")),
                    "priority": meta.get("priority", 0)
                })
                seen.add("eventCount")

        # purchase vs donation_click 비교는 건수(eventCount) 우선
        if ("donation_click" in q) and any(k in q for k in ["purchase", "구매"]):
            if "eventCount" in seen:
                for c in candidates:
                    if c.get("name") == "eventCount":
                        c["score"] = max(c.get("score", 0), 0.97)
                        c["matched_by"] = "event_pair_compare_rule"
            else:
                meta = GA4_METRICS.get("eventCount", {})
                candidates.append({
                    "name": "eventCount",
                    "score": 0.97,
                    "matched_by": "event_pair_compare_rule",
                    "scope": meta.get("scope") or MetricCandidateExtractor._infer_scope_from_category(meta.get("category")),
                    "priority": meta.get("priority", 0)
                })
                seen.add("eventCount")

        # 클릭/이벤트 항목 탐색 질문은 이벤트 카운트 지표 우선
        click_terms = ["클릭", "눌", "tap", "click"]
        item_probe_terms = ["항목", "무엇", "뭐", "어떤", "많이", "상위"]
        if any(k in q for k in click_terms) and any(k in q for k in item_probe_terms):
            for m_name, sc in [("eventCount", 0.96), ("keyEvents", 0.88), ("totalUsers", 0.80)]:
                if m_name in seen:
                    for c in candidates:
                        if c.get("name") == m_name:
                            c["score"] = max(c.get("score", 0), sc)
                            c["matched_by"] = "click_event_rule"
                    continue
                meta = GA4_METRICS.get(m_name, {})
                scope = meta.get("scope") or MetricCandidateExtractor._infer_scope_from_category(meta.get("category"))
                candidates.append({
                    "name": m_name,
                    "score": sc,
                    "matched_by": "click_event_rule",
                    "scope": scope,
                    "priority": meta.get("priority", 0)
                })
                seen.add(m_name)
            # 매출 언급이 없으면 금액 지표는 후순위로 낮춤
            if not any(k in q for k in ["매출", "수익", "금액", "revenue"]):
                for c in candidates:
                    if c.get("name") in ["purchaseRevenue", "itemRevenue", "grossItemRevenue"]:
                        c["score"] = max(0.0, c.get("score", 0) - 0.25)

        # "정기후원의 클릭수" 같은 패턴은 donation_click + donation_name(eventCount)로 보정
        has_donation_entity = bool(re.search(r"[가-힣A-Za-z0-9_]+후원", question))
        if has_donation_entity and any(k in q for k in ["클릭수", "클릭", "click"]) and not any(k in q for k in ["메뉴", "gnb", "lnb", "footer"]):
            for m_name, sc in [("eventCount", 0.99), ("keyEvents", 0.90)]:
                if m_name in seen:
                    for c in candidates:
                        if c.get("name") == m_name:
                            c["score"] = max(c.get("score", 0), sc)
                            c["matched_by"] = "donation_entity_click_rule"
                    continue
                meta = GA4_METRICS.get(m_name, {})
                scope = meta.get("scope") or MetricCandidateExtractor._infer_scope_from_category(meta.get("category"))
                candidates.append({
                    "name": m_name,
                    "score": sc,
                    "matched_by": "donation_entity_click_rule",
                    "scope": scope,
                    "priority": meta.get("priority", 0)
                })
                seen.add(m_name)

        # 스크롤 질의는 scroll 이벤트 카운트 우선
        if any(k in q for k in ["스크롤", "scroll"]):
            for m_name, sc in [("eventCount", 0.95), ("keyEvents", 0.84), ("scrolledUsers", 0.82)]:
                if m_name in seen:
                    for c in candidates:
                        if c.get("name") == m_name:
                            c["score"] = max(c.get("score", 0), sc)
                            c["matched_by"] = "scroll_rule"
                    continue
                meta = GA4_METRICS.get(m_name, {})
                if not meta:
                    continue
                scope = meta.get("scope") or MetricCandidateExtractor._infer_scope_from_category(meta.get("category"))
                candidates.append({
                    "name": m_name,
                    "score": sc,
                    "matched_by": "scroll_rule",
                    "scope": scope,
                    "priority": meta.get("priority", 0)
                })
                seen.add(m_name)

        # 이벤트 파라미터 존재/조회 질의는 eventCount로 기본 조회 가능하게 보강
        event_token = _extract_event_name_token(question)
        if ("후원" in q and "클릭" in q) and not event_token:
            event_token = "donation_click"
        if event_token:
            if "eventCount" in seen:
                for c in candidates:
                    if c.get("name") == "eventCount":
                        c["score"] = max(c.get("score", 0), 0.97)
                        c["matched_by"] = "event_token_metric_rule"
            else:
                meta = GA4_METRICS.get("eventCount", {})
                candidates.append({
                    "name": "eventCount",
                    "score": 0.97,
                    "matched_by": "event_token_metric_rule",
                    "scope": meta.get("scope") or MetricCandidateExtractor._infer_scope_from_category(meta.get("category")),
                    "priority": meta.get("priority", 0)
                })
                seen.add("eventCount")
        param_probe_terms = ["파라미터", "매개변수", "네임", "이름", "name", "값", "없어", "있어", "menu_name", "menu name", "메뉴명", "메뉴 네임"]
        if event_token and any(k in q for k in param_probe_terms):
            if "eventCount" in seen:
                for c in candidates:
                    if c.get("name") == "eventCount":
                        c["score"] = max(c.get("score", 0), 0.90)
                        c["matched_by"] = "event_param_probe_rule"
            else:
                meta = GA4_METRICS.get("eventCount", {})
                candidates.append({
                    "name": "eventCount",
                    "score": 0.90,
                    "matched_by": "event_param_probe_rule",
                    "scope": meta.get("scope") or MetricCandidateExtractor._infer_scope_from_category(meta.get("category")),
                    "priority": meta.get("priority", 0)
                })
                seen.add("eventCount")

        # 이벤트 클릭 발생량 질의는 eventCount를 강하게 우선
        if any(k in q for k in ["클릭", "click"]) and any(k in q for k in ["얼마나", "몇", "건수", "횟수", "일어났", "발생"]):
            if "eventCount" in seen:
                for c in candidates:
                    if c.get("name") == "eventCount":
                        c["score"] = max(c.get("score", 0), 0.98)
                        c["matched_by"] = "click_volume_rule"
            else:
                meta = GA4_METRICS.get("eventCount", {})
                candidates.append({
                    "name": "eventCount",
                    "score": 0.98,
                    "matched_by": "click_volume_rule",
                    "scope": meta.get("scope") or MetricCandidateExtractor._infer_scope_from_category(meta.get("category")),
                    "priority": meta.get("priority", 0)
                })
                seen.add("eventCount")
            for c in candidates:
                if c.get("name") in ["purchaseRevenue", "itemRevenue", "grossItemRevenue", "totalRevenue"]:
                    c["score"] = max(0.0, c.get("score", 0) - 0.35)

        # 파라미터명이 직접 언급된 질문은 eventCount를 기본 지표로 사용
        if any(tok in q for tok in KNOWN_CUSTOM_PARAM_TOKENS):
            if "eventCount" in seen:
                for c in candidates:
                    if c.get("name") == "eventCount":
                        c["score"] = max(c.get("score", 0), 0.90)
                        c["matched_by"] = "custom_param_metric_rule"
            else:
                meta = GA4_METRICS.get("eventCount", {})
                candidates.append({
                    "name": "eventCount",
                    "score": 0.90,
                    "matched_by": "custom_param_metric_rule",
                    "scope": meta.get("scope") or MetricCandidateExtractor._infer_scope_from_category(meta.get("category")),
                    "priority": meta.get("priority", 0)
                })
                seen.add("eventCount")

        # "클릭수" 질의는 page_view가 아니라 이벤트 카운트를 우선
        if "클릭수" in q or ("클릭" in q and any(k in q for k in ["수", "개수", "횟수"])):
            if "eventCount" in seen:
                for c in candidates:
                    if c.get("name") == "eventCount":
                        c["score"] = max(c.get("score", 0), 0.96)
                        c["matched_by"] = "click_count_rule"
            else:
                meta = GA4_METRICS.get("eventCount", {})
                candidates.append({
                    "name": "eventCount",
                    "score": 0.96,
                    "matched_by": "click_count_rule",
                    "scope": meta.get("scope") or MetricCandidateExtractor._infer_scope_from_category(meta.get("category")),
                    "priority": meta.get("priority", 0)
                })
                seen.add("eventCount")
            for c in candidates:
                if c.get("name") in ["screenPageViews", "views"]:
                    c["score"] = max(0.0, c.get("score", 0) - 0.20)

        # 묶기/그룹 질문은 분해 지표(eventCount) 우선, 매출 지표는 후순위
        if any(k in q for k in ["묶어서", "묶어", "group by"]) and not any(k in q for k in ["매출", "수익", "금액", "revenue"]):
            if "eventCount" in seen:
                for c in candidates:
                    if c.get("name") == "eventCount":
                        c["score"] = max(c.get("score", 0), 0.94)
                        c["matched_by"] = "group_by_rule"
            else:
                meta = GA4_METRICS.get("eventCount", {})
                candidates.append({
                    "name": "eventCount",
                    "score": 0.94,
                    "matched_by": "group_by_rule",
                    "scope": meta.get("scope") or MetricCandidateExtractor._infer_scope_from_category(meta.get("category")),
                    "priority": meta.get("priority", 0)
                })
                seen.add("eventCount")
            for c in candidates:
                if c.get("name") in ["itemRevenue", "grossItemRevenue", "purchaseRevenue"]:
                    c["score"] = max(0.0, c.get("score", 0) - 0.22)

        if any(k in question for k in ["반응", "효과", "성과"]):
            for m_name, sc in [("keyEvents", 0.84), ("engagementRate", 0.80), ("eventCount", 0.78)]:
                if m_name in seen:
                    for c in candidates:
                        if c.get("name") == m_name:
                            c["score"] = max(c.get("score", 0), sc)
                            c["matched_by"] = "reaction_semantic_rule"
                    continue
                meta = GA4_METRICS.get(m_name, {})
                scope = meta.get("scope") or MetricCandidateExtractor._infer_scope_from_category(meta.get("category"))
                candidates.append({
                    "name": m_name,
                    "score": sc,
                    "matched_by": "reaction_semantic_rule",
                    "scope": scope,
                    "priority": meta.get("priority", 0)
                })
                seen.add(m_name)
            # 프로그램/항목 반응 질문은 item 스코프 지표도 함께 후보로 제공
            if any(k in question for k in ["프로그램", "항목", "상품", "후원"]):
                for m_name, sc in [("itemRevenue", 0.86), ("transactions", 0.82), ("itemsViewed", 0.80)]:
                    if m_name in seen:
                        for c in candidates:
                            if c.get("name") == m_name:
                                c["score"] = max(c.get("score", 0), sc)
                                c["matched_by"] = "reaction_item_scope_rule"
                        continue
                    meta = GA4_METRICS.get(m_name, {})
                    scope = meta.get("scope") or MetricCandidateExtractor._infer_scope_from_category(meta.get("category"))
                    candidates.append({
                        "name": m_name,
                        "score": sc,
                        "matched_by": "reaction_item_scope_rule",
                        "scope": scope,
                        "priority": meta.get("priority", 0)
                    })
                    seen.add(m_name)
            # 프로그램 질문은 donation_name(커스텀) 기준 event 스코프도 강화
            if any(k in question for k in ["프로그램", "노블클럽", "천원의 힘", "donation_name"]):
                for m_name, sc in [("eventCount", 0.93), ("keyEvents", 0.90), ("purchaseRevenue", 0.82)]:
                    if m_name in seen:
                        for c in candidates:
                            if c.get("name") == m_name:
                                c["score"] = max(c.get("score", 0), sc)
                                c["matched_by"] = "reaction_program_event_rule"
                        continue
                    meta = GA4_METRICS.get(m_name, {})
                    scope = meta.get("scope") or MetricCandidateExtractor._infer_scope_from_category(meta.get("category"))
                    candidates.append({
                        "name": m_name,
                        "score": sc,
                        "matched_by": "reaction_program_event_rule",
                        "scope": scope,
                        "priority": meta.get("priority", 0)
                    })
                    seen.add(m_name)

        # 국가별 비율/구성비 질문은 eventCount 우선
        if any(k in q for k in ["국가", "country"]) and any(k in q for k in ["비율", "비중", "구성비", "점유율"]):
            for m_name, sc in [("eventCount", 0.92), ("totalUsers", 0.80)]:
                if m_name in seen:
                    for c in candidates:
                        if c.get("name") == m_name:
                            c["score"] = max(c.get("score", 0), sc)
                            c["matched_by"] = "country_ratio_rule"
                    continue
                meta = GA4_METRICS.get(m_name, {})
                scope = meta.get("scope") or MetricCandidateExtractor._infer_scope_from_category(meta.get("category"))
                candidates.append({
                    "name": m_name,
                    "score": sc,
                    "matched_by": "country_ratio_rule",
                    "scope": scope,
                    "priority": meta.get("priority", 0)
                })
                seen.add(m_name)

        # 클릭-구매 전환 비율
        if any(k in question for k in ["클릭", "조회"]) and any(k in question for k in ["구매", "후원"]) and any(k in question for k in ["전환", "비율", "율"]):
            for m_name, sc in [("purchaserRate", 0.97), ("purchaseToViewRate", 0.90)]:
                if m_name in seen:
                    for c in candidates:
                        if c.get("name") == m_name:
                            c["score"] = max(c.get("score", 0), sc)
                            c["matched_by"] = "click_purchase_conversion_rule"
                    continue
                meta = GA4_METRICS.get(m_name, {})
                scope = meta.get("scope") or MetricCandidateExtractor._infer_scope_from_category(meta.get("category"))
                candidates.append({
                    "name": m_name,
                    "score": sc,
                    "matched_by": "click_purchase_conversion_rule",
                    "scope": scope,
                    "priority": meta.get("priority", 0)
                })
                seen.add(m_name)

        # 일반 전환율 질의는 rate 계열 지표 우선
        if any(k in q for k in ["전환율", "conversion rate", "전환 비율"]):
            prefer_rates = {"sessionKeyEventRate": 0.98, "purchaserRate": 0.92, "purchaseToViewRate": 0.90}
            for c in candidates:
                n = c.get("name")
                if n in prefer_rates:
                    c["score"] = max(c.get("score", 0), prefer_rates[n])
                    c["matched_by"] = "generic_conversion_rate_rule"
                elif n in {"keyEvents", "eventCount", "activeUsers"}:
                    c["score"] = max(0.0, c.get("score", 0) - 0.28)
            for m_name, sc in prefer_rates.items():
                if m_name in seen:
                    continue
                meta = GA4_METRICS.get(m_name, {})
                scope = meta.get("scope") or MetricCandidateExtractor._infer_scope_from_category(meta.get("category"))
                candidates.append({
                    "name": m_name,
                    "score": sc,
                    "matched_by": "generic_conversion_rate_rule",
                    "scope": scope,
                    "priority": meta.get("priority", 0)
                })
                seen.add(m_name)

        # 주간 비교(지난주 vs 그 전주) + 사용자 계열 질문은 activeUsers 우선
        if ("지난주" in q and ("그 전주" in q or "전주" in q)) and any(k in q for k in ["사용자", "유저"]):
            prefer = {"activeUsers": 0.995, "newUsers": 0.85}
            for c in candidates:
                n = c.get("name")
                if n in prefer:
                    c["score"] = max(c.get("score", 0), prefer[n])
                    c["matched_by"] = "week_compare_users_rule"
                elif n in {"eventCount", "keyEvents", "purchaseRevenue", "itemRevenue"}:
                    c["score"] = max(0.0, c.get("score", 0) - 0.30)
            for m_name, sc in prefer.items():
                if m_name in seen:
                    continue
                meta = GA4_METRICS.get(m_name, {})
                scope = meta.get("scope") or MetricCandidateExtractor._infer_scope_from_category(meta.get("category"))
                candidates.append({
                    "name": m_name,
                    "score": sc,
                    "matched_by": "week_compare_users_rule",
                    "scope": scope,
                    "priority": meta.get("priority", 0)
                })
                seen.add(m_name)

        # 엔티티 비교 질문인데 metric이 비어있을 때 기본 지표 보강
        has_entities = len(_extract_entity_terms(question)) > 0
        if has_entities and not candidates:
            for m_name, sc in [("itemRevenue", 0.84), ("purchaseRevenue", 0.82), ("transactions", 0.80)]:
                meta = GA4_METRICS.get(m_name, {})
                scope = meta.get("scope") or MetricCandidateExtractor._infer_scope_from_category(meta.get("category"))
                candidates.append({
                    "name": m_name,
                    "score": sc,
                    "matched_by": "entity_fallback_metric_rule",
                    "scope": scope,
                    "priority": meta.get("priority", 0)
                })
                seen.add(m_name)

        # purchase 이벤트 매개변수 조회는 eventCount를 기본 지표로 보강
        purchase_param_aliases = [
            "is_regular_donation", "country_name", "domestic_children_count",
            "overseas_children_count", "letter_translation", "donation_name"
        ]
        if (any(k in q for k in ["매개변수", "파라미터", "parameter"]) or any(k in q for k in purchase_param_aliases)) and any(k in q for k in ["purchase", "구매", "후원"]):
            if "eventCount" not in seen:
                meta = GA4_METRICS.get("eventCount", {})
                candidates.append({
                    "name": "eventCount",
                    "score": 0.90,
                    "matched_by": "purchase_param_rule",
                    "scope": meta.get("scope") or MetricCandidateExtractor._infer_scope_from_category(meta.get("category")),
                    "priority": meta.get("priority", 0)
                })
                seen.add("eventCount")

        # "후원명/도네이션명 + 매출" 질의는 purchaseRevenue를 우선
        donation_name_tokens = ["후원 이름", "후원명", "donation_name"]
        revenue_tokens = ["매출", "수익", "revenue", "금액"]
        if any(k in q for k in donation_name_tokens) and any(k in q for k in revenue_tokens):
            if "purchaseRevenue" in seen:
                for c in candidates:
                    if c.get("name") == "purchaseRevenue":
                        c["score"] = max(c.get("score", 0), 0.97)
                        c["matched_by"] = "donation_name_revenue_rule"
            else:
                meta = GA4_METRICS.get("purchaseRevenue", {})
                candidates.append({
                    "name": "purchaseRevenue",
                    "score": 0.97,
                    "matched_by": "donation_name_revenue_rule",
                    "scope": meta.get("scope") or MetricCandidateExtractor._infer_scope_from_category(meta.get("category")),
                    "priority": meta.get("priority", 0)
                })
                seen.add("purchaseRevenue")

        # 후원 이름/후원명 질의는 donation_name 축 이벤트 지표를 우선
        if any(k in q for k in donation_name_tokens):
            for m_name, sc in [("eventCount", 0.94), ("purchaseRevenue", 0.90)]:
                if m_name in seen:
                    for c in candidates:
                        if c.get("name") == m_name:
                            c["score"] = max(c.get("score", 0), sc)
                            c["matched_by"] = "donation_name_axis_rule"
                    continue
                meta = GA4_METRICS.get(m_name, {})
                scope = meta.get("scope") or MetricCandidateExtractor._infer_scope_from_category(meta.get("category"))
                candidates.append({
                    "name": m_name,
                    "score": sc,
                    "matched_by": "donation_name_axis_rule",
                    "scope": scope,
                    "priority": meta.get("priority", 0)
                })
                seen.add(m_name)

        # 프로그램/후원명 + "얼마나" 질의는 매출/구매자 지표 우선
        if any(k in q for k in ["프로그램", "노블클럽", "천원의 힘", "그린노블클럽", "추모기부"]) and any(k in q for k in ["얼마나", "몇", "후원했", "규모"]):
            for m_name, sc in [("purchaseRevenue", 0.96), ("totalPurchasers", 0.90), ("transactions", 0.86)]:
                if m_name in seen:
                    for c in candidates:
                        if c.get("name") == m_name:
                            c["score"] = max(c.get("score", 0), sc)
                            c["matched_by"] = "program_amount_rule"
                    continue
                meta = GA4_METRICS.get(m_name, {})
                scope = meta.get("scope") or MetricCandidateExtractor._infer_scope_from_category(meta.get("category"))
                candidates.append({
                    "name": m_name,
                    "score": sc,
                    "matched_by": "program_amount_rule",
                    "scope": scope,
                    "priority": meta.get("priority", 0)
                })
                seen.add(m_name)

        # "신규/처음 구매자/후원자" 보정:
        # 신규 사용자(newUsers)가 아니라 구매자 계열 지표를 우선한다.
        has_new = any(k in question for k in ["신규", "새로운", "최초", "첫", "처음"])
        has_buyer = any(k in question for k in ["구매자", "구매", "후원자", "후원"])
        if has_new and has_buyer:
            buyer_priority = {
                "firstTimePurchasers": 0.98,
                "totalPurchasers": 0.93,
                "transactions": 0.90
            }
            for c in candidates:
                n = c.get("name")
                if n in buyer_priority:
                    c["score"] = max(c.get("score", 0), buyer_priority[n])
                    c["matched_by"] = "new_buyer_rule"
                if n == "newUsers":
                    c["score"] = max(0.0, c.get("score", 0) - 0.25)
                if n == "activeUsers":
                    c["score"] = max(0.0, c.get("score", 0) - 0.25)
            for m_name, sc in buyer_priority.items():
                if m_name in seen:
                    continue
                meta = GA4_METRICS.get(m_name, {})
                scope = meta.get("scope") or MetricCandidateExtractor._infer_scope_from_category(
                    meta.get("category")
                )
                candidates.append({
                    "name": m_name,
                    "score": sc,
                    "matched_by": "new_buyer_rule",
                    "scope": scope,
                    "priority": meta.get("priority", 0)
                })
                seen.add(m_name)

        # "첫 후원자/첫 구매자 몇 퍼센트"는 비율 지표를 최우선
        percent_tokens = ["퍼센트", "percent", "%", "비율", "율"]
        if has_new and has_buyer and any(k in q for k in percent_tokens):
            prefer_rate = {
                "firstTimePurchaserRate": 0.99,
                "firstTimePurchasers": 0.93,
                "totalPurchasers": 0.92,
            }
            for c in candidates:
                n = c.get("name")
                if n in prefer_rate:
                    c["score"] = max(c.get("score", 0), prefer_rate[n])
                    c["matched_by"] = "first_buyer_rate_rule"
                elif n in {"activeUsers", "newUsers", "purchaseRevenue", "itemRevenue", "purchaserRate", "purchaseToViewRate"}:
                    c["score"] = max(0.0, c.get("score", 0) - 0.30)
            for m_name, sc in prefer_rate.items():
                if m_name in seen:
                    continue
                meta = GA4_METRICS.get(m_name, {})
                scope = meta.get("scope") or MetricCandidateExtractor._infer_scope_from_category(meta.get("category"))
                candidates.append({
                    "name": m_name,
                    "score": sc,
                    "matched_by": "first_buyer_rate_rule",
                    "scope": scope,
                    "priority": meta.get("priority", 0)
                })
                seen.add(m_name)

        # "신규 후원자 비율"도 첫 구매자 비율로 정규화
        if ("신규" in q) and has_buyer and any(k in q for k in percent_tokens):
            for c in candidates:
                n = c.get("name")
                if n == "firstTimePurchaserRate":
                    c["score"] = max(c.get("score", 0), 0.995)
                    c["matched_by"] = "new_buyer_rate_rule"
                elif n in {"purchaserRate", "purchaseToViewRate"}:
                    c["score"] = max(0.0, c.get("score", 0) - 0.35)
            if "firstTimePurchaserRate" not in seen:
                meta = GA4_METRICS.get("firstTimePurchaserRate", {})
                scope = meta.get("scope") or MetricCandidateExtractor._infer_scope_from_category(meta.get("category"))
                candidates.append({
                    "name": "firstTimePurchaserRate",
                    "score": 0.995,
                    "matched_by": "new_buyer_rate_rule",
                    "scope": scope,
                    "priority": meta.get("priority", 0)
                })
                seen.add("firstTimePurchaserRate")

        # "사용자수 + 구매한 사용자" 복합 질의는 activeUsers + totalPurchasers를 모두 제공
        if any(k in q for k in ["사용자수", "사용자 수", "활성 사용자", "사용자"]) and any(k in q for k in ["구매한 사용자", "구매 사용자", "구매자", "후원자", "구매한"]):
            pair_priority = {"activeUsers": 0.95, "totalPurchasers": 0.96}
            for c in candidates:
                n = c.get("name")
                if n in pair_priority:
                    c["score"] = max(c.get("score", 0), pair_priority[n])
                    c["matched_by"] = "user_and_purchaser_rule"
                elif n in {"transactions", "itemRevenue", "purchaseRevenue"}:
                    c["score"] = max(0.0, c.get("score", 0) - 0.25)
            for m_name, sc in pair_priority.items():
                if m_name in seen:
                    continue
                meta = GA4_METRICS.get(m_name, {})
                scope = meta.get("scope") or MetricCandidateExtractor._infer_scope_from_category(meta.get("category"))
                candidates.append({
                    "name": m_name,
                    "score": sc,
                    "matched_by": "user_and_purchaser_rule",
                    "scope": scope,
                    "priority": meta.get("priority", 0)
                })
                seen.add(m_name)

        # "채널/소스/매체 + 구매자수" 질의는 구매자 지표 우선 (매출 지표는 후순위)
        if any(k in q for k in ["채널", "소스", "매체", "유입", "경로"]) and any(k in q for k in ["구매자수", "구매자 수", "구매자", "후원자"]):
            prefer_buyers = {"totalPurchasers": 0.99, "firstTimePurchasers": 0.90}
            for c in candidates:
                n = c.get("name")
                if n in prefer_buyers:
                    c["score"] = max(c.get("score", 0), prefer_buyers[n])
                    c["matched_by"] = "acq_buyer_count_rule"
                elif n in {"purchaseRevenue", "itemRevenue", "grossItemRevenue", "totalRevenue"}:
                    c["score"] = max(0.0, c.get("score", 0) - 0.35)
            for m_name, sc in prefer_buyers.items():
                if m_name in seen:
                    continue
                meta = GA4_METRICS.get(m_name, {})
                scope = meta.get("scope") or MetricCandidateExtractor._infer_scope_from_category(meta.get("category"))
                candidates.append({
                    "name": m_name,
                    "score": sc,
                    "matched_by": "acq_buyer_count_rule",
                    "scope": scope,
                    "priority": meta.get("priority", 0)
                })
                seen.add(m_name)

        # "매출 일으킨 사용자"는 구매자 수 질의로 해석
        if any(k in q for k in ["매출 일으킨", "구매를 일으킨", "구매 일으킨"]) and any(k in q for k in ["사용자", "유저", "사람"]):
            prefer_buyers = {"totalPurchasers": 0.995, "firstTimePurchasers": 0.90}
            for c in candidates:
                n = c.get("name")
                if n in prefer_buyers:
                    c["score"] = max(c.get("score", 0), prefer_buyers[n])
                    c["matched_by"] = "revenue_contributor_user_rule"
                elif n in {"purchaseRevenue", "itemRevenue", "grossItemRevenue"}:
                    c["score"] = max(0.0, c.get("score", 0) - 0.35)
            for m_name, sc in prefer_buyers.items():
                if m_name in seen:
                    continue
                meta = GA4_METRICS.get(m_name, {})
                scope = meta.get("scope") or MetricCandidateExtractor._infer_scope_from_category(meta.get("category"))
                candidates.append({
                    "name": m_name,
                    "score": sc,
                    "matched_by": "revenue_contributor_user_rule",
                    "scope": scope,
                    "priority": meta.get("priority", 0)
                })
                seen.add(m_name)

        # "구매수 + 전체 구매자" 복합 질의는 transactions + totalPurchasers를 모두 제공
        if any(k in q for k in ["와", "과", ","]) and any(k in q for k in ["구매수", "구매 건수", "구매건수", "트랜잭션"]) and any(k in q for k in ["전체 구매자", "구매자", "후원자"]):
            pair_priority = {"transactions": 0.98, "totalPurchasers": 0.97}
            for c in candidates:
                n = c.get("name")
                if n in pair_priority:
                    c["score"] = max(c.get("score", 0), pair_priority[n])
                    c["matched_by"] = "purchase_count_and_total_purchasers_rule"
                elif n in {"activeUsers", "newUsers"}:
                    c["score"] = max(0.0, c.get("score", 0) - 0.20)
            for m_name, sc in pair_priority.items():
                if m_name in seen:
                    continue
                meta = GA4_METRICS.get(m_name, {})
                scope = meta.get("scope") or MetricCandidateExtractor._infer_scope_from_category(meta.get("category"))
                candidates.append({
                    "name": m_name,
                    "score": sc,
                    "matched_by": "purchase_count_and_total_purchasers_rule",
                    "scope": scope,
                    "priority": meta.get("priority", 0)
                })
                seen.add(m_name)

        # "전체 항목/프로그램/메뉴" 질문에서 totalUsers 계열 과매칭 억제
        list_query_terms = [
            "전체 항목", "전체 목록", "전체 프로그램", "프로그램 전체",
            "메뉴 전체", "전체 보여", "전부 보여", "다 보여"
        ]
        if any(t in q for t in list_query_terms) and not any(t in q for t in ["사용자", "유저", "user"]):
            for c in candidates:
                if c.get("name") in {"totalUsers", "activeUsers", "newUsers"}:
                    c["score"] = 0.0
            if any(k in q for k in ["클릭", "click", "메뉴", "프로그램", "이벤트"]) and "eventCount" not in seen:
                meta = GA4_METRICS.get("eventCount", {})
                candidates.append({
                    "name": "eventCount",
                    "score": 0.88,
                    "matched_by": "list_query_event_bias_rule",
                    "scope": meta.get("scope") or MetricCandidateExtractor._infer_scope_from_category(meta.get("category")),
                    "priority": meta.get("priority", 0)
                })
                seen.add("eventCount")

        candidates = [c for c in candidates if c.get("score", 0) > 0]
        
        # Score 기준 정렬
        candidates.sort(key=lambda x: x["score"], reverse=True)
        
        logging.info(f"[MetricExtractor] Found {len(candidates)} candidates")
        for c in candidates[:5]:  # Log top 5
            logging.info(f"  - {c['name']}: {c['score']:.2f} ({c['matched_by']})")
        
        return candidates
    
    @staticmethod
    def _calculate_explicit_score(q: str, metric_name: str, meta: Dict) -> float:
        """명시적 매칭 점수 계산 (0~1)"""
        score = 0.0
        q_norm = _normalize_text(q)
        
        # API key 매칭
        if metric_name.lower() in q:
            score = max(score, 0.85)
        if _normalize_text(metric_name) in q_norm:
            score = max(score, 0.85)
        
        # UI name 매칭
        ui_name = meta.get("ui_name", "").lower()
        if ui_name and not _is_too_short_term(ui_name) and ui_name in q:
            score = max(score, 0.95)
        if ui_name and not _is_too_short_term(ui_name) and _normalize_text(ui_name) in q_norm:
            score = max(score, 0.95)
        
        # Alias 매칭
        for alias in meta.get("aliases", []):
            if _is_too_short_term(alias):
                continue
            if alias.lower() in q:
                score = max(score, 0.90)
            if _normalize_text(alias) in q_norm:
                score = max(score, 0.90)
        
        # kr_semantics 매칭 (약한 매칭)
        for sem in meta.get("kr_semantics", []):
            if _is_too_short_term(sem):
                continue
            if sem.lower() in q:
                score = max(score, 0.70)
            if _normalize_text(sem) in q_norm:
                score = max(score, 0.70)
        
        return score
    
    @staticmethod
    def _infer_scope_from_category(category: str) -> str:
        """Category에서 scope 추론"""
        CATEGORY_TO_SCOPE = {
            "ecommerce": "item",
            "time": "event",
            "event": "event",
            "page": "event",
            "device": "event",
            "geo": "event",
            "traffic": "event",
            "user": "event",
            "ads": "event",
        }
        return CATEGORY_TO_SCOPE.get(category, "event")


# =============================================================================
# Dimension Candidate Extractor
# =============================================================================

class DimensionCandidateExtractor:
    """
    Dimension 후보 추출기
    
    반환 형식:
    [
      {"name": "date", "score": 0.90, "matched_by": "explicit", "scope": "event"},
      {"name": "deviceCategory", "score": 0.75, "matched_by": "semantic", "scope": "event"}
    ]
    """
    
    @staticmethod
    def extract(question: str, semantic=None) -> List[Dict[str, Any]]:
        """질문에서 Dimension 후보 추출"""
        q = question.lower()
        candidates = []
        
        # 1. Explicit matching
        for dim_name, meta in GA4_DIMENSIONS.items():
            score = DimensionCandidateExtractor._calculate_explicit_score(q, dim_name, meta)
            
            if score > 0:
                scope = meta.get("scope") or DimensionCandidateExtractor._infer_scope_from_category(
                    meta.get("category")
                )
                
                candidates.append({
                    "name": dim_name,
                    "score": score,
                    "matched_by": "explicit",
                    "scope": scope,
                    "category": meta.get("category"),
                    "priority": meta.get("priority", 0)
                })
        
        # 2. Semantic matching
        if semantic:
            sem_candidates = semantic.match_dimension(question)
            for sem in sem_candidates:
                name = sem.get("name")
                confidence = sem.get("confidence", 0)
                
                if any(c["name"] == name for c in candidates):
                    continue
                
                if confidence >= 0.25:
                    meta = GA4_DIMENSIONS.get(name, {})
                    scope = meta.get("scope") or DimensionCandidateExtractor._infer_scope_from_category(
                        meta.get("category")
                    )
                    
                    candidates.append({
                        "name": name,
                        "score": confidence,
                        "matched_by": "semantic",
                        "scope": scope,
                        "category": meta.get("category"),
                        "priority": meta.get("priority", 0)
                    })

        # 후원 유형 비교는 itemName 차원을 우선 사용
        donation_keywords = ["후원", "정기후원", "일시후원"]
        ratio_keywords = ["비중", "구성비", "점유율", "나눠줘", "나눠", "비교"]
        if any(k in question for k in donation_keywords) and any(k in question for k in ratio_keywords):
            if not any(c.get("name") == "itemName" for c in candidates):
                candidates.append({
                    "name": "itemName",
                    "score": 0.95,
                    "matched_by": "donation_ratio_rule",
                    "scope": "item",
                    "category": "ecommerce",
                    "priority": GA4_DIMENSIONS.get("itemName", {}).get("priority", 0)
                })

        # 후원 유형 전환 비율 -> 정기후원 여부 차원 우선
        if any(k in question for k in ["후원", "정기", "일시"]) and any(k in question for k in ["전환", "비율", "율"]):
            if not any(c.get("name") == "customEvent:is_regular_donation" for c in candidates):
                candidates.append({
                    "name": "customEvent:is_regular_donation",
                    "score": 0.95,
                    "matched_by": "donation_type_conversion_rule",
                    "scope": "event",
                    "category": "event",
                    "priority": GA4_DIMENSIONS.get("customEvent:is_regular_donation", {}).get("priority", 0)
                })

        # 후원유형 매출 질문은 정기후원 여부 차원 우선
        if any(k in q for k in ["후원 유형", "후원유형"]) and any(k in q for k in ["매출", "수익", "revenue", "금액"]):
            if not any(c.get("name") == "customEvent:is_regular_donation" for c in candidates):
                candidates.append({
                    "name": "customEvent:is_regular_donation",
                    "score": 0.97,
                    "matched_by": "donation_type_revenue_dim_rule",
                    "scope": "event",
                    "category": "event",
                    "priority": GA4_DIMENSIONS.get("customEvent:is_regular_donation", {}).get("priority", 0)
                })
            if not any(c.get("name") == "customEvent:donation_name" for c in candidates):
                candidates.append({
                    "name": "customEvent:donation_name",
                    "score": 0.90,
                    "matched_by": "donation_type_revenue_dim_rule",
                    "scope": "event",
                    "category": "event",
                    "priority": GA4_DIMENSIONS.get("customEvent:donation_name", {}).get("priority", 0)
                })

        # 일반 "유형" 후속 질문은 후원명/상품유형 우선 (Y/N 단독 응답 방지)
        if any(k in q for k in ["유형", "타입", "종류"]) and not any(k in q for k in ["채널", "소스", "매체", "디바이스", "국가", "페이지"]):
            prefer_dims = [("customEvent:donation_name", 0.97), ("itemCategory", 0.93), ("customEvent:is_regular_donation", 0.86)]
            if any(k in q for k in ["상품", "카테고리", "item"]):
                prefer_dims = [("itemCategory", 0.98), ("customEvent:donation_name", 0.90), ("customEvent:is_regular_donation", 0.82)]
            if any(k in q for k in ["후원 이름", "후원이름", "후원명", "donation_name", "이름"]):
                prefer_dims = [("customEvent:donation_name", 0.99), ("itemCategory", 0.90), ("customEvent:is_regular_donation", 0.82)]
            for d_name, sc in prefer_dims:
                if d_name in GA4_DIMENSIONS and not any(c.get("name") == d_name for c in candidates):
                    meta = GA4_DIMENSIONS.get(d_name, {})
                    candidates.append({
                        "name": d_name,
                        "score": sc,
                        "matched_by": "generic_type_followup_rule",
                        "scope": meta.get("scope") or DimensionCandidateExtractor._infer_scope_from_category(meta.get("category")),
                        "category": meta.get("category"),
                        "priority": meta.get("priority", 0)
                    })

        # 메뉴명/메뉴 네임 질의는 customEvent:menu_name 우선
        if any(k in q for k in ["menu_name", "menu name", "메뉴명", "메뉴 네임", "메뉴이름"]):
            if not any(c.get("name") == "customEvent:menu_name" for c in candidates):
                candidates.append({
                    "name": "customEvent:menu_name",
                    "score": 0.97,
                    "matched_by": "menu_name_rule",
                    "scope": "event",
                    "category": "event",
                    "priority": GA4_DIMENSIONS.get("customEvent:menu_name", {}).get("priority", 0)
                })

        # 상품유형/상품 카테고리 질의는 itemCategory 우선
        if any(k in q for k in ["상품유형", "상품 유형", "상품 카테고리", "카테고리별 상품", "유형별 상품"]):
            if not any(c.get("name") == "itemCategory" for c in candidates):
                candidates.append({
                    "name": "itemCategory",
                    "score": 0.98,
                    "matched_by": "item_category_rule",
                    "scope": "item",
                    "category": "ecommerce",
                    "priority": GA4_DIMENSIONS.get("itemCategory", {}).get("priority", 0)
                })

        # 상품 랭킹/최고 매출 질문은 itemName 우선
        if any(k in q for k in ["상품"]) and any(k in q for k in ["가장", "최고", "최저", "높은", "낮은", "1위", "top", "상위"]):
            if not any(c.get("name") == "itemName" for c in candidates):
                candidates.append({
                    "name": "itemName",
                    "score": 0.99,
                    "matched_by": "item_ranking_rule",
                    "scope": "item",
                    "category": "ecommerce",
                    "priority": GA4_DIMENSIONS.get("itemName", {}).get("priority", 0)
                })

        # 소스/매체/광고 유입 질문은 sourceMedium 우선
        if any(k in q for k in ["소스", "매체", "source", "medium", "광고"]):
            if not any(c.get("name") == "sourceMedium" for c in candidates):
                candidates.append({
                    "name": "sourceMedium",
                    "score": 0.97,
                    "matched_by": "source_medium_rule",
                    "scope": "event",
                    "category": "traffic",
                    "priority": GA4_DIMENSIONS.get("sourceMedium", {}).get("priority", 0)
                })
        if any(k in q for k in ["paid", "display", "direct", "organic", "referral", "unassigned", "cross-network"]):
            if not any(c.get("name") == "defaultChannelGroup" for c in candidates):
                candidates.append({
                    "name": "defaultChannelGroup",
                    "score": 0.96,
                    "matched_by": "channel_token_rule",
                    "scope": "event",
                    "category": "traffic",
                    "priority": GA4_DIMENSIONS.get("defaultChannelGroup", {}).get("priority", 0)
                })

        # 후원유형 + 클릭수는 donation_name 우선
        if any(k in q for k in ["후원유형", "후원 유형", "후원명"]) and any(k in q for k in ["클릭", "click"]):
            if not any(c.get("name") == "customEvent:donation_name" for c in candidates):
                candidates.append({
                    "name": "customEvent:donation_name",
                    "score": 0.96,
                    "matched_by": "donation_click_dim_rule",
                    "scope": "event",
                    "category": "event",
                    "priority": GA4_DIMENSIONS.get("customEvent:donation_name", {}).get("priority", 0)
                })

        # donation + click 질의는 donation_name 축 우선
        if "donation" in q and any(k in q for k in ["클릭", "click"]):
            if not any(c.get("name") == "customEvent:donation_name" for c in candidates):
                candidates.append({
                    "name": "customEvent:donation_name",
                    "score": 0.98,
                    "matched_by": "donation_token_click_dim_rule",
                    "scope": "event",
                    "category": "event",
                    "priority": GA4_DIMENSIONS.get("customEvent:donation_name", {}).get("priority", 0)
                })

        # 스크롤 질문은 퍼센트/페이지 차원 보강
        if any(k in q for k in ["스크롤", "scroll"]):
            for d_name, sc in [("customEvent:percent_scrolled", 0.95), ("pagePath", 0.88)]:
                if d_name in GA4_DIMENSIONS and not any(c.get("name") == d_name for c in candidates):
                    meta = GA4_DIMENSIONS.get(d_name, {})
                    candidates.append({
                        "name": d_name,
                        "score": sc,
                        "matched_by": "scroll_dim_rule",
                        "scope": meta.get("scope") or DimensionCandidateExtractor._infer_scope_from_category(meta.get("category")),
                        "category": meta.get("category"),
                        "priority": meta.get("priority", 0)
                    })
        
        candidates.sort(key=lambda x: x["score"], reverse=True)
        
        logging.info(f"[DimensionExtractor] Found {len(candidates)} candidates")
        for c in candidates[:3]:
            logging.info(f"  - {c['name']}: {c['score']:.2f} ({c['matched_by']})")
        
        return candidates
    
    @staticmethod
    def _calculate_explicit_score(q: str, dim_name: str, meta: Dict) -> float:
        """명시적 매칭 점수 계산"""
        score = 0.0
        q_norm = _normalize_text(q)
        
        if dim_name.lower() in q:
            score = max(score, 0.85)
        if _normalize_text(dim_name) in q_norm:
            score = max(score, 0.85)
        
        ui_name = meta.get("ui_name", "").lower()
        if ui_name and not _is_too_short_term(ui_name) and ui_name in q:
            score = max(score, 0.95)
        if ui_name and not _is_too_short_term(ui_name) and _normalize_text(ui_name) in q_norm:
            score = max(score, 0.95)
        
        for alias in meta.get("aliases", []):
            if _is_too_short_term(alias):
                continue
            if alias.lower() in q:
                score = max(score, 0.90)
            if _normalize_text(alias) in q_norm:
                score = max(score, 0.90)
        
        for sem in meta.get("kr_semantics", []):
            if _is_too_short_term(sem):
                continue
            if sem.lower() in q:
                score = max(score, 0.70)
            if _normalize_text(sem) in q_norm:
                score = max(score, 0.70)
        
        return score
    
    @staticmethod
    def _infer_scope_from_category(category: str) -> str:
        CATEGORY_TO_SCOPE = {
            "ecommerce": "item",
            "time": "event",
            "event": "event",
            "page": "event",
            "device": "event",
            "geo": "event",
            "traffic": "event",
            "user": "event",
            "ads": "event",
        }
        return CATEGORY_TO_SCOPE.get(category, "event")


# =============================================================================
# Modifier Extractor
# =============================================================================

class ModifierExtractor:
    """
    질문에서 수정자(Modifiers) 추출
    
    반환 형식:
    {
      "limit": 10,
      "needs_total": True,
      "needs_breakdown": True,
      "scope_hint": ["item"],
      "order_hint": "desc"
    }
    """
    
    @staticmethod
    def extract(question: str) -> Dict[str, Any]:
        """질문에서 modifier 추출"""
        q = question.lower()
        modifiers = {}
        purchase_param_aliases = [
            "is_regular_donation", "country_name", "domestic_children_count",
            "overseas_children_count", "letter_translation", "donation_name"
        ]
        
        # 1. TopN limit
        limit_match = re.search(r'(top\s*(\d+)|상위\s*(\d+)|(\d+)\s*위|1\s*[-~]\s*(\d+)|(\d+)\s*개)', q)
        if limit_match:
            # 매칭된 그룹에서 숫자 추출
            nums = [g for g in limit_match.groups() if g and g.isdigit()]
            if nums:
                modifiers["limit"] = int(nums[0])

        # "가장/최고/최저"는 Top1로 해석
        if any(k in q for k in ["가장", "최고", "최저", "높은", "낮은"]) and any(k in q for k in ["상품", "매출", "이벤트", "후원"]):
            modifiers["limit"] = 1
        
        # 2. "총" / "전체" 키워드
        if any(k in q for k in ["총", "전체", "합계", "total"]):
            modifiers["needs_total"] = True

        # "총 매출 + 상품별 매출" 복합 질의
        if any(k in q for k in ["총 매출", "총매출", "전체 매출"]) and any(k in q for k in ["상품별", "상품 별", "아이템별", "제품별"]):
            modifiers["needs_total"] = True
            modifiers["needs_breakdown"] = True
            modifiers["scope_hint"] = ["item"]

        # "상품별 매출" 단독 질의도 itemName 분해 강제
        if any(k in q for k in ["상품별", "상품 별", "아이템별", "제품별"]) and any(k in q for k in ["매출", "수익", "금액"]):
            modifiers["needs_breakdown"] = True
            modifiers["needs_total"] = False if "총 매출" not in q and "총매출" not in q else modifiers.get("needs_total", False)
            modifiers["scope_hint"] = ["item"]
            modifiers["force_dimensions"] = ["itemName"]
            modifiers["entity_field_hint"] = "itemName"

        # 2.1 "전체 항목/목록"은 합계가 아니라 전체 breakdown 확장으로 해석
        if any(k in q for k in ["전체 항목", "전체 목록", "전체 프로그램", "모든 항목", "전부 보여", "다 보여", "메뉴 전체", "gnb메뉴 전체", "전체 보여줘", "이것 전체", "이거 전체"]):
            modifiers["all_items"] = True
            modifiers["needs_breakdown"] = True
            modifiers["needs_total"] = False
            modifiers.pop("limit", None)
        
        # 3. "~별" / "기준" 키워드
        if any(k in q for k in ["별", "기준", "따라", "by "]):
            modifiers["needs_breakdown"] = True
        if any(k in q for k in ["묶어서", "묶어", "그룹", "group by"]):
            modifiers["needs_breakdown"] = True
            modifiers["needs_total"] = False
        if len(q.strip()) <= 20 and any(k in q for k in ["name", "이름", "네임"]):
            modifiers["needs_breakdown"] = True
            modifiers["needs_total"] = False

        # 3.1 비교/탐색형 자연어는 breakdown
        if any(k in q for k in ["어떤", "무슨", "어디", "가장", "많이", "상위", "top"]) and any(k in q for k in ["후원", "프로그램", "국가", "채널", "카테고리", "유형"]):
            modifiers["needs_breakdown"] = True
            if any(k in q for k in ["가장", "많이"]) and not any(k in q for k in ["top", "상위"]):
                modifiers["limit"] = 5
        if any(k in q for k in ["가장", "최고", "최저", "높은", "낮은", "1위"]) and any(k in q for k in ["상품", "매출", "후원"]):
            modifiers["needs_breakdown"] = True
            modifiers["needs_total"] = True
            modifiers["scope_hint"] = ["item"]
            modifiers["force_dimensions"] = ["itemName"]
            modifiers["entity_field_hint"] = "itemName"
            modifiers["limit"] = 1
        if "국가" in q and any(k in q for k in ["많이", "어디", "어떤"]):
            modifiers["needs_breakdown"] = True

        # 3.15 해외/국내 비교는 국가 기준으로 강제 (국내=South Korea, 해외=기타)
        if "해외" in q and "국내" in q:
            modifiers["needs_breakdown"] = True
            modifiers["force_dimensions"] = ["country"]
            modifiers["scope_hint"] = ["event"]
            modifiers["entity_field_hint"] = "country"
            # itemName 기반 엔티티 필터는 제거 (국가 전체 비교 목적)
            modifiers.pop("entity_contains", None)
            modifiers.pop("item_name_contains", None)

        # 3.5 비중/구성비/비율 요청은 breakdown 강제
        if any(k in q for k in ["비중", "구성비", "비율", "점유율", "나눠줘", "나눠"]):
            modifiers["needs_breakdown"] = True

        # 지난달+이번달 / 지난주+이번주 비교 질의는 시간 차원 breakdown 강제
        if ("지난달" in q and "이번달" in q):
            modifiers["needs_breakdown"] = True
            modifiers["needs_total"] = False
            modifiers["scope_hint"] = ["event"]
            modifiers["force_dimensions"] = ["yearMonth"]
        if ("지난주" in q and "이번주" in q):
            modifiers["needs_breakdown"] = True
            modifiers["needs_total"] = False
            modifiers["scope_hint"] = ["event"]
            modifiers["force_dimensions"] = ["week"]
        if ("지난주" in q and ("그 전주" in q or "전주" in q)) and any(k in q for k in ["사용자", "유저", "세션"]):
            modifiers["needs_trend"] = True
            modifiers["needs_breakdown"] = True
            modifiers["needs_total"] = False
            modifiers["scope_hint"] = ["event"]
            # 추이는 일별 날짜 축을 기본으로 유지한다.
            modifiers["force_dimensions"] = ["date"]
            modifiers["force_metrics"] = ["activeUsers"]

        # 지난주 vs 전주: dimension 없어도 비교 가능하도록 week 축 자동 부여
        if ("지난주" in q and ("그 전주" in q or "전주" in q)):
            modifiers["needs_breakdown"] = True
            modifiers["needs_total"] = False
            modifiers["scope_hint"] = ["event"]
            if "force_dimensions" not in modifiers:
                modifiers["force_dimensions"] = ["week"]
            modifiers["auto_prev_period_compare"] = True

        # 추이 질문은 기본적으로 일별(date) 차원 강제
        if any(k in q for k in ["추이", "흐름", "일별", "변화"]) and "force_dimensions" not in modifiers:
            modifiers["needs_trend"] = True
            modifiers["needs_breakdown"] = True
            modifiers["needs_total"] = False
            modifiers["scope_hint"] = ["event"]
            modifiers["force_dimensions"] = ["date"]
        # 일반 전환율 질의: 금액/완료건수 맥락이 없을 때만 기본 rate 지표 강제
        has_amount_or_count_context = any(
            k in q for k in [
                "금액", "매출", "수익", "완료", "완료건수", "완료 건수",
                "건수", "횟수", "구매수", "트랜잭션", "transaction"
            ]
        )
        if any(k in q for k in ["전환율", "conversion rate", "전환 비율"]) and not has_amount_or_count_context:
            modifiers["force_metrics"] = ["sessionKeyEventRate", "purchaserRate", "purchaseToViewRate"]
        if q.strip() in ["비교", "비교해", "비교해서", "대비", "증감"]:
            modifiers["needs_breakdown"] = True
            modifiers["needs_total"] = False

        # 3.55 채널/소스/매체 축 요청은 breakdown 강제
        if any(k in q for k in ["채널", "소스", "매체", "경로", "source", "medium", "광고", "paid", "display"]):
            modifiers["needs_breakdown"] = True
            modifiers["scope_hint"] = ["event"]
            if any(k in q for k in ["소스", "매체", "source", "medium", "광고"]) and "force_dimensions" not in modifiers:
                modifiers["force_dimensions"] = ["sourceMedium"]
            elif ("경로" in q) and "force_dimensions" not in modifiers:
                modifiers["force_dimensions"] = ["defaultChannelGroup"]

        # 3.555 채널/소스/매체 + 구매자수 질문은 구매자 지표 강제
        if any(k in q for k in ["채널", "소스", "매체", "유입", "경로"]) and any(k in q for k in ["구매자수", "구매자 수", "구매자", "후원자"]):
            modifiers["needs_breakdown"] = True
            modifiers["needs_total"] = False
            modifiers["scope_hint"] = ["event"]
            modifiers["force_metrics"] = ["totalPurchasers"]

        # 3.553 "매출 일으킨 사용자"는 채널별 구매자수로 강제
        if any(k in q for k in ["매출 일으킨", "구매를 일으킨", "구매 일으킨"]) and any(k in q for k in ["사용자", "유저", "사람"]) and any(k in q for k in ["채널", "유입", "경로"]):
            modifiers["needs_breakdown"] = True
            modifiers["needs_total"] = False
            modifiers["scope_hint"] = ["event"]
            modifiers["force_metrics"] = ["totalPurchasers"]
            modifiers["force_dimensions"] = ["defaultChannelGroup"]
            modifiers["event_filter"] = "purchase"

        # 3.552 유입축 + 구매/매출 질문은 purchase 필터 기반 분해
        if any(k in q for k in ["채널", "소스", "매체", "유입", "경로", "source", "medium"]) and any(k in q for k in ["구매", "매출", "수익", "후원"]):
            modifiers["needs_breakdown"] = True
            modifiers["needs_total"] = False
            modifiers["scope_hint"] = ["event"]
            if any(k in q for k in ["소스", "매체", "source", "medium"]):
                modifiers["force_dimensions"] = ["sourceMedium"]
            else:
                modifiers["force_dimensions"] = ["defaultChannelGroup"]
            modifiers["event_filter"] = "purchase"

        # 3.551 사용자수 + 구매자 복합 질의는 2개 지표를 고정
        if any(k in q for k in ["사용자수", "사용자 수", "활성 사용자", "사용자"]) and any(k in q for k in ["구매한 사용자", "구매 사용자", "구매자", "후원자", "구매한"]):
            modifiers["scope_hint"] = ["event"]
            modifiers["force_metrics"] = ["activeUsers", "totalPurchasers"]

        # 3.551b 구매수 + 전체 구매자 복합 질의
        if any(k in q for k in ["구매수", "구매 건수", "구매건수", "트랜잭션"]) and any(k in q for k in ["전체 구매자", "구매자", "후원자"]) and any(k in q for k in ["와", "과", ","]):
            modifiers["scope_hint"] = ["event"]
            modifiers["force_metrics"] = ["transactions", "totalPurchasers"]
            modifiers["needs_total"] = True
            modifiers["needs_breakdown"] = False
            modifiers["suppress_entity_filters"] = True
            modifiers.pop("entity_contains", None)
            modifiers.pop("item_name_contains", None)

        # "총 후원금액 + 후원 완료 건수 + 후원 전환율" 류 복합 질의
        # 전환율은 purchase / donation_click(완료/클릭) 계산으로 별도 블록에서 산출
        has_conversion = any(k in q for k in ["전환율", "전환 비율", "conversion rate"])
        has_total_amount = any(k in q for k in ["총 후원금액", "총후원금액", "총 매출", "총매출", "후원금액", "후원 금액"])
        has_completion = any(k in q for k in ["완료 건수", "완료건수", "후원 완료", "구매 건수", "구매수", "트랜잭션"])
        if has_conversion and (has_total_amount or has_completion):
            modifiers["scope_hint"] = ["event"]
            modifiers["needs_total"] = True
            modifiers["needs_breakdown"] = False
            modifiers["event_filter"] = "purchase"
            # 완료 건수는 purchase 이벤트의 eventCount로 해석
            modifiers["force_metrics"] = ["purchaseRevenue", "eventCount"]
            modifiers["needs_conversion_block"] = True
            modifiers["suppress_entity_filters"] = True
            modifiers.pop("entity_contains", None)
            modifiers.pop("item_name_contains", None)

        # "어떤 경로에서 ... 구매를 많이"는 구매자 지표 우선
        if "경로" in q and any(k in q for k in ["구매", "후원"]) and any(k in q for k in ["사용자", "유저", "사람"]):
            modifiers["scope_hint"] = ["event"]
            modifiers["force_metrics"] = ["totalPurchasers"]
            modifiers["needs_breakdown"] = True
            modifiers["needs_total"] = False
            if "force_dimensions" not in modifiers:
                modifiers["force_dimensions"] = ["defaultChannelGroup"]
            modifiers["event_filter"] = "purchase"

        product_type_query = any(k in q for k in ["상품유형", "상품 유형", "상품 카테고리", "카테고리별 상품"]) or ("상품" in q and "유형" in q)
        # 3.56 상품유형 질의는 itemCategory 기준 breakdown 강제
        if product_type_query:
            modifiers["needs_breakdown"] = True
            modifiers["needs_total"] = False
            modifiers["scope_hint"] = ["item"]
            modifiers["force_dimensions"] = ["itemCategory"]
            modifiers["entity_field_hint"] = "itemCategory"

        # 3.6 엔티티 추출: 브랜드/캠페인/상품명/후원명 등 contains 필터
        domestic_overseas_case = ("해외" in q and "국내" in q)
        entity_terms = _extract_entity_terms(question)
        if entity_terms and not domestic_overseas_case and not modifiers.get("suppress_entity_filters"):
            uniq = []
            seen = set()
            for t in entity_terms:
                tt = t.strip()
                if tt and tt not in seen:
                    seen.add(tt)
                    uniq.append(tt)
            if uniq:
                modifiers["item_name_contains"] = uniq[:3]
                modifiers["entity_contains"] = uniq[:3]
                modifiers["needs_breakdown"] = True
                scope_hint = modifiers.get("scope_hint", [])
                # 질문 어휘로 필드 힌트 추정
                if any(k in q for k in ["캠페인", "채널", "소스", "매체", "랜딩", "페이지", "이벤트"]):
                    if any(k in q for k in ["캠페인"]):
                        modifiers["entity_field_hint"] = "defaultChannelGroup"
                    elif any(k in q for k in ["채널"]):
                        modifiers["entity_field_hint"] = "defaultChannelGroup"
                    elif any(k in q for k in ["소스", "매체"]):
                        if any(t in q for t in ["display", "paid", "organic", "direct", "referral", "unassigned", "cross-network"]):
                            modifiers["entity_field_hint"] = "defaultChannelGroup"
                        else:
                            modifiers["entity_field_hint"] = "sourceMedium"
                    elif any(k in q for k in ["랜딩", "페이지"]):
                        modifiers["entity_field_hint"] = "landingPage"
                    elif any(k in q for k in ["이벤트"]):
                        modifiers["entity_field_hint"] = "eventName"
                    if "event" not in scope_hint:
                        scope_hint.append("event")
                else:
                    if "entity_field_hint" not in modifiers:
                        modifiers["entity_field_hint"] = "itemBrand" if any(k in q for k in ["브랜드"]) else "itemName"
                    if "item" not in scope_hint:
                        scope_hint.append("item")
                modifiers["scope_hint"] = scope_hint

        # source/medium 분석에서 채널 토큰(display/paid/organic...)이 있으면
        # 분해는 sourceMedium으로, 필터는 defaultChannelGroup으로 고정
        if any(k in q for k in ["소스", "매체", "source", "medium"]) and any(t in q for t in ["display", "paid", "organic", "direct", "referral", "unassigned", "cross-network"]):
            modifiers["needs_breakdown"] = True
            modifiers["scope_hint"] = ["event"]
            modifiers["force_dimensions"] = ["sourceMedium"]
            modifiers["entity_field_hint"] = "defaultChannelGroup"

        # 3.7 "매개변수/정보" 요청이면 item 프로파일용 차원 강제
        if any(k in q for k in ["매개변수", "파라미터", "상세", "정보", "어떤 것을 더 알 수", "무엇을 더 알 수"]):
            if any(k in q for k in ["후원", "상품", "아이템", "항목"]):
                modifiers["needs_profile"] = True
                modifiers["force_dimensions"] = ["itemName", "itemCategory", "itemBrand", "itemVariant"]

        # 3.75 이벤트 클릭 항목 탐색 (e.g., gnb_click 어떤 항목 많이?)
        event_token = _extract_event_name_token(question)
        click_terms = ["클릭", "눌", "tap", "click"]

        # 후원 클릭 명시 질의는 donation_click으로 정규화
        if ("후원" in q and "클릭" in q) and not event_token:
            event_token = "donation_click"

        if any(k in q for k in click_terms) and any(k in q for k in ["항목", "무엇", "뭐", "어떤", "많이", "상위"]):
            modifiers["needs_breakdown"] = True
            modifiers["needs_total"] = False
            modifiers["scope_hint"] = ["event"]
            modifiers["prefer_event_scope"] = True
            force_dims = modifiers.get("force_dimensions", [])
            for d in ["customEvent:menu_name", "linkText", "linkUrl", "customEvent:donation_name", "eventName"]:
                if d in GA4_DIMENSIONS and d not in force_dims:
                    force_dims.append(d)
            modifiers["force_dimensions"] = force_dims
            if event_token:
                modifiers["event_filter"] = event_token
            if "entity_field_hint" not in modifiers:
                modifiers["entity_field_hint"] = "linkText"

        # 클릭 발생량 질문은 eventCount + eventName 분해
        if any(k in q for k in click_terms) and any(k in q for k in ["얼마나", "몇", "건수", "횟수", "일어났"]):
            modifiers["needs_breakdown"] = True
            modifiers["needs_total"] = False
            modifiers["scope_hint"] = ["event"]
            force_dims = modifiers.get("force_dimensions", [])
            for d in ["eventName"]:
                if d in GA4_DIMENSIONS and d not in force_dims:
                    force_dims.append(d)
            modifiers["force_dimensions"] = force_dims
            if event_token:
                modifiers["event_filter"] = event_token

        # 3.76 이벤트명 + 메뉴명(파라미터) 조회
        if event_token and any(k in q for k in ["menu_name", "menu name", "메뉴명", "메뉴 네임", "메뉴이름"]):
            modifiers["needs_breakdown"] = True
            modifiers["needs_total"] = False
            modifiers["scope_hint"] = ["event"]
            modifiers["event_filter"] = event_token
            force_dims = modifiers.get("force_dimensions", [])
            for d in ["customEvent:menu_name", "eventName"]:
                if d in GA4_DIMENSIONS and d not in force_dims:
                    force_dims.append(d)
            modifiers["force_dimensions"] = force_dims
            modifiers["entity_field_hint"] = "customEvent:menu_name"

        # 3.77 파라미터명 직접 질의 시 해당 customEvent 차원을 최우선으로 고정
        explicit_param_map = {
            "menu_name": ["menu_name", "menu name", "메뉴명", "메뉴 네임", "메뉴이름"],
            "button_name": ["button_name", "버튼명", "버튼 이름"],
            "banner_name": ["banner_name", "배너명", "배너 이름"],
            "click_button": ["click_button", "클릭버튼"],
            "click_location": ["click_location", "클릭위치"],
            "click_section": ["click_section", "클릭섹션"],
            "click_text": ["click_text", "클릭텍스트", "클릭 문자열"],
            "content_category": ["content_category", "콘텐츠카테고리"],
            "content_name": ["content_name", "콘텐츠명", "콘텐츠이름"],
            "content_type": ["content_type", "콘텐츠유형", "콘텐츠 타입"],
            "country_name": ["country_name", "국가명"],
            "detail_category": ["detail_category", "상세카테고리"],
            "donation_name": ["donation_name", "후원명", "후원 이름"],
            "event_category": ["event_category", "event category", "이벤트 카테고리"],
            "event_label": ["event_label", "event label", "이벤트 라벨"],
            "is_regular_donation": ["is_regular_donation", "정기후원여부"],
            "letter_translation": ["letter_translation", "편지번역", "번역여부"],
            "main_category": ["main_category", "메인카테고리"],
            "payment_type": ["payment_type", "결제유형", "결제 타입"],
            "percent_scrolled": ["percent_scrolled", "스크롤비율"],
            "referrer_host": ["referrer_host", "리퍼러 호스트", "유입호스트"],
            "referrer_pathname": ["referrer_pathname", "리퍼러 경로", "유입경로"],
            "step": ["step", "스텝", "단계"],
            "sub_category": ["sub_category", "서브카테고리"],
            "domestic_children_count": ["domestic_children_count", "국내아동수"],
            "overseas_children_count": ["overseas_children_count", "해외아동수"],
        }
        selected_param = None
        for param_name, aliases in explicit_param_map.items():
            if any(a in q for a in aliases):
                selected_param = param_name
                break
        if selected_param and not product_type_query:
            modifiers["needs_breakdown"] = True
            modifiers["needs_total"] = False
            modifiers["scope_hint"] = ["event"]
            force_dim = f"customEvent:{selected_param}"
            modifiers["force_dimensions"] = [force_dim, "eventName"] if force_dim in GA4_DIMENSIONS else ["eventName"]
            modifiers["entity_field_hint"] = force_dim if force_dim in GA4_DIMENSIONS else "eventName"
            if event_token:
                modifiers["event_filter"] = event_token
            if selected_param in {"donation_name", "menu_name"} and any(k in q for k in ["묶어서", "묶어", "별", "기준"]):
                if force_dim in GA4_DIMENSIONS:
                    modifiers["force_dimensions"] = [force_dim]

        # 3.78 후원유형 클릭수는 donation_click 기준으로 분해
        if any(k in q for k in ["후원유형", "후원 유형", "후원명"]) and any(k in q for k in ["클릭수", "클릭", "click"]) and not any(k in q for k in ["매출", "수익", "금액", "revenue"]):
            modifiers["needs_breakdown"] = True
            modifiers["needs_total"] = False
            modifiers["scope_hint"] = ["event"]
            force_dims = modifiers.get("force_dimensions", [])
            for d in ["customEvent:donation_name", "eventName"]:
                if d in GA4_DIMENSIONS and d not in force_dims:
                    force_dims.append(d)
            modifiers["force_dimensions"] = force_dims
            modifiers["event_filter"] = "donation_click"
            modifiers["entity_field_hint"] = "customEvent:donation_name"

        if "donation" in q and any(k in q for k in ["클릭", "click"]):
            modifiers["needs_breakdown"] = True
            modifiers["needs_total"] = False
            modifiers["scope_hint"] = ["event"]
            modifiers["event_filter"] = "donation_click"
            modifiers["force_dimensions"] = ["customEvent:donation_name"]
            modifiers["entity_field_hint"] = "customEvent:donation_name"

        # "정기후원의 클릭수" 류 질의도 donation_click 기준으로 강제
        if re.search(r"[가-힣A-Za-z0-9_]+후원", question) and any(k in q for k in ["클릭수", "클릭", "click"]) and not any(k in q for k in ["메뉴", "gnb", "lnb", "footer"]):
            modifiers["needs_breakdown"] = True
            modifiers["needs_total"] = False
            modifiers["scope_hint"] = ["event"]
            modifiers["event_filter"] = "donation_click"
            force_dims = modifiers.get("force_dimensions", [])
            for d in ["customEvent:donation_name"]:
                if d in GA4_DIMENSIONS and d not in force_dims:
                    force_dims.append(d)
            modifiers["force_dimensions"] = force_dims
            modifiers["force_metrics"] = ["eventCount"]
            modifiers["entity_field_hint"] = "customEvent:donation_name"

        # 3.78b 후원유형 매출은 purchase + 정기후원여부로 분해
        if any(k in q for k in ["후원유형", "후원 유형"]) and any(k in q for k in ["매출", "수익", "금액", "revenue"]):
            modifiers["needs_breakdown"] = True
            modifiers["needs_total"] = False
            modifiers["scope_hint"] = ["event"]
            modifiers["event_filter"] = "purchase"
            modifiers["force_dimensions"] = ["customEvent:is_regular_donation"]
            modifiers["entity_field_hint"] = "customEvent:is_regular_donation"

        # 이벤트 종류/목록은 eventName 기준으로 강제
        if any(k in q for k in ["이벤트 종류", "이벤트 목록", "무슨 이벤트", "어떤 이벤트"]):
            modifiers["needs_breakdown"] = True
            modifiers["needs_total"] = False
            modifiers["scope_hint"] = ["event"]
            modifiers["force_dimensions"] = ["eventName"]
            modifiers["force_metrics"] = ["eventCount"]
            modifiers["entity_field_hint"] = "eventName"

        # 3.78c 일반 "유형" 후속 질문은 유형 차원 breakdown으로 유도
        if any(k in q for k in ["유형", "타입", "종류"]) and not any(k in q for k in ["채널", "소스", "매체", "디바이스", "국가", "페이지"]) and "force_dimensions" not in modifiers:
            modifiers["needs_breakdown"] = True
            modifiers["needs_total"] = False
            modifiers["scope_hint"] = ["event"]
            if any(k in q for k in ["매출", "수익", "구매", "후원"]):
                modifiers["event_filter"] = "purchase"
            if any(k in q for k in ["후원 이름", "후원이름", "후원명", "donation_name", "이름"]) and "customEvent:donation_name" in GA4_DIMENSIONS:
                modifiers["force_dimensions"] = ["customEvent:donation_name"]
                modifiers["entity_field_hint"] = "customEvent:donation_name"
            elif any(k in q for k in ["상품", "카테고리", "item"]) and "itemCategory" in GA4_DIMENSIONS:
                modifiers["force_dimensions"] = ["itemCategory"]
                modifiers["entity_field_hint"] = "itemCategory"
            elif "customEvent:donation_name" in GA4_DIMENSIONS:
                modifiers["force_dimensions"] = ["customEvent:donation_name"]
                modifiers["entity_field_hint"] = "customEvent:donation_name"

        # 3.905 첫 후원자/첫 구매자 비율 질문은 비율 지표 중심
        if any(k in q for k in ["첫", "최초", "처음", "신규"]) and any(k in q for k in ["후원자", "구매자"]) and any(k in q for k in ["퍼센트", "percent", "%", "비율", "율"]):
            modifiers["force_metrics"] = ["firstTimePurchaserRate", "firstTimePurchasers", "totalPurchasers"]

        # 3.79 scroll 질의: 이벤트/퍼센트(페이지별이면 pagePath 포함)
        if any(k in q for k in ["스크롤", "scroll"]):
            modifiers["needs_breakdown"] = True
            modifiers["needs_total"] = False
            modifiers["scope_hint"] = ["event"]
            modifiers["event_filter"] = "scroll"
            force_dims = modifiers.get("force_dimensions", [])
            for d in ["customEvent:percent_scrolled", "eventName"]:
                if d in GA4_DIMENSIONS and d not in force_dims:
                    force_dims.append(d)
            if any(k in q for k in ["페이지별", "페이지", "page"]):
                for d in ["pagePath"]:
                    if d in GA4_DIMENSIONS and d not in force_dims:
                        force_dims.append(d)
            modifiers["force_dimensions"] = force_dims
            modifiers["entity_field_hint"] = "customEvent:percent_scrolled"

        # 3.80 event token 정정 follow-up (예: donation_click말하는건데)
        if event_token and any(k in q for k in ["말하는", "말한", "그거", "아니", "이벤트"]):
            modifiers["needs_breakdown"] = True
            modifiers["needs_total"] = False
            modifiers["scope_hint"] = ["event"]
            modifiers["event_filter"] = event_token

        # 3.81 purchase vs donation_click 동시 비교 (donation_name 기준)
        if "donation_click" in q and any(k in q for k in ["purchase", "구매"]) and any(k in q for k in ["donation_name", "후원명", "name", "구분"]):
            modifiers["needs_breakdown"] = True
            modifiers["needs_total"] = False
            modifiers["scope_hint"] = ["event"]
            modifiers["event_filters"] = ["purchase", "donation_click"]
            modifiers["force_dimensions"] = ["eventName", "customEvent:donation_name"]
            modifiers["entity_field_hint"] = "eventName"
            modifiers["suppress_purchase_param_rule"] = True
            modifiers.pop("event_filter", None)
            modifiers.pop("entity_contains", None)
            modifiers.pop("item_name_contains", None)

        # 3.8 purchase 커스텀 파라미터 조회
        if (not modifiers.get("suppress_purchase_param_rule")) and any(k in q for k in ["purchase", "구매", "후원"]) and any(k in q for k in ["매개변수", "파라미터", "parameter"] + purchase_param_aliases):
            modifiers["needs_breakdown"] = True
            modifiers["entity_field_hint"] = "eventName"
            modifiers["event_filter"] = "purchase"
            force_dims = modifiers.get("force_dimensions", [])
            for d in [
                "eventName",
                "customEvent:is_regular_donation",
                "customEvent:country_name",
                "customEvent:domestic_children_count",
                "customEvent:overseas_children_count",
                "customEvent:letter_translation",
                "customEvent:donation_name",
            ]:
                if d not in force_dims:
                    force_dims.append(d)
            modifiers["force_dimensions"] = force_dims
            scope_hint = modifiers.get("scope_hint", [])
            if "event" not in scope_hint:
                scope_hint.append("event")
            modifiers["scope_hint"] = scope_hint

        # 3.85 국가별 + 엔티티(프로그램/후원명) 질의는 country breakdown + donation_name 필터 우선
        if any(k in q for k in ["국가별", "국가", "해외", "국내"]) and ("entity_contains" in modifiers or "item_name_contains" in modifiers):
            modifiers["needs_breakdown"] = True
            modifiers["scope_hint"] = ["event"]
            force_dims = modifiers.get("force_dimensions", [])
            for d in ["country"]:
                if d in GA4_DIMENSIONS and d not in force_dims:
                    force_dims.append(d)
            modifiers["force_dimensions"] = force_dims
            if "entity_field_hint" not in modifiers or modifiers.get("entity_field_hint") == "itemName":
                modifiers["entity_field_hint"] = "customEvent:donation_name"

        # 3.9 "어떤 후원 이름으로 매출" -> donation_name x purchaseRevenue
        donation_name_tokens = ["후원 이름", "후원명", "donation_name"]
        revenue_tokens = ["매출", "수익", "revenue", "금액"]
        if (not modifiers.get("event_filters")) and any(k in q for k in donation_name_tokens) and any(k in q for k in revenue_tokens):
            modifiers["needs_breakdown"] = True
            modifiers["event_filter"] = "purchase"
            modifiers["force_dimensions"] = ["customEvent:donation_name"]
            modifiers["entity_field_hint"] = "customEvent:donation_name"
            scope_hint = modifiers.get("scope_hint", [])
            if "event" not in scope_hint:
                scope_hint.append("event")
            modifiers["scope_hint"] = scope_hint

        # 3.10 프로그램 명 질문은 donation_name 파라미터 우선
        if any(k in q for k in ["프로그램", "노블클럽", "천원의 힘", "donation_name"]):
            modifiers["needs_breakdown"] = True
            # 매출/구매 맥락일 때만 purchase 필터를 건다.
            if (not modifiers.get("event_filters")) and any(k in q for k in ["매출", "수익", "구매", "purchase", "얼마나", "몇", "후원했", "규모"]):
                modifiers["event_filter"] = "purchase"
            force_dims = modifiers.get("force_dimensions", [])
            for d in ["customEvent:donation_name"]:
                if d not in force_dims:
                    force_dims.append(d)
            modifiers["force_dimensions"] = force_dims
            modifiers["entity_field_hint"] = "customEvent:donation_name"
            modifiers["scope_hint"] = ["event"]

        # 3.10b 후원 이름/후원명 질문은 donation_name 축 강제
        if any(k in q for k in ["후원 이름", "후원이름", "후원명", "donation_name"]):
            modifiers["needs_breakdown"] = True
            modifiers["needs_total"] = False
            modifiers["scope_hint"] = ["event"]
            modifiers["force_dimensions"] = ["customEvent:donation_name"]
            modifiers["entity_field_hint"] = "customEvent:donation_name"
            if (not modifiers.get("event_filters")) and any(k in q for k in ["매출", "수익", "구매", "얼마나", "몇", "후원했"]):
                modifiers["event_filter"] = "purchase"

        # 3.11 후원 유형 전환율(클릭->구매) 질문
        if any(k in q for k in ["후원 유형", "정기", "일시"]) and any(k in q for k in ["전환", "비율", "율"]) and any(k in q for k in ["클릭", "구매"]):
            modifiers["needs_breakdown"] = True
            modifiers["scope_hint"] = ["event"]
            force_dims = modifiers.get("force_dimensions", [])
            for d in ["customEvent:is_regular_donation", "eventName"]:
                if d not in force_dims:
                    force_dims.append(d)
            modifiers["force_dimensions"] = force_dims
            modifiers["entity_field_hint"] = "eventName"
        
        # 4. Scope hint
        scope_hints = []
        if any(k in q for k in ["상품", "아이템", "제품", "item", "항목"]):
            scope_hints.append("item")
        if any(k in q for k in ["사용자", "유저", "user"]):
            scope_hints.append("user")
        if scope_hints and "scope_hint" not in modifiers:
            modifiers["scope_hint"] = scope_hints

        if modifiers.get("prefer_event_scope"):
            modifiers["scope_hint"] = ["event"]
            modifiers.pop("prefer_event_scope", None)

        # 국내/해외 비교는 항상 국가 기준으로 정규화
        if "해외" in q and "국내" in q:
            modifiers["needs_breakdown"] = True
            modifiers["force_dimensions"] = ["country"]
            modifiers["scope_hint"] = ["event"]
            modifiers["entity_field_hint"] = "country"
            modifiers.pop("entity_contains", None)
            modifiers.pop("item_name_contains", None)
        
        # 5. Order hint
        if any(k in q for k in ["높은", "많은", "큰", "상위", "top"]):
            modifiers["order_hint"] = "desc"
        elif any(k in q for k in ["낮은", "적은", "작은", "하위", "bottom"]):
            modifiers["order_hint"] = "asc"

        # internal flag cleanup
        modifiers.pop("suppress_purchase_param_rule", None)
        
        logging.info(f"[ModifierExtractor] Extracted: {modifiers}")
        return modifiers


# =============================================================================
# Follow-up / Drill-down Helpers
# =============================================================================

def _is_drilldown_followup(question: str) -> bool:
    q = (question or "").lower()
    tokens = ["더 내려", "세분", "드릴다운", "상세", "깊게", "나눠서", "기준으로", "원인", "이유", "왜"]
    dim_tokens = ["채널", "소스", "매체", "경로", "국가", "디바이스", "상품", "카테고리", "후원명", "donation_name"]
    return any(t in q for t in tokens) or any(t in q for t in dim_tokens)


def _infer_drilldown_dimension(question: str, last_state: Optional[Dict]) -> Optional[str]:
    q = (question or "").lower()
    if any(k in q for k in ["소스/매체", "source/medium"]):
        return "sourceMedium"
    if any(k in q for k in ["소스", "source"]):
        return "source"
    if any(k in q for k in ["매체", "medium", "경로", "유입"]):
        return "sourceMedium"
    if "채널" in q:
        return "defaultChannelGroup"
    if any(k in q for k in ["캠페인", "campaign"]):
        for cand in ["campaignName", "sessionCampaignName", "firstUserCampaignName", "customEvent:campaign_name"]:
            if cand in GA4_DIMENSIONS:
                return cand
        return "sourceMedium"
    if any(k in q for k in ["국가", "country"]):
        return "country"
    if any(k in q for k in ["디바이스", "기기", "device"]):
        return "deviceCategory"
    if any(k in q for k in ["상품", "카테고리"]):
        return "itemName"
    if any(k in q for k in ["후원명", "donation_name"]):
        return "customEvent:donation_name"
    # 차원 미지정 드릴다운은 acquisition 계층(channel→source→source/medium→campaign)로 이동
    if last_state and isinstance(last_state.get("dimensions"), list) and last_state.get("dimensions"):
        prev_dim = (last_state["dimensions"][0] or {}).get("name")
        if any(k in q for k in ["더 내려", "세분", "드릴다운", "상세", "깊게"]):
            campaign_dim = None
            for cand in ["campaignName", "sessionCampaignName", "firstUserCampaignName", "customEvent:campaign_name"]:
                if cand in GA4_DIMENSIONS:
                    campaign_dim = cand
                    break
            hierarchy = ["defaultChannelGroup", "source", "sourceMedium"] + ([campaign_dim] if campaign_dim else [])
            alias = {
                "sessionDefaultChannelGroup": "defaultChannelGroup",
                "sessionSource": "source",
                "sessionSourceMedium": "sourceMedium",
            }
            normalized_prev = alias.get(prev_dim, prev_dim)
            if normalized_prev in hierarchy:
                idx = hierarchy.index(normalized_prev)
                if idx + 1 < len(hierarchy):
                    return hierarchy[idx + 1]
            if normalized_prev == "defaultChannelGroup":
                return "source"
            if normalized_prev in ["source", "sourceMedium"]:
                return "sourceMedium"
    return None


# =============================================================================
# Main Orchestrator
# =============================================================================

class CandidateExtractor:
    """
    전체 후보 추출 오케스트레이터
    
    사용법:
    extractor = CandidateExtractor()
    result = extractor.extract(question, semantic=semantic_matcher)
    
    result = {
      "intent": "topn",
      "metric_candidates": [...],
      "dimension_candidates": [...],
      "date_range": {"start_date": "...", "end_date": "..."},
      "modifiers": {...}
    }
    """
    
    def __init__(self):
        pass

    @staticmethod
    def _use_local_intent_parser() -> bool:
        value = os.getenv("USE_LOCAL_INTENT_PARSER", "").strip().lower()
        return value in {"1", "true", "yes", "y", "on"}

    @staticmethod
    def _merge_local_intent(
        question: str,
        intent: str,
        metric_candidates: List[Dict[str, Any]],
        dimension_candidates: List[Dict[str, Any]],
        modifiers: Dict[str, Any]
    ) -> Dict[str, Any]:
        if not CandidateExtractor._use_local_intent_parser():
            return {
                "intent": intent,
                "metric_candidates": metric_candidates,
                "dimension_candidates": dimension_candidates,
                "modifiers": modifiers
            }

        try:
            from ollama_intent_parser import extract_intent

            llm = extract_intent(question) or {}
            logging.info(f"[CandidateExtractor] Local parser: {llm}")
        except Exception as e:
            logging.warning(f"[CandidateExtractor] Local parser skipped: {e}")
            return {
                "intent": intent,
                "metric_candidates": metric_candidates,
                "dimension_candidates": dimension_candidates,
                "modifiers": modifiers
            }

        allowed_intents = {
            "metric_single", "metric_multi", "breakdown", "topn",
            "trend", "comparison", "category_list"
        }
        llm_intent = str(llm.get("intent") or "").strip()
        if llm_intent in allowed_intents:
            intent = llm_intent

        llm_limit = llm.get("limit")
        if isinstance(llm_limit, int) and llm_limit > 0:
            modifiers["limit"] = llm_limit

        metric_index = {c.get("name"): idx for idx, c in enumerate(metric_candidates)}
        for raw_metric in llm.get("metrics", []) or []:
            metric_name = _resolve_metric_name(raw_metric)
            if not metric_name:
                continue
            meta = GA4_METRICS.get(metric_name, {})
            scope = meta.get("scope") or MetricCandidateExtractor._infer_scope_from_category(meta.get("category"))
            if metric_name in metric_index:
                idx = metric_index[metric_name]
                metric_candidates[idx]["score"] = max(metric_candidates[idx].get("score", 0), 0.92)
                metric_candidates[idx]["matched_by"] = "local_llm"
            else:
                metric_candidates.append({
                    "name": metric_name,
                    "score": 0.92,
                    "matched_by": "local_llm",
                    "scope": scope,
                    "priority": meta.get("priority", 0)
                })
                metric_index[metric_name] = len(metric_candidates) - 1

        dim_index = {c.get("name"): idx for idx, c in enumerate(dimension_candidates)}
        for raw_dim in llm.get("dimensions", []) or []:
            dim_name = _resolve_dimension_name(raw_dim)
            if not dim_name:
                continue
            meta = GA4_DIMENSIONS.get(dim_name, {})
            scope = meta.get("scope") or DimensionCandidateExtractor._infer_scope_from_category(meta.get("category"))
            if dim_name in dim_index:
                idx = dim_index[dim_name]
                dimension_candidates[idx]["score"] = max(dimension_candidates[idx].get("score", 0), 0.90)
                dimension_candidates[idx]["matched_by"] = "local_llm"
            else:
                dimension_candidates.append({
                    "name": dim_name,
                    "score": 0.90,
                    "matched_by": "local_llm",
                    "scope": scope,
                    "priority": meta.get("priority", 0)
                })
                dim_index[dim_name] = len(dimension_candidates) - 1

        return {
            "intent": intent,
            "metric_candidates": metric_candidates,
            "dimension_candidates": dimension_candidates,
            "modifiers": modifiers
        }

    @staticmethod
    def _build_matching_debug(
        intent: str,
        metric_candidates: List[Dict[str, Any]],
        dimension_candidates: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        metric_local = [c for c in metric_candidates if c.get("matched_by") == "local_llm"]
        dim_local = [c for c in dimension_candidates if c.get("matched_by") == "local_llm"]
        top_metric = metric_candidates[0] if metric_candidates else {}
        top_dim = dimension_candidates[0] if dimension_candidates else {}
        return {
            "intent": intent,
            "local_parser_enabled": CandidateExtractor._use_local_intent_parser(),
            "local_llm_used": bool(metric_local or dim_local),
            "metric_candidates_total": len(metric_candidates),
            "dimension_candidates_total": len(dimension_candidates),
            "metric_local_llm_count": len(metric_local),
            "dimension_local_llm_count": len(dim_local),
            "top_metric": {
                "name": top_metric.get("name"),
                "matched_by": top_metric.get("matched_by"),
                "score": top_metric.get("score"),
            } if top_metric else {},
            "top_dimension": {
                "name": top_dim.get("name"),
                "matched_by": top_dim.get("matched_by"),
                "score": top_dim.get("score"),
            } if top_dim else {},
        }
    
    def extract(
        self,
        question: str,
        last_state: Optional[Dict] = None,
        date_context: Optional[Dict] = None,
        semantic=None
    ) -> Dict[str, Any]:
        """
        질문에서 모든 후보 추출
        
        Returns:
            {
              "intent": str,
              "metric_candidates": List[Dict],
              "dimension_candidates": List[Dict],
              "date_range": Dict,
              "modifiers": Dict
            }
        """
        logging.info(f"[CandidateExtractor] Extracting from: {question}")
        
        # 1. Intent
        intent = IntentClassifier.classify(question)
        
        # 2. Dates
        date_range = DateParser.parse(question, last_state, date_context)
        
        # 3. Metrics
        metric_candidates = MetricCandidateExtractor.extract(question, semantic)
        
        # 4. Dimensions
        dimension_candidates = DimensionCandidateExtractor.extract(question, semantic)
        
        # 5. Modifiers
        modifiers = ModifierExtractor.extract(question)

        # 확장 질의는 독립 intent가 아니라 직전 질의의 drill-down으로 처리
        if last_state and _is_drilldown_followup(question):
            modifiers["needs_breakdown"] = True
            modifiers["needs_total"] = False
            modifiers["inherit_last_filters"] = True
            modifiers["preserve_context"] = True
            drill_dim = _infer_drilldown_dimension(question, last_state)
            if drill_dim:
                modifiers["force_dimensions"] = [drill_dim]
                dim_meta = GA4_DIMENSIONS.get(drill_dim, {})
                dim_scope = dim_meta.get("scope") or "event"
                dimension_candidates = [{
                    "name": drill_dim,
                    "score": 0.97,
                    "matched_by": "drilldown_followup",
                    "scope": dim_scope,
                    "category": dim_meta.get("category"),
                    "priority": dim_meta.get("priority", 0),
                }]
                intent = "breakdown"

            # 원인 분석은 현재 metric/dimension을 유지하고 분해만 추가
            if any(k in question.lower() for k in ["원인", "이유", "왜", "해석"]):
                intent = "breakdown"
                modifiers["cause_analysis_mode"] = True
                if not dimension_candidates and isinstance(last_state.get("dimensions"), list):
                    for d in (last_state.get("dimensions") or []):
                        d_name = (d or {}).get("name")
                        if not d_name:
                            continue
                        dim_meta = GA4_DIMENSIONS.get(d_name, {})
                        dimension_candidates.append({
                            "name": d_name,
                            "score": 0.96,
                            "matched_by": "cause_analysis_context",
                            "scope": dim_meta.get("scope") or "event",
                            "category": dim_meta.get("category"),
                            "priority": dim_meta.get("priority", 0),
                        })
                # 이전 차원이 없으면 최소 breakdown 차원 추가
                if not dimension_candidates:
                    fallback_dim = "defaultChannelGroup" if "defaultChannelGroup" in GA4_DIMENSIONS else "sourceMedium"
                    dimension_candidates = [{
                        "name": fallback_dim,
                        "score": 0.84,
                        "matched_by": "cause_analysis_fallback",
                        "scope": "event",
                        "category": "traffic",
                        "priority": GA4_DIMENSIONS.get(fallback_dim, {}).get("priority", 0),
                    }]

            if not metric_candidates and isinstance(last_state.get("metrics"), list):
                for m in last_state.get("metrics", []):
                    m_name = (m or {}).get("name")
                    if not m_name:
                        continue
                    metric_candidates.append({
                        "name": m_name,
                        "score": 0.95,
                        "matched_by": "drilldown_followup",
                        "scope": GA4_METRICS.get(m_name, {}).get("scope") or "event",
                        "priority": GA4_METRICS.get(m_name, {}).get("priority", 0),
                    })

        # 5.1 Optional: local LLM intent/matching merge
        merged = self._merge_local_intent(
            question=question,
            intent=intent,
            metric_candidates=metric_candidates,
            dimension_candidates=dimension_candidates,
            modifiers=modifiers
        )
        intent = merged["intent"]
        metric_candidates = sorted(merged["metric_candidates"], key=lambda x: x.get("score", 0), reverse=True)
        dimension_candidates = sorted(merged["dimension_candidates"], key=lambda x: x.get("score", 0), reverse=True)
        modifiers = merged["modifiers"]
        
        result = {
            "intent": intent,
            "metric_candidates": metric_candidates,
            "dimension_candidates": dimension_candidates,
            "date_range": date_range,
            "modifiers": modifiers,
            "matching_debug": self._build_matching_debug(
                intent=intent,
                metric_candidates=metric_candidates,
                dimension_candidates=dimension_candidates
            )
        }
        
        logging.info(f"[CandidateExtractor] Intent: {intent}")
        logging.info(f"[CandidateExtractor] Metrics: {len(metric_candidates)} candidates")
        logging.info(f"[CandidateExtractor] Dimensions: {len(dimension_candidates)} candidates")
        logging.info(f"[CandidateExtractor] Modifiers: {modifiers}")
        
        return result
        # 이벤트 종류/이벤트 목록 질의는 eventName 우선
        if any(k in q for k in ["이벤트 종류", "이벤트 목록", "무슨 이벤트", "어떤 이벤트"]):
            if not any(c.get("name") == "eventName" for c in candidates):
                candidates.append({
                    "name": "eventName",
                    "score": 0.99,
                    "matched_by": "event_category_list_rule",
                    "scope": "event",
                    "category": "event",
                    "priority": GA4_DIMENSIONS.get("eventName", {}).get("priority", 0)
                })
