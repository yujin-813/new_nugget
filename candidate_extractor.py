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
import logging
from datetime import date, datetime, timedelta
from typing import List, Dict, Optional, Any

from ga4_metadata import GA4_METRICS, GA4_DIMENSIONS
from ml_module import parse_dates


# =============================================================================
# Date Parser (기존 유지)
# =============================================================================

class DateParser:
    """날짜 추출 (변경 없음)"""
    
    @staticmethod
    def parse(question, last_state=None, date_context=None):
        delta_dates = {"start_date": None, "end_date": None, "is_relative_shift": False}
        q = question.lower()
        
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
        
        # 1. Category List (최우선)
        if "종류" in q or "무슨 이벤트" in q or "어떤 이벤트" in q:
            return "category_list"
        
        # 2. TopN (명시적 숫자)
        if re.search(r'(top\s*\d+|상위\s*\d+|\d+위|1-\d+|\d+개)', q):
            return "topn"
        
        # 3. Trend
        if any(k in q for k in ["추이", "흐름", "일별", "변화", "trend", "daily"]):
            return "trend"
        
        # 4. Comparison
        if any(k in q for k in ["전주 대비", "비교", "차이", "증감", "compare", "vs"]):
            return "comparison"
        
        # 5. Breakdown
        if any(k in q for k in ["별", "기준", "따라", "by "]):
            return "breakdown"
        
        # 6. Multi-metric (여러 지표 언급)
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
        item_keywords = ["상품", "아이템", "제품", "상품별", "아이템별", "제품별"]
        if any(kw in question for kw in item_keywords):
            for candidate in candidates:
                if candidate.get("scope") == "item":
                    candidate["score"] = min(candidate["score"] + 0.15, 1.0)
                    logging.info(f"[MetricExtractor] Boosted item-scoped metric: {candidate['name']} -> {candidate['score']:.2f}")
        
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
        
        # API key 매칭
        if metric_name.lower() in q:
            score = max(score, 0.85)
        
        # UI name 매칭
        ui_name = meta.get("ui_name", "").lower()
        if ui_name and ui_name in q:
            score = max(score, 0.95)
        
        # Alias 매칭
        for alias in meta.get("aliases", []):
            if alias.lower() in q:
                score = max(score, 0.90)
        
        # kr_semantics 매칭 (약한 매칭)
        for sem in meta.get("kr_semantics", []):
            if sem.lower() in q:
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
        
        candidates.sort(key=lambda x: x["score"], reverse=True)
        
        logging.info(f"[DimensionExtractor] Found {len(candidates)} candidates")
        for c in candidates[:3]:
            logging.info(f"  - {c['name']}: {c['score']:.2f} ({c['matched_by']})")
        
        return candidates
    
    @staticmethod
    def _calculate_explicit_score(q: str, dim_name: str, meta: Dict) -> float:
        """명시적 매칭 점수 계산"""
        score = 0.0
        
        if dim_name.lower() in q:
            score = max(score, 0.85)
        
        ui_name = meta.get("ui_name", "").lower()
        if ui_name and ui_name in q:
            score = max(score, 0.95)
        
        for alias in meta.get("aliases", []):
            if alias.lower() in q:
                score = max(score, 0.90)
        
        for sem in meta.get("kr_semantics", []):
            if sem.lower() in q:
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
        
        # 1. TopN limit
        limit_match = re.search(r'(top\s*(\d+)|상위\s*(\d+)|(\d+)위|1-(\d+)|(\d+)개)', q)
        if limit_match:
            # 매칭된 그룹에서 숫자 추출
            nums = [g for g in limit_match.groups() if g and g.isdigit()]
            if nums:
                modifiers["limit"] = int(nums[0])
        
        # 2. "총" / "전체" 키워드
        if any(k in q for k in ["총", "전체", "합계", "total"]):
            modifiers["needs_total"] = True
        
        # 3. "~별" / "기준" 키워드
        if any(k in q for k in ["별", "기준", "따라", "by "]):
            modifiers["needs_breakdown"] = True
        
        # 4. Scope hint
        scope_hints = []
        if any(k in q for k in ["상품", "아이템", "제품", "item"]):
            scope_hints.append("item")
        if any(k in q for k in ["사용자", "유저", "user"]):
            scope_hints.append("user")
        if scope_hints:
            modifiers["scope_hint"] = scope_hints
        
        # 5. Order hint
        if any(k in q for k in ["높은", "많은", "큰", "상위", "top"]):
            modifiers["order_hint"] = "desc"
        elif any(k in q for k in ["낮은", "적은", "작은", "하위", "bottom"]):
            modifiers["order_hint"] = "asc"
        
        logging.info(f"[ModifierExtractor] Extracted: {modifiers}")
        return modifiers


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
        
        result = {
            "intent": intent,
            "metric_candidates": metric_candidates,
            "dimension_candidates": dimension_candidates,
            "date_range": date_range,
            "modifiers": modifiers
        }
        
        logging.info(f"[CandidateExtractor] Intent: {intent}")
        logging.info(f"[CandidateExtractor] Metrics: {len(metric_candidates)} candidates")
        logging.info(f"[CandidateExtractor] Dimensions: {len(dimension_candidates)} candidates")
        logging.info(f"[CandidateExtractor] Modifiers: {modifiers}")
        
        return result
