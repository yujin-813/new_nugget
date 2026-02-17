# planner.py
# GA4 Execution Plan Builder
"""
GA4 분석 파이프라인의 핵심 결정 레이어.
후보군(Candidates)을 입력받아 실행 가능한 ExecutionPlan을 생성한다.

핵심 원칙:
1. 엔진은 Plan대로만 실행 (추론 금지)
2. LLM은 후보 추출만, 결정은 Planner가 담당
3. Scope별로 Block을 분리하여 안정성 확보
4. Dimension 선택은 Priority + Data Quality 기반
"""

import logging
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta

from ga4_metadata import GA4_DIMENSIONS, GA4_METRICS, DEFAULT_METRIC, DEFAULT_TIME_DIMENSION


# =============================================================================
# Data Structures
# =============================================================================

@dataclass
class PlanBlock:
    """
    실행 가능한 단일 쿼리 블록.
    엔진은 이 블록을 받아 GA4 RunReportRequest로 변환한다.
    """
    block_id: str
    scope: str  # "event" | "item" | "user"
    block_type: str  # "total" | "breakdown" | "breakdown_topn" | "trend" | "compare"
    
    metrics: List[Dict[str, str]]  # [{"name": "activeUsers"}, ...]
    dimensions: List[Dict[str, str]]  # [{"name": "date"}, ...]
    
    filters: Dict[str, Any] = field(default_factory=dict)
    order_bys: List[Dict[str, Any]] = field(default_factory=list)
    limit: Optional[int] = None
    
    # 내부 메타
    title: str = ""
    description: str = ""


@dataclass
class ExecutionPlan:
    """
    전체 실행 계획. 여러 개의 PlanBlock으로 구성.
    """
    property_id: str
    start_date: str
    end_date: str
    
    blocks: List[PlanBlock] = field(default_factory=list)
    
    # 메타
    intent: str = "metric_single"
    question: str = ""


# =============================================================================
# GA4Planner
# =============================================================================

class GA4Planner:
    """
    후보군 + 의도 -> ExecutionPlan 생성
    """
    
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
    
    def __init__(self):
        pass
    
    def build_plan(
        self,
        property_id: str,
        question: str,
        intent: str,
        metric_candidates: List[Dict],
        dimension_candidates: List[Dict],
        date_range: Optional[Dict] = None,
        modifiers: Optional[Dict] = None,
        last_state: Optional[Dict] = None
    ) -> ExecutionPlan:
        """
        메인 진입점: 후보군 -> ExecutionPlan
        
        Args:
            property_id: GA4 속성 ID
            question: 원본 질문
            intent: 의도 (metric_single, breakdown, trend, comparison, topn 등)
            metric_candidates: [{"name": "...", "score": ..., "scope": ...}, ...]
            dimension_candidates: [{"name": "...", "score": ..., "scope": ...}, ...]
            date_range: {"start_date": "...", "end_date": "..."}
            modifiers: {"limit": 10, "needs_total": True, "needs_breakdown": True, ...}
            last_state: 이전 상태 (선택)
        
        Returns:
            ExecutionPlan
        """
        logging.info(f"[Planner] Building plan for intent={intent}")
        
        # 1. Date 확정
        start_date, end_date = self._resolve_dates(date_range, last_state)
        
        # 2. Metrics 확정
        final_metrics = self._select_metrics(metric_candidates, intent, modifiers)
        
        # 3. Dimensions 확정
        final_dimensions = self._select_dimensions(
            dimension_candidates, final_metrics, intent, modifiers
        )
        
        # 4. Scope 분리 및 Block 생성
        blocks = self._create_blocks(
            final_metrics, final_dimensions, intent, modifiers
        )
        
        plan = ExecutionPlan(
            property_id=property_id,
            start_date=start_date,
            end_date=end_date,
            blocks=blocks,
            intent=intent,
            question=question
        )
        
        logging.info(f"[Planner] Created {len(blocks)} blocks")
        for b in blocks:
            logging.info(f"  - {b.block_id} ({b.scope}): {len(b.metrics)} metrics, {len(b.dimensions)} dims")
        
        return plan
    
    # -------------------------------------------------------------------------
    # Date Resolution
    # -------------------------------------------------------------------------
    
    def _resolve_dates(self, date_range: Optional[Dict], last_state: Optional[Dict]) -> tuple:
        """날짜 확정 (후보 -> last_state -> default)"""
        if date_range and date_range.get("start_date") and date_range.get("end_date"):
            return date_range["start_date"], date_range["end_date"]
        
        if last_state:
            s = last_state.get("start_date")
            e = last_state.get("end_date")
            if s and e:
                return s, e
        
        # Default: last 7 days
        today = datetime.today().date()
        start = (today - timedelta(days=7)).strftime("%Y-%m-%d")
        end = today.strftime("%Y-%m-%d")
        return start, end
    
    # -------------------------------------------------------------------------
    # Metric Selection
    # -------------------------------------------------------------------------
    
    def _select_metrics(
        self,
        candidates: List[Dict],
        intent: str,
        modifiers: Optional[Dict]
    ) -> List[Dict[str, str]]:
        """
        Metric 후보 -> 최종 선택
        
        정책:
        - 후보가 없으면 DEFAULT_METRIC
        - 후보가 여러 개면 score 높은 순으로 정렬 후 상위 선택
        - intent가 multi면 여러 개 허용
        """
        if not candidates:
            return [{"name": DEFAULT_METRIC}]
        
        # score 기준 정렬
        sorted_candidates = sorted(candidates, key=lambda x: x.get("score", 0), reverse=True)
        
        if intent in ["metric_multi", "comparison"]:
            # 여러 개 허용 (최대 5개)
            return [{"name": c["name"]} for c in sorted_candidates[:5]]
        else:
            # 단일
            return [{"name": sorted_candidates[0]["name"]}]
    
    # -------------------------------------------------------------------------
    # Dimension Selection
    # -------------------------------------------------------------------------
    
    def _select_dimensions(
        self,
        candidates: List[Dict],
        final_metrics: List[Dict],
        intent: str,
        modifiers: Optional[Dict]
    ) -> List[Dict[str, str]]:
        """
        Dimension 후보 -> 최종 선택
        
        정책:
        - breakdown/topn이면 dimension 필수
        - trend면 time dimension 우선
        - 후보가 없으면 scope별 기본 dimension
        """
        if not modifiers:
            modifiers = {}
        
        # Trend: time dimension 우선
        if intent == "trend" or modifiers.get("needs_trend"):
            time_dims = [c for c in candidates if self._is_time_dimension(c["name"])]
            if time_dims:
                return [{"name": time_dims[0]["name"]}]
            return [{"name": DEFAULT_TIME_DIMENSION}] if DEFAULT_TIME_DIMENSION else []
        
        # Breakdown/TopN: non-time dimension 필수
        if intent in ["breakdown", "topn"] or modifiers.get("needs_breakdown"):
            non_time = [c for c in candidates if not self._is_time_dimension(c["name"])]
            if non_time:
                # score 높은 것 선택
                best = max(non_time, key=lambda x: x.get("score", 0))
                return [{"name": best["name"]}]
            
            # 후보 없으면 scope별 기본
            metric_scopes = self._get_metric_scopes(final_metrics)
            if "item" in metric_scopes:
                return self._get_default_dimension_for_scope("item")
            return self._get_default_dimension_for_scope("event")
        
        # 기타: 후보가 있으면 사용, 없으면 빈 리스트 (total)
        if candidates:
            best = max(candidates, key=lambda x: x.get("score", 0))
            return [{"name": best["name"]}]
        
        return []
    
    def _is_time_dimension(self, dim_name: str) -> bool:
        """시간 차원 여부"""
        meta = GA4_DIMENSIONS.get(dim_name, {})
        return meta.get("category") == "time"
    
    def _get_metric_scopes(self, metrics: List[Dict]) -> set:
        """Metric들의 scope 집합"""
        scopes = set()
        for m in metrics:
            meta = GA4_METRICS.get(m["name"], {})
            scope = meta.get("scope") or self.CATEGORY_TO_SCOPE.get(meta.get("category"), "event")
            scopes.add(scope)
        return scopes
    
    def _get_default_dimension_for_scope(self, scope: str) -> List[Dict[str, str]]:
        """
        Scope별 기본 dimension 선택
        
        정책:
        - is_label=True인 것 우선
        - priority 높은 순
        - time dimension 제외
        """
        candidates = []
        
        for dim_name, meta in GA4_DIMENSIONS.items():
            cat = meta.get("category")
            dim_scope = meta.get("scope") or self.CATEGORY_TO_SCOPE.get(cat, "event")
            
            if dim_scope == scope and cat != "time":
                is_label = meta.get("is_label", False)
                priority = meta.get("priority", 0)
                candidates.append((dim_name, is_label, priority))
        
        if not candidates:
            return []
        
        # is_label=True 우선, 그 다음 priority
        candidates.sort(key=lambda x: (not x[1], -x[2]))
        return [{"name": candidates[0][0]}]
    
    # -------------------------------------------------------------------------
    # Block Creation
    # -------------------------------------------------------------------------
    
    def _create_blocks(
        self,
        metrics: List[Dict],
        dimensions: List[Dict],
        intent: str,
        modifiers: Optional[Dict]
    ) -> List[PlanBlock]:
        """
        Metrics + Dimensions -> PlanBlock 리스트
        
        핵심 로직:
        1. Scope별로 metrics 분리
        2. "총합 + breakdown" 패턴이면 block 2개 생성
        3. TopN이면 order_by 자동 설정
        """
        if not modifiers:
            modifiers = {}
        
        # Scope별 metrics 분리
        scope_groups = self._split_metrics_by_scope(metrics)
        
        blocks = []
        
        for scope, scope_metrics in scope_groups.items():
            # Dimension 필터링 (scope 일치)
            scoped_dims = self._filter_dimensions_by_scope(dimensions, scope)
            
            # "총합 + breakdown" 패턴 감지
            needs_total = modifiers.get("needs_total", False)
            needs_breakdown = modifiers.get("needs_breakdown", False) or intent in ["breakdown", "topn"]
            
            if needs_total and needs_breakdown:
                # Block 1: Total (dimension 없음)
                total_block = PlanBlock(
                    block_id=f"total_{scope}",
                    scope=scope,
                    block_type="total",
                    metrics=scope_metrics,
                    dimensions=[],
                    title=f"{scope.upper()} 전체 요약"
                )
                blocks.append(total_block)
                
                # Block 2: Breakdown
                breakdown_dims = scoped_dims or self._get_default_dimension_for_scope(scope)
                breakdown_block = self._create_breakdown_block(
                    scope, scope_metrics, breakdown_dims, modifiers
                )
                blocks.append(breakdown_block)
            
            elif needs_breakdown:
                # Breakdown만
                breakdown_dims = scoped_dims or self._get_default_dimension_for_scope(scope)
                breakdown_block = self._create_breakdown_block(
                    scope, scope_metrics, breakdown_dims, modifiers
                )
                blocks.append(breakdown_block)
            
            else:
                # Total만
                total_block = PlanBlock(
                    block_id=f"total_{scope}",
                    scope=scope,
                    block_type="total",
                    metrics=scope_metrics,
                    dimensions=scoped_dims,
                    title=f"{scope.upper()} 지표"
                )
                blocks.append(total_block)
        
        return blocks
    
    def _create_breakdown_block(
        self,
        scope: str,
        metrics: List[Dict],
        dimensions: List[Dict],
        modifiers: Dict
    ) -> PlanBlock:
        """Breakdown 블록 생성 (TopN 포함)"""
        limit = modifiers.get("limit")
        
        block_type = "breakdown_topn" if limit else "breakdown"
        
        # Order by 설정
        order_bys = []
        if limit and metrics:
            # Primary metric으로 정렬
            primary_metric = self._select_primary_metric(metrics)
            order_bys = [{
                "metric": primary_metric,
                "desc": True
            }]
        
        title = f"{scope.upper()} 상세" if not limit else f"{scope.upper()} TOP {limit}"
        
        return PlanBlock(
            block_id=f"breakdown_{scope}",
            scope=scope,
            block_type=block_type,
            metrics=metrics,
            dimensions=dimensions,
            order_bys=order_bys,
            limit=limit,
            title=title
        )
    
    def _select_primary_metric(self, metrics: List[Dict]) -> str:
        """
        Primary metric 선택 (정렬 기준)
        
        정책:
        - priority 높은 것
        - 없으면 첫 번째
        """
        if not metrics:
            return DEFAULT_METRIC
        
        best = metrics[0]["name"]
        best_priority = GA4_METRICS.get(best, {}).get("priority", 0)
        
        for m in metrics[1:]:
            p = GA4_METRICS.get(m["name"], {}).get("priority", 0)
            if p > best_priority:
                best = m["name"]
                best_priority = p
        
        return best
    
    def _split_metrics_by_scope(self, metrics: List[Dict]) -> Dict[str, List[Dict]]:
        """Metrics를 scope별로 분리"""
        scope_groups = {}
        
        for m in metrics:
            meta = GA4_METRICS.get(m["name"], {})
            scope = meta.get("scope") or self.CATEGORY_TO_SCOPE.get(meta.get("category"), "event")
            scope_groups.setdefault(scope, []).append(m)
        
        return scope_groups
    
    def _filter_dimensions_by_scope(self, dimensions: List[Dict], target_scope: str) -> List[Dict]:
        """Dimension을 scope별로 필터링"""
        filtered = []
        
        for d in dimensions:
            meta = GA4_DIMENSIONS.get(d["name"], {})
            cat = meta.get("category")
            dim_scope = meta.get("scope") or self.CATEGORY_TO_SCOPE.get(cat, "event")
            
            if dim_scope == target_scope:
                filtered.append(d)
        
        return filtered
