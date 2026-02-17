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
        modifiers = dict(modifiers or {})

        # "전체 항목" 후속질문은 직전 breakdown 맥락을 강제 재사용
        if modifiers.get("all_items") and last_state and last_state.get("dimensions"):
            intent = "breakdown"
            modifiers["needs_breakdown"] = True
            modifiers["needs_total"] = False
            modifiers.pop("limit", None)
            metric_candidates = []
            for m in last_state.get("metrics", []) or []:
                m_name = m.get("name")
                if not m_name:
                    continue
                metric_candidates.append({
                    "name": m_name,
                    "score": 0.99,
                    "matched_by": "last_state_all_items",
                    "scope": self._metric_scope(m_name),
                    "priority": GA4_METRICS.get(m_name, {}).get("priority", 0),
                })
            dimension_candidates = []
            for d in last_state.get("dimensions", []) or []:
                d_name = d.get("name")
                if not d_name:
                    continue
                dim_meta = GA4_DIMENSIONS.get(d_name, {})
                dim_scope = dim_meta.get("scope") or self.CATEGORY_TO_SCOPE.get(dim_meta.get("category"), "event")
                dimension_candidates.append({
                    "name": d_name,
                    "score": 0.99,
                    "matched_by": "last_state_all_items",
                    "scope": dim_scope,
                    "category": dim_meta.get("category"),
                    "priority": dim_meta.get("priority", 0),
                })
        
        # 1. Date 확정
        start_date, end_date = self._resolve_dates(date_range, last_state)

        # 1.5 Context 보강:
        # TopN/Breakdown follow-up에서 후보가 비어 있으면 이전 state를 재사용한다.
        metric_candidates = metric_candidates or []
        dimension_candidates = dimension_candidates or []
        if intent in ["topn", "breakdown"] and last_state:
            # dimension이 명시된 새 질문(예: 채널별/디바이스별)이면
            # 이전 metric을 무조건 재사용하지 않는다.
            if not metric_candidates and not dimension_candidates and last_state.get("metrics"):
                metric_candidates = []
                for m in last_state.get("metrics", []):
                    m_name = m.get("name")
                    if not m_name:
                        continue
                    metric_candidates.append({
                        "name": m_name,
                        "score": 0.80,
                        "matched_by": "last_state",
                        "scope": self._metric_scope(m_name),
                        "priority": GA4_METRICS.get(m_name, {}).get("priority", 0),
                    })
            if not dimension_candidates and last_state.get("dimensions"):
                dimension_candidates = []
                for d in last_state.get("dimensions", []):
                    d_name = d.get("name")
                    if not d_name:
                        continue
                    dim_meta = GA4_DIMENSIONS.get(d_name, {})
                    dim_scope = dim_meta.get("scope") or self.CATEGORY_TO_SCOPE.get(dim_meta.get("category"), "event")
                    dimension_candidates.append({
                        "name": d_name,
                        "score": 0.80,
                        "matched_by": "last_state",
                        "scope": dim_scope,
                        "category": dim_meta.get("category"),
                        "priority": dim_meta.get("priority", 0),
                    })
            if "scope_hint" not in modifiers and dimension_candidates:
                inferred_scope = dimension_candidates[0].get("scope")
                if inferred_scope:
                    modifiers["scope_hint"] = [inferred_scope]
        # metric_single라도 breakdown 힌트가 있으면 이전 metric을 재사용
        if intent == "metric_single" and modifiers.get("needs_breakdown") and last_state and not metric_candidates:
            if last_state.get("metrics"):
                metric_candidates = []
                for m in last_state.get("metrics", []):
                    m_name = m.get("name")
                    if not m_name:
                        continue
                    metric_candidates.append({
                        "name": m_name,
                        "score": 0.80,
                        "matched_by": "last_state",
                        "scope": self._metric_scope(m_name),
                        "priority": GA4_METRICS.get(m_name, {}).get("priority", 0),
                    })
        # breakdown/topn에서도 metric 후보가 비어 있으면 이전 metric 재사용
        if intent in ["breakdown", "topn"] and modifiers.get("needs_breakdown") and last_state and not metric_candidates:
            if last_state.get("metrics"):
                metric_candidates = []
                for m in last_state.get("metrics", []):
                    m_name = m.get("name")
                    if not m_name:
                        continue
                    metric_candidates.append({
                        "name": m_name,
                        "score": 0.86,
                        "matched_by": "last_state_breakdown_metric",
                        "scope": self._metric_scope(m_name),
                        "priority": GA4_METRICS.get(m_name, {}).get("priority", 0),
                    })
        
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
        if not modifiers:
            modifiers = {}
        
        # score 기준 정렬
        sorted_candidates = sorted(candidates, key=lambda x: x.get("score", 0), reverse=True)
        scope_hints = modifiers.get("scope_hint", []) if modifiers else []

        # scope 힌트가 있으면 우선 적용 (총합+상세 복합이 아닌 일반 breakdown/topn)
        if scope_hints and not modifiers.get("needs_total"):
            preferred_scope = scope_hints[0]
            scoped = [c for c in sorted_candidates if self._metric_scope(c["name"]) == preferred_scope]
            if scoped:
                sorted_candidates = scoped + [c for c in sorted_candidates if c not in scoped]

        # breakdown 질문은 단일 total보다 breakdown 적합 metric(item/event)을 우선
        if intent in ["breakdown", "topn"] and len(sorted_candidates) > 1:
            preferred = []
            others = []
            for c in sorted_candidates:
                n = c.get("name", "")
                if n in ["itemRevenue", "purchaseRevenue", "purchaserRate", "purchaseToViewRate", "eventCount", "keyEvents", "transactions"]:
                    preferred.append(c)
                else:
                    others.append(c)
            if preferred:
                sorted_candidates = preferred + others

        # 복합 질문 처리:
        # 예) "총 매출 + 상품별 매출 TOP N"
        # - breakdown scope(예: item) 1개
        # - total용 scope(예: event) 1개를 함께 선택
        if modifiers.get("needs_total") and (modifiers.get("needs_breakdown") or intent in ["breakdown", "topn"]):
            scope_hints = modifiers.get("scope_hint", [])
            breakdown_scope = scope_hints[0] if scope_hints else None
            if not breakdown_scope:
                breakdown_scope = self._metric_scope(sorted_candidates[0]["name"])

            breakdown_metric = None
            for c in sorted_candidates:
                if self._metric_scope(c["name"]) == breakdown_scope:
                    breakdown_metric = c["name"]
                    break
            if not breakdown_metric:
                breakdown_metric = sorted_candidates[0]["name"]

            selected = [breakdown_metric]

            # total metric은 다른 scope에서, 가능한 경우 같은 concept로 맞춤
            breakdown_concept = self._metric_concept(breakdown_metric)
            total_metric = None
            for c in sorted_candidates:
                c_name = c["name"]
                if c_name in selected:
                    continue
                if self._metric_scope(c_name) == breakdown_scope:
                    continue
                if breakdown_concept and self._metric_concept(c_name) == breakdown_concept:
                    total_metric = c_name
                    break

            if not total_metric:
                for c in sorted_candidates:
                    c_name = c["name"]
                    if c_name in selected:
                        continue
                    if self._metric_scope(c_name) != breakdown_scope:
                        total_metric = c_name
                        break

            if total_metric:
                selected = [total_metric, breakdown_metric]

            return [{"name": m} for m in selected]
        
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

        # 강제 차원 지정 (프로파일 조회 등)
        forced = modifiers.get("force_dimensions") or []
        if forced:
            valid = [{"name": d} for d in forced if d in GA4_DIMENSIONS]
            if valid:
                return valid
        
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
        needs_total = modifiers.get("needs_total", False)
        needs_breakdown = modifiers.get("needs_breakdown", False) or intent in ["breakdown", "topn"]

        # 복합 scope 최적화:
        # total + breakdown이면서 scope가 2개 이상이면
        # - total은 event 우선 1개 scope
        # - breakdown은 scope_hint(없으면 item 우선) 1개 scope
        if needs_total and needs_breakdown and len(scope_groups) > 1:
            scope_hints = modifiers.get("scope_hint", [])
            breakdown_scope = scope_hints[0] if scope_hints and scope_hints[0] in scope_groups else None
            if not breakdown_scope:
                breakdown_scope = "item" if "item" in scope_groups else next(iter(scope_groups.keys()))

            non_breakdown = [s for s in scope_groups.keys() if s != breakdown_scope]
            if "event" in non_breakdown:
                total_scope = "event"
            elif non_breakdown:
                total_scope = non_breakdown[0]
            else:
                total_scope = breakdown_scope

            # total block
            total_metrics = scope_groups.get(total_scope, [])
            if total_metrics:
                blocks.append(PlanBlock(
                    block_id=f"total_{total_scope}",
                    scope=total_scope,
                    block_type="total",
                    metrics=total_metrics,
                    dimensions=[],
                    title=f"{total_scope.upper()} 전체 요약"
                ))

            # breakdown block
            breakdown_metrics = scope_groups.get(breakdown_scope, [])
            if breakdown_metrics:
                scoped_dims = self._filter_dimensions_by_scope(dimensions, breakdown_scope)
                breakdown_dims = scoped_dims or self._get_default_dimension_for_scope(breakdown_scope)
                blocks.append(self._create_breakdown_block(
                    breakdown_scope, breakdown_metrics, breakdown_dims, modifiers
                ))

            return blocks

        # Trend는 합계(total)가 아니라 시계열(trend) 블록으로 고정
        if intent == "trend" or modifiers.get("needs_trend"):
            for scope, scope_metrics in scope_groups.items():
                scoped_dims = self._filter_dimensions_by_scope(dimensions, scope)
                trend_dims = scoped_dims or ([{"name": DEFAULT_TIME_DIMENSION}] if DEFAULT_TIME_DIMENSION else [])
                trend_filters = self._build_entity_filters(scope, modifiers, trend_dims)
                blocks.append(PlanBlock(
                    block_id=f"trend_{scope}",
                    scope=scope,
                    block_type="trend",
                    metrics=scope_metrics,
                    dimensions=trend_dims,
                    filters=trend_filters,
                    title=f"{scope.upper()} 추이"
                ))
            return blocks
        
        for scope, scope_metrics in scope_groups.items():
            # Dimension 필터링 (scope 일치)
            scoped_dims = self._filter_dimensions_by_scope(dimensions, scope)
            
            # "총합 + breakdown" 패턴 감지
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

    def _metric_scope(self, metric_name: str) -> str:
        meta = GA4_METRICS.get(metric_name, {})
        return meta.get("scope") or self.CATEGORY_TO_SCOPE.get(meta.get("category"), "event")

    def _metric_concept(self, metric_name: str) -> str:
        return GA4_METRICS.get(metric_name, {}).get("concept")
    
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

        filters = self._build_entity_filters(scope, modifiers, dimensions)
        entity_terms = modifiers.get("entity_contains") or modifiers.get("item_name_contains") or []
        if entity_terms:
            title += f" ({', '.join(entity_terms)})"
        
        return PlanBlock(
            block_id=f"breakdown_{scope}",
            scope=scope,
            block_type=block_type,
            metrics=metrics,
            dimensions=dimensions,
            filters=filters,
            order_bys=order_bys,
            limit=limit,
            title=title
        )

    def _build_entity_filters(self, scope: str, modifiers: Dict, dimensions: List[Dict]) -> Dict[str, Any]:
        filters = {}
        if modifiers.get("event_filter"):
            filters["event_filter"] = modifiers.get("event_filter")
        entity_terms = modifiers.get("entity_contains") or modifiers.get("item_name_contains") or []
        if not entity_terms:
            return filters

        hint_field = modifiers.get("entity_field_hint")
        field = hint_field
        available_dims = {d.get("name") for d in dimensions if isinstance(d, dict)}
        # 힌트 필드는 dimensions에 없더라도 GA4 메타에 존재하면 필터에 사용
        if field and field not in GA4_DIMENSIONS and field not in available_dims:
            field = None
        if not field:
            if scope == "item":
                field = "itemName"
            else:
                for cand in ["customEvent:donation_name", "eventName", "linkText", "defaultChannelGroup", "sourceMedium", "landingPage"]:
                    if cand in available_dims:
                        field = cand
                        break
                if not field:
                    for cand in ["customEvent:donation_name", "eventName", "linkText", "sourceMedium"]:
                        if cand in GA4_DIMENSIONS:
                            field = cand
                            break
        if not field:
            field = "itemName" if scope == "item" else (next(iter(available_dims)) if available_dims else None)
        if not field:
            return filters

        filters["dimension_filters"] = [
            {"field": field, "match_type": "contains", "value": t}
            for t in entity_terms
        ]
        filters["dimension_filters_operator"] = "or" if len(entity_terms) > 1 else "and"
        return filters
    
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
