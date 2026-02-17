# pipeline.py
# GA4 Complete Pipeline Orchestrator
"""
전체 파이프라인을 통합하는 메인 오케스트레이터.
Extract -> Plan -> Execute -> Aggregate 흐름을 관리한다.
"""

import logging
from typing import Dict, Any, Optional

from candidate_extractor import CandidateExtractor
from planner import GA4Planner
from plan_executor import PlanExecutor
from db_manager import DBManager


class GA4Pipeline:
    """
    GA4 분석 파이프라인 통합 오케스트레이터
    
    사용법:
    pipeline = GA4Pipeline()
    result = pipeline.run(
        question="상품별 매출 TOP 10",
        property_id="123456",
        conversation_id="conv_001",
        semantic=semantic_matcher
    )
    """
    
    def __init__(self):
        self.extractor = CandidateExtractor()
        self.planner = GA4Planner()
        self.executor = PlanExecutor()
    
    def run(
        self,
        question: str,
        property_id: str,
        conversation_id: Optional[str] = None,
        semantic=None
    ) -> Dict[str, Any]:
        """
        전체 파이프라인 실행
        
        Args:
            question: 사용자 질문
            property_id: GA4 속성 ID
            conversation_id: 대화 ID (선택)
            semantic: SemanticMatcher (선택)
        
        Returns:
            {
              "status": "ok",
              "message": "...",
              "account": property_id,
              "period": "...",
              "blocks": [...],
              "plot_data": []
            }
        """
        logging.info("=" * 70)
        logging.info("🚀 [GA4 PIPELINE] Start")
        logging.info(f"Question: {question}")
        logging.info(f"Property ID: {property_id}")
        logging.info("=" * 70)
        
        try:
            # ================================================================
            # STEP 1: Load Context
            # ================================================================
            last_state = None
            if conversation_id:
                last_state = DBManager.load_last_state(conversation_id, source="ga4")
                logging.info(f"[Pipeline] Loaded last state: {last_state is not None}")
            
            # ================================================================
            # STEP 2: Extract Candidates
            # ================================================================
            logging.info("[Pipeline] STEP 2: Extracting candidates...")
            
            extraction_result = self.extractor.extract(
                question=question,
                last_state=last_state,
                date_context=None,
                semantic=semantic
            )
            
            intent = extraction_result["intent"]
            metric_candidates = extraction_result["metric_candidates"]
            dimension_candidates = extraction_result["dimension_candidates"]
            date_range = extraction_result["date_range"]
            modifiers = extraction_result["modifiers"]
            matching_debug = extraction_result.get("matching_debug", {})
            
            logging.info(f"[Pipeline] Intent: {intent}")
            logging.info(f"[Pipeline] Metric candidates: {len(metric_candidates)}")
            logging.info(f"[Pipeline] Dimension candidates: {len(dimension_candidates)}")
            logging.info(f"[Pipeline] Modifiers: {modifiers}")

            q = (question or "").lower()
            is_period_inquiry = any(k in q for k in [
                "언제부터", "언제까지", "기간", "몇일부터", "몇일", "from", "to"
            ])
            # 지표 없이 날짜 확인만 묻는 질문은 직전/파싱 기간을 바로 응답
            if is_period_inquiry and not metric_candidates:
                s = date_range.get("start_date") if isinstance(date_range, dict) else None
                e = date_range.get("end_date") if isinstance(date_range, dict) else None
                if not s or not e:
                    if last_state:
                        s = last_state.get("start_date")
                        e = last_state.get("end_date")
                if s and e:
                    return {
                        "status": "ok",
                        "message": f"현재 분석 기준 기간은 **{s} ~ {e}** 입니다.",
                        "account": property_id,
                        "period": f"{s} ~ {e}",
                        "blocks": [],
                        "plot_data": [],
                        "matching_debug": matching_debug
                    }

            # 매칭 실패 시 무리한 기본 지표 추론 대신 명시적으로 질의 보강 요청
            top_score = metric_candidates[0].get("score", 0) if metric_candidates else 0
            has_prev_metrics = bool(last_state and last_state.get("metrics"))
            short_dimension_followup = (
                len(q) <= 20 and
                any(k in q for k in [
                    "채널별", "디바이스별", "기기별", "랜딩페이지", "소스별", "매체별", "분해",
                    "해외", "국내", "국가", "유형", "카테고리", "프로그램",
                    "메뉴명", "후원명", "묶어서", "전체", "스크롤", "페이지별", "click", "클릭"
                ])
            )
            if (not metric_candidates and not short_dimension_followup) or (not has_prev_metrics and top_score < 0.55):
                return {
                    "status": "clarify",
                    "message": "질문에서 매칭 가능한 지표를 찾지 못했습니다. 사용 가능한 지표명(예: 활성 사용자, 세션, 구매 수익, 상품 수익)으로 다시 질문해 주세요.",
                    "blocks": [],
                    "plot_data": [],
                    "matching_debug": matching_debug
                }
            
            # ================================================================
            # STEP 3: Build Execution Plan
            # ================================================================
            logging.info("[Pipeline] STEP 3: Building execution plan...")
            
            execution_plan = self.planner.build_plan(
                property_id=property_id,
                question=question,
                intent=intent,
                metric_candidates=metric_candidates,
                dimension_candidates=dimension_candidates,
                date_range=date_range,
                modifiers=modifiers,
                last_state=last_state
            )
            
            logging.info(f"[Pipeline] Created {len(execution_plan.blocks)} blocks")
            for block in execution_plan.blocks:
                logging.info(f"  - {block.block_id} ({block.scope}, {block.block_type})")
                logging.info(f"    Metrics: {[m['name'] for m in block.metrics]}")
                logging.info(f"    Dimensions: {[d['name'] for d in block.dimensions]}")
            
            # ================================================================
            # STEP 4: Execute Plan
            # ================================================================
            logging.info("[Pipeline] STEP 4: Executing plan...")
            
            result = self.executor.execute(
                execution_plan=execution_plan,
                property_id=property_id
            )
            if isinstance(result, dict):
                result["matching_debug"] = matching_debug
            
            # ================================================================
            # STEP 5: Save State
            # ================================================================
            if conversation_id and execution_plan.blocks:
                anchor_block = None
                for b in execution_plan.blocks:
                    if b.block_type in ["breakdown", "breakdown_topn", "trend"] and b.dimensions:
                        anchor_block = b
                        break
                if not anchor_block:
                    anchor_block = execution_plan.blocks[0]
                final_state = {
                    "metrics": anchor_block.metrics,
                    "dimensions": anchor_block.dimensions,
                    "start_date": execution_plan.start_date,
                    "end_date": execution_plan.end_date,
                    "intent": intent
                }
                
                DBManager.save_success_state(conversation_id, "ga4", final_state)
                logging.info("[Pipeline] Saved final state")
            
            logging.info("=" * 70)
            logging.info("✅ [GA4 PIPELINE] Complete")
            logging.info("=" * 70)
            
            return result
        
        except Exception as e:
            logging.error(f"[GA4 Pipeline Error] {e}", exc_info=True)
            return {
                "status": "error",
                "message": f"분석 중 오류 발생: {str(e)}",
                "blocks": [],
                "plot_data": [],
                "matching_debug": {}
            }
