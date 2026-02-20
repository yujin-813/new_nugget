# pipeline.py
# GA4 Complete Pipeline Orchestrator
"""
전체 파이프라인을 통합하는 메인 오케스트레이터.
Extract -> Plan -> Execute -> Aggregate 흐름을 관리한다.
"""

import logging
import os
from typing import Dict, Any, Optional

from candidate_extractor import CandidateExtractor
from planner import GA4Planner
from plan_executor import PlanExecutor
from db_manager import DBManager


def _looks_explanatory_question(question: str) -> bool:
    q = (question or "").lower()
    explain_tokens = ["뭐야", "무엇", "무슨 뜻", "뜻", "의미", "정의", "설명해", "뭔지", "알려줘"]
    return any(t in q for t in explain_tokens)


def _safe_general_answer(question: str) -> str:
    q = (question or "").strip()
    # LLM 응답 시도 (실패 시 결정론적 안전 폴백)
    if os.getenv("OPENAI_API_KEY"):
        try:
            import openai
            res = openai.ChatCompletion.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "너는 데이터 분석 도우미다. "
                            "사실 단정은 피하고, 모르면 모른다고 말한다. "
                            "한국어로 3문장 이내로 답한다."
                        )
                    },
                    {
                        "role": "user",
                        "content": (
                            f"질문: {q}\n"
                            "현재 데이터 조회로는 정의를 확정할 수 없는 상황이다. "
                            "일반 설명 + 확인 방법(메타데이터/정의 문서 확인)을 함께 답해라."
                        )
                    }
                ],
                temperature=0.2
            )
            content = str(res["choices"][0]["message"]["content"]).strip()
            if content:
                return content
        except Exception:
            pass

    return (
        "질문하신 항목은 현재 연결된 데이터만으로 업무 정의를 확정할 수 없습니다. "
        "일반적으로는 분석용 라벨(예: 상품/후원/이벤트 분류값)로 사용됩니다. "
        "정확한 정의는 측정기준(메타데이터) 문서에서 확인해 주세요."
    )


def _has_data_signal(question: str) -> bool:
    q = (question or "").lower()
    tokens = [
        "매출", "수익", "사용자", "세션", "이벤트", "클릭", "구매", "비율", "율",
        "추이", "비교", "상위", "top", "채널", "소스", "매체", "국가", "기간", "전주", "지난주",
        "후원", "상품", "이름", "후원명", "donation_name", "경로", "트랜잭션", "처음", "신규"
    ]
    return any(t in q for t in tokens)


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

            # 짧은 후속 질의(예: "매출은 어때?")는 직전 breakdown 문맥을 우선 상속
            if last_state and intent == "metric_single":
                q_short = (question or "").strip().lower()
                has_metric_word = any(t in q_short for t in ["매출", "수익", "구매", "사용자", "세션", "전환"])
                has_dim_word = any(t in q_short for t in ["채널", "소스", "매체", "국가", "유형", "카테고리", "후원명", "상품", "이름", "경로", "페이지"])
                if has_metric_word and not has_dim_word and len(q_short) <= 20 and (last_state.get("dimensions") or last_state.get("intent") in ["breakdown", "topn", "comparison"]):
                    modifiers["needs_breakdown"] = True
                    modifiers["needs_total"] = modifiers.get("needs_total", True)
                    inherited_dims = []
                    for d in (last_state.get("dimensions") or []):
                        d_name = d.get("name") if isinstance(d, dict) else None
                        if d_name:
                            inherited_dims.append({"name": d_name, "score": 0.98, "matched_by": "pipeline_followup_inherit", "scope": "event"})
                    if inherited_dims:
                        existing = {d.get("name") for d in dimension_candidates}
                        for d in inherited_dims:
                            if d["name"] not in existing:
                                dimension_candidates.append(d)

            # 설명형 일반 질문은 데이터 매칭 실패 전이라도 LLM/폴백 답변 제공
            if _looks_explanatory_question(question) and not _has_data_signal(question):
                return {
                    "status": "ok",
                    "message": _safe_general_answer(question),
                    "account": property_id,
                    "blocks": [],
                    "plot_data": [],
                    "matching_debug": matching_debug
                }

            q = (question or "").lower()
            # trend/비교성 사용자 질문은 후보가 약해도 기본 지표를 보강
            if not metric_candidates:
                if intent == "trend" and any(k in q for k in ["사용자", "유저", "세션", "추이", "일별", "흐름"]):
                    metric_candidates = [{"name": "activeUsers", "score": 0.86, "matched_by": "pipeline_trend_default", "scope": "event"}]
                elif intent == "comparison" and any(k in q for k in ["사용자", "유저", "세션"]):
                    metric_candidates = [{"name": "activeUsers", "score": 0.82, "matched_by": "pipeline_compare_default", "scope": "event"}]
            period_terms = [
                "언제부터", "언제까지", "기간", "몇일부터", "몇일", "from", "to",
                "기준이야", "기준이야?", "기준인가", "기준이냐", "기준이", "기준은", "기준"
            ]
            relative_period_terms = ["지난주", "이번주", "지난달", "이번달", "어제", "오늘"]
            is_period_inquiry = any(k in q for k in period_terms) or any(k in q for k in relative_period_terms)
            analytics_tokens = [
                "매출", "수익", "사용자", "세션", "전환", "클릭", "구매", "후원", "후원자", "신규", "처음",
                "top", "상위", "비율", "추이", "원인", "분석", "상품", "경로", "채널", "소스", "매체"
            ]
            is_period_only_question = is_period_inquiry and not any(t in q for t in analytics_tokens)
            # 날짜 확인 질문은 직전/파싱 기간을 바로 응답
            if is_period_only_question:
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
                    "메뉴명", "후원명", "묶어서", "전체", "스크롤", "페이지별", "click", "클릭",
                    "소스", "매체", "광고", "paid", "display", "name", "이름"
                ])
            )
            short_compare_followup = (
                len(q) <= 20 and
                any(k in q for k in ["비교", "대비", "증감", "차이", "vs"]) and
                has_prev_metrics
            )
            top_dim_score = dimension_candidates[0].get("score", 0) if dimension_candidates else 0
            has_dimension_signal = bool(dimension_candidates) and top_dim_score >= 0.60
            if (not metric_candidates and not short_dimension_followup and not short_compare_followup and not has_dimension_signal) or (not has_prev_metrics and top_score < 0.55 and not has_dimension_signal):
                if _looks_explanatory_question(question):
                    return {
                        "status": "ok",
                        "message": _safe_general_answer(question),
                        "account": property_id,
                        "blocks": [],
                        "plot_data": [],
                        "matching_debug": matching_debug
                    }
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
