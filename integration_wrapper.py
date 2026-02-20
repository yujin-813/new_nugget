# integration_wrapper.py
# GA4 New Pipeline Integration Wrapper
"""
기존 handle_question과 새 파이프라인을 연결하는 통합 래퍼.
점진적 마이그레이션을 위한 Feature Flag 지원.
"""

import logging
from typing import Dict, Any, Optional

from pipeline import GA4Pipeline
from qa_module import GA4AnalysisEngine


# Feature Flag: 새 파이프라인 사용 여부
USE_NEW_PIPELINE = True  # True로 설정하면 새 파이프라인 사용


def _detect_response_misalignment(question: str, legacy_result: Dict[str, Any]) -> str:
    q = (question or "").lower()
    msg = str((legacy_result or {}).get("message", "") or "").lower()
    raw_data = (legacy_result or {}).get("raw_data") if isinstance(legacy_result, dict) else []
    sample = raw_data[0] if isinstance(raw_data, list) and raw_data and isinstance(raw_data[0], dict) else {}
    keys = set(sample.keys()) if isinstance(sample, dict) else set()

    buyer_terms = ["구매자수", "구매자 수", "구매자", "후원자", "매출 일으킨 사용자"]
    if any(t in q for t in buyer_terms):
        if ("원" in msg) and ("명" not in msg):
            return "buyer_as_revenue"

    percent_terms = ["퍼센트", "percent", "%", "비율", "율"]
    if any(t in q for t in percent_terms):
        if "%" not in msg:
            return "missing_percent"

    type_terms = ["유형", "타입", "종류"]
    if any(t in q for t in type_terms):
        has_type_signal = any(t in msg for t in ["정기후원 여부", "유형", "y", "n", "itemcategory", "상품 카테고리"])
        if not has_type_signal:
            return "missing_type_breakdown"

    if any(t in q for t in ["소스", "매체", "source", "medium"]):
        if ("소스/매체" not in msg and "sourcemedium" not in msg) and ("sourceMedium" not in keys):
            return "missing_source_medium"

    return ""


def _build_retry_question(question: str, reason: str) -> str:
    if reason == "buyer_as_revenue":
        return f"{question} (구매자수 기준, totalPurchasers)"
    if reason == "missing_percent":
        return f"{question} (비율 지표 기준, firstTimePurchaserRate)"
    if reason == "missing_type_breakdown":
        return f"{question} (유형별 분해 기준, customEvent:donation_name)"
    if reason == "missing_source_medium":
        return f"{question} (소스/매체 기준, sourceMedium)"
    return question


def handle_ga4_question(
    question: str,
    property_id: str,
    conversation_id: Optional[str] = None,
    semantic=None,
    user_name: str = ""
) -> Dict[str, Any]:
    """
    GA4 질문 처리 통합 래퍼
    
    Feature Flag에 따라 새 파이프라인 또는 기존 엔진 사용
    
    Args:
        question: 사용자 질문
        property_id: GA4 속성 ID
        conversation_id: 대화 ID
        semantic: SemanticMatcher
    
    Returns:
        {
          "status": "ok",
          "message": "...",
          "blocks": [...],
          "plot_data": []
        }
    """
    if USE_NEW_PIPELINE:
        logging.info("[Integration] Using NEW PIPELINE")
        pipeline = GA4Pipeline()
        result = pipeline.run(
            question=question,
            property_id=property_id,
            conversation_id=conversation_id,
            semantic=semantic
        )
        
        # 🔥 Convert new format to legacy format for frontend compatibility
        from response_adapter import adapt_pipeline_response_to_legacy
        legacy_result = adapt_pipeline_response_to_legacy(
            result,
            question=question,
            user_name=user_name
        )

        # 질문-응답 어긋남이 감지되면 1회 재시도 (강제 슬롯 힌트 부여)
        retry_reason = _detect_response_misalignment(question, legacy_result)
        if retry_reason:
            retry_q = _build_retry_question(question, retry_reason)
            if retry_q != question:
                logging.info(f"[Integration] Retry due to misalignment: {retry_reason} -> {retry_q}")
                retry_result = pipeline.run(
                    question=retry_q,
                    property_id=property_id,
                    conversation_id=conversation_id,
                    semantic=semantic
                )
                retry_legacy = adapt_pipeline_response_to_legacy(
                    retry_result,
                    question=question,  # 사용자 원문 기준으로 메시지 생성
                    user_name=user_name
                )
                retry_reason2 = _detect_response_misalignment(question, retry_legacy)
                if not retry_reason2:
                    return retry_legacy
        return legacy_result
    else:
        logging.info("[Integration] Using LEGACY ENGINE")
        engine = GA4AnalysisEngine()
        return engine.process(
            question=question,
            property_id=property_id,
            conversation_id=conversation_id,
            prev_source=None,
            semantic=semantic
        )


# 사용 예시:
# from integration_wrapper import handle_ga4_question
#
# result = handle_ga4_question(
#     question="상품별 매출 TOP 10",
#     property_id="123456",
#     conversation_id="conv_001"
# )
