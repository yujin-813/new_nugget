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
