# response_adapter.py
# Response Format Adapter
"""
새 파이프라인의 응답 형식을 기존 프론트엔드 형식으로 변환
"""

import logging
from typing import Dict, Any, List


def adapt_pipeline_response_to_legacy(pipeline_response: Dict[str, Any]) -> Dict[str, Any]:
    """
    새 파이프라인 응답을 기존 형식으로 변환
    """
    blocks = pipeline_response.get("blocks", [])
    
    if not blocks:
        return {
            "message": "죄송합니다, 요청하신 데이터를 찾을 수 없습니다.",
            "raw_data": [],
            "structured": {},
            "plot_data": []
        }
    
    # 메시지 생성 (자연스러운 대화체)
    message_parts = []
    raw_data = []
    structured = {}
    
    from ga4_metadata import GA4_METRICS
    
    # 블록 타입별 그룹화
    total_blocks = [b for b in blocks if b.get("type") == "total"]
    breakdown_blocks = [b for b in blocks if b.get("type") in ["breakdown", "breakdown_topn", "trend"]]
    
    # Total blocks 처리
    for block in total_blocks:
        data = block.get("data", {})
        
        for key, value in data.items():
            metric_info = GA4_METRICS.get(key, {})
            ui_name = metric_info.get("ui_name", key)
            category = metric_info.get("category", "")
            
            structured[ui_name] = value
            
            # 자연스러운 메시지 (카테고리에 따라 단위 변경)
            if category == "ecommerce" and "revenue" in key.lower():
                message_parts.append(f"{ui_name}은 **{value}**원입니다.")
            elif "user" in key.lower() or "visitor" in key.lower():
                message_parts.append(f"{ui_name}은 **{value}**명입니다.")
            elif "event" in key.lower() or "session" in key.lower():
                message_parts.append(f"{ui_name}은 **{value}**회입니다.")
            else:
                message_parts.append(f"{ui_name}: **{value}**")
    
    # Breakdown blocks 처리
    for block in breakdown_blocks:
        data = block.get("data")
        title = block.get("title", "상세 분석")
        
        if isinstance(data, list) and data:
            raw_data.extend(data)
            
            # 자연스러운 메시지
            count = len(data)
            if "TOP" in title.upper() or "상위" in title:
                message_parts.append(f"\n상위 {count}개 항목은 다음과 같습니다:")
            else:
                message_parts.append(f"\n{title} ({count}개 항목):")
    
    # 최종 메시지
    if not message_parts:
        final_message = "분석이 완료되었습니다."
    else:
        final_message = "\n".join(message_parts)
    
    return {
        "message": final_message,
        "raw_data": raw_data,
        "structured": structured,
        "plot_data": []
    }
