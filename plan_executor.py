# plan_executor.py
# GA4 ExecutionPlan Executor
"""
Planner가 생성한 ExecutionPlan을 실행하는 레이어.
PlanBlock 단위로 GA4 API를 호출하고 결과를 표준화한다.
"""

import logging
import pandas as pd
from typing import List, Dict, Any, Tuple
from flask import session

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest, 
    FilterExpression, 
    Filter, 
    OrderBy,
    Dimension,
    Metric,
    DateRange
)
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

from planner import ExecutionPlan, PlanBlock
from ga4_metadata import GA4_METRICS, GA4_DIMENSIONS
from insight_presenter import present_raw_data


def format_value(v):
    """값 포맷팅 (숫자에 천단위 구분자)"""
    try:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return f"{v:,.0f}"
        return str(v)
    except:
        return str(v)


class PlanExecutor:
    """
    ExecutionPlan을 실행하는 엔진
    
    사용법:
    executor = PlanExecutor()
    results = executor.execute(execution_plan, property_id)
    """
    
    def __init__(self):
        pass
    
    def execute(
        self,
        execution_plan: ExecutionPlan,
        property_id: str
    ) -> Dict[str, Any]:
        """
        ExecutionPlan 실행
        
        Args:
            execution_plan: Planner가 생성한 실행 계획
            property_id: GA4 속성 ID
        
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
        logging.info(f"[PlanExecutor] Executing plan with {len(execution_plan.blocks)} blocks")
        
        block_results = []
        
        for block in execution_plan.blocks:
            logging.info(f"[PlanExecutor] Executing block: {block.block_id}")
            
            try:
                result = self._execute_block(
                    block=block,
                    property_id=property_id,
                    start_date=execution_plan.start_date,
                    end_date=execution_plan.end_date
                )
                
                if result:
                    block_results.append(result)
            
            except Exception as e:
                logging.error(f"[PlanExecutor] Block {block.block_id} failed: {e}", exc_info=True)
                # Block 실패 시 계속 진행 (partial success)
                continue
        
        # 결과 통합
        final_response = {
            "status": "ok" if block_results else "partial_error",
            "message": f"{len(block_results)}개 블록 분석 완료",
            "account": property_id,
            "period": f"{execution_plan.start_date} ~ {execution_plan.end_date}",
            "blocks": block_results,
            "plot_data": []
        }
        
        logging.info(f"[PlanExecutor] Execution complete: {len(block_results)} blocks succeeded")
        return final_response
    
    def _execute_block(
        self,
        block: PlanBlock,
        property_id: str,
        start_date: str,
        end_date: str
    ) -> Dict[str, Any]:
        """
        단일 PlanBlock 실행
        
        Returns:
            {
              "block_id": "...",
              "title": "...",
              "type": "total" | "breakdown",
              "data": {...} or [...]
            }
        """
        # GA4 API 호출
        df = self._call_ga4_api(
            property_id=property_id,
            metrics=block.metrics,
            dimensions=block.dimensions,
            start_date=start_date,
            end_date=end_date,
            filters=block.filters,
            order_bys=block.order_bys,
            limit=block.limit
        )
        
        if df.empty:
            logging.warning(f"[PlanExecutor] Block {block.block_id} returned empty DataFrame")
            return None
        
        # 결과 처리
        if block.block_type == "total":
            return self._process_total_block(block, df)
        elif block.block_type in ["breakdown", "breakdown_topn"]:
            return self._process_breakdown_block(block, df)
        elif block.block_type == "trend":
            return self._process_trend_block(block, df)
        else:
            logging.warning(f"[PlanExecutor] Unknown block type: {block.block_type}")
            return None
    
    def _call_ga4_api(
        self,
        property_id: str,
        metrics: List[Dict],
        dimensions: List[Dict],
        start_date: str,
        end_date: str,
        filters: Dict,
        order_bys: List[Dict],
        limit: int = None
    ) -> pd.DataFrame:
        """
        GA4 Data API 호출
        
        Returns:
            DataFrame with columns: [dimension1, dimension2, ..., metric1, metric2, ...]
        """
        # Credentials
        credentials = Credentials(
            token=session['credentials']['token'],
            refresh_token=session['credentials'].get('refresh_token'),
            token_uri=session['credentials']['token_uri'],
            client_id=session['credentials']['client_id'],
            client_secret=session['credentials']['client_secret']
        )
        
        if credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
        
        client = BetaAnalyticsDataClient(credentials=credentials)
        
        # Build request
        ga4_dimensions = [Dimension(name=d["name"]) for d in dimensions]
        ga4_metrics = [Metric(name=m["name"]) for m in metrics]
        
        date_ranges = [DateRange(start_date=start_date, end_date=end_date)]
        
        # Filters
        filter_ex = None
        if filters and filters.get("event_filter"):
            filter_ex = FilterExpression(
                filter=Filter(
                    field_name="eventName",
                    string_filter=Filter.StringFilter(value=filters["event_filter"])
                )
            )
        
        # Order by
        ga4_order_bys = []
        if order_bys:
            for order in order_bys:
                if "metric" in order:
                    ga4_order_bys.append(
                        OrderBy(
                            metric=OrderBy.MetricOrderBy(metric_name=order["metric"]),
                            desc=order.get("desc", True)
                        )
                    )
                elif "dimension" in order:
                    ga4_order_bys.append(
                        OrderBy(
                            dimension=OrderBy.DimensionOrderBy(dimension_name=order["dimension"]),
                            desc=order.get("desc", False)
                        )
                    )
        
        # Request
        request = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=ga4_dimensions,
            metrics=ga4_metrics,
            date_ranges=date_ranges,
            dimension_filter=filter_ex,
            order_bys=ga4_order_bys if ga4_order_bys else None,
            limit=limit
        )
        
        logging.info(f"[GA4 API] Calling with {len(ga4_dimensions)} dims, {len(ga4_metrics)} metrics")
        
        # Execute
        response = client.run_report(request)
        
        # Parse response
        rows = []
        for row in response.rows:
            row_data = {}
            
            # Dimensions
            for i, dim_value in enumerate(row.dimension_values):
                dim_name = dimensions[i]["name"]
                row_data[dim_name] = dim_value.value
            
            # Metrics
            for i, metric_value in enumerate(row.metric_values):
                metric_name = metrics[i]["name"]
                row_data[metric_name] = metric_value.value
            
            rows.append(row_data)
        
        df = pd.DataFrame(rows)
        
        # Numeric conversion
        for m in metrics:
            if m["name"] in df.columns:
                df[m["name"]] = pd.to_numeric(df[m["name"]], errors="coerce")
        
        logging.info(f"[GA4 API] Returned {len(df)} rows")
        return df
    
    def _process_total_block(self, block: PlanBlock, df: pd.DataFrame) -> Dict[str, Any]:
        """Total block 처리 (합계만)"""
        total_values = {}
        
        for m in block.metrics:
            key = m["name"]
            if key in df.columns:
                val = pd.to_numeric(df[key], errors="coerce").sum()
                if pd.notnull(val):
                    total_values[key] = format_value(float(val))
        
        return {
            "block_id": block.block_id,
            "title": block.title or "전체 요약",
            "type": "total",
            "data": total_values
        }
    
    def _process_breakdown_block(self, block: PlanBlock, df: pd.DataFrame) -> Dict[str, Any]:
        """Breakdown block 처리 (행 데이터)"""
        raw = df.where(pd.notnull(df), None).to_dict(orient="records")
        raw = present_raw_data(raw)
        
        return {
            "block_id": block.block_id,
            "title": block.title or "상세 분석",
            "type": "breakdown",
            "data": raw
        }
    
    def _process_trend_block(self, block: PlanBlock, df: pd.DataFrame) -> Dict[str, Any]:
        """Trend block 처리 (시계열 데이터)"""
        # Trend는 breakdown과 유사하지만 plot_data 생성 가능
        raw = df.where(pd.notnull(df), None).to_dict(orient="records")
        raw = present_raw_data(raw)
        
        return {
            "block_id": block.block_id,
            "title": block.title or "추이 분석",
            "type": "trend",
            "data": raw
        }
