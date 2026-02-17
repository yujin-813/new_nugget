import logging
import pandas as pd
from flask import session
import openai
from datetime import date, datetime, timedelta
import json
import re
import os
import hashlib
import difflib
import uuid
from dotenv import load_dotenv
import copy
def generate_unique_id():
    return str(uuid.uuid4())

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, FilterExpression, Filter, OrderBy
from google.analytics.data_v1beta.types import GetMetadataRequest

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request


from db_manager import DBManager, DB_PATH
from file_engine import file_engine
from mixed_engine import MixedAnalysisEngine
from query_parser import QueryParser
from state_resolver import StateResolver
from langchain_community.chat_message_histories import SQLChatMessageHistory
from prompt_builder import GA4PromptBuilder
from query_parser import DateParser, IntentClassifier
from insight_presenter import present_structured_insight, present_raw_data

from relation_classifier import classify_relation
from state_policy import apply_relation_policy
from ga4_metadata import (
    GA4_DIMENSIONS, GA4_METRICS, GA4_ADS_DIMENSIONS,
    DEFAULT_METRIC, DEFAULT_TIME_DIMENSION, DEFAULT_DIMENSION
)

def format_value(v):
    try:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return f"{v:,.0f}"
        return str(v)
    except:
        return str(v)


# Basic Setup
load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")

# [Logging Fix] Explicit configuration for visibility in Dev mode
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    force=True
)
logging.getLogger("werkzeug").setLevel(logging.INFO)

UPLOAD_FOLDER = 'uploaded_files'

from sqlalchemy import create_engine

class FallbackChatHistory:
    """ 
    LangChain SQLChatMessageHistory가 깨질 경우를 대비한 안전 fallback.
    시스템이 죽지 않게 최소한의 add_user_message / add_ai_message만 제공.
    """
    def __init__(self):
        self.messages = []

    def add_user_message(self, content: str):
        self.messages.append({"role": "user", "content": content})

    def add_ai_message(self, content: str):
        self.messages.append({"role": "assistant", "content": content})


class ConversationSummaryMemory:
    """대화의 요약 및 문맥(Context) 관리 전담 - Conversation ID 기반"""

    @staticmethod
    def get_chat_history(user_id, conversation_id):
        """
        LangChain SQLChatMessageHistory를 안전하게 생성.
        LangChain 버전마다 인자명이 달라서 try/except로 대응한다.
        """
        session_key = f"{user_id}:{conversation_id}"
        db_url = f"sqlite:///{DB_PATH}"

        try:
            # 최신 방식: engine 직접 주입 가능 버전 대응
            engine = create_engine(db_url)

            try:
                return SQLChatMessageHistory(
                    table_name="chat_history",
                    session_id=session_key,
                    engine=engine
                )
            except TypeError:
                pass

            # 다른 버전: connection_string
            try:
                return SQLChatMessageHistory(
                    table_name="chat_history",
                    session_id=session_key,
                    connection_string=db_url
                )
            except TypeError:
                pass

            # 구버전: connection
            try:
                return SQLChatMessageHistory(
                    table_name="chat_history",
                    session_id=session_key,
                    connection=db_url
                )
            except TypeError:
                pass

            raise RuntimeError("SQLChatMessageHistory init signature mismatch")

        except Exception as e:
            logging.error(f"[ConversationSummaryMemory] Failed to init SQLChatMessageHistory: {e}")
            return FallbackChatHistory()

class GA4AnalysisEngine:
    def __init__(self):
        DBManager.init_db()
        self.response_cache = {}
        self.metadata_cache = {}
    def _map_custom_metric(self, metric_name, metadata):
        # 1. 이미 정식 metric이면 그대로 사용
        if metric_name in metadata["metrics"]:
            return metric_name

        # 2. customEvent metric 시도
        candidate = f"customEvent:{metric_name}"
        if candidate in metadata["metrics"]:
            return candidate

        # 3. customUser metric
        candidate = f"customUser:{metric_name}"
        if candidate in metadata["metrics"]:
            return candidate

        # 4. customItem metric
        candidate = f"customItem:{metric_name}"
        if candidate in metadata["metrics"]:
            return candidate

        return None

    def process(self, question, property_id, conversation_id=None, prev_source=None, semantic=None):
       


        
        logging.info("====================================================")
        logging.info("🔥 [GA4 ENGINE START]")
        logging.info(f"[INPUT] question={question}")
        logging.info(f"[INPUT] property_id={property_id}")
        logging.info(f"[INPUT] conversation_id={conversation_id}")
        logging.info("====================================================")
        if property_id not in self.metadata_cache:
            self.metadata_cache[property_id] = self._load_metadata(property_id)

        cache_hit, cached_resp = self._check_cache(property_id, question, conversation_id)
        if cache_hit:
            logging.info("⚡ Cache HIT → Returning cached response")
            return cached_resp

        try:
            # STEP 1 [Source Reset Logic (v9.1)]
            if prev_source and prev_source != "ga4":
                logging.info(f"[GA4 Engine] Source changed ({prev_source} -> ga4). Loading fresh state.")
                last_state = None
            else:
                last_state = DBManager.load_last_state(conversation_id, source="ga4")
            
            logging.info(f"[STEP 1] Loaded last_state: {last_state}")

            # STEP 2
            
            deep_keywords = ["자세히", "상세히", "왜", "원인", "전략", "인사이트", "해석", "의미", "분석해"]
            
            date_context = DateParser.get_default_context()
            
            # [Phase 7] Intent Layer Integration
            intent = IntentClassifier.classify(question)
            logging.info(f"[GA4Engine] Early Intent Classification: {intent}")

            delta = QueryParser.parse_to_delta(
                question,
                last_state,
                date_context,
                semantic=semantic
            )

            logging.info(f"[DEBUG] delta metrics raw: {delta.get('metrics')}")
            logging.info(f"[DEBUG] delta metrics type: {type(delta.get('metrics'))}")
            # 🔥 Clarify Detection (Semantic Mid Confidence)
            if delta.get("clarify_candidates"):
                candidate = delta["clarify_candidates"][0]

                session["pending_clarify"] = {
                    "type": candidate["type"],      # metric or dimension
                    "value": candidate["candidate"]
                }

                return {
                    "message": f"혹시 '{candidate['candidate']}'을 의미하셨나요?",
                    "status": "clarify",
                    "options": ["예", "아니오"],
                    "account": property_id,
                    "period": "확인 필요",
                    "plot_data": []
                }


            # [P2] Detect Trend Intent (Force Time Series)
            if intent == "trend" or any(kw in question for kw in ["추이", "흐름", "그래프", "daily", "일별", "변화", "추세"]):
                logging.info(f"[Trend] Detected trend keywords in question or intent: {question}")
                delta["is_trend_query"] = True
                existing_dims = delta.get("dimensions", [])
                if not any(d["name"] == "date" for d in existing_dims):
                    delta["dimensions"] = [{"name": "date"}] + existing_dims
                logging.info(f"[Trend] Dimension overridden to 'date'")


            analysis_plan = {
                "metrics": delta.get("metrics", []),
                "group_by": delta.get("dimensions", []),
                "trend": delta.get("is_trend_query", False),
                "comparison": len(delta.get("periods", [])) > 1,
                "analysis_depth": "detailed" if any(k in question for k in deep_keywords) else "short"
            }

            delta["analysis_plan"] = analysis_plan
            logging.info(f"[STEP 2] Parsed delta: {delta}")
            
           
            # STEP 3
            relation = classify_relation(question, last_state, delta)
            logging.info(f"[Relation] relation={relation}")

            last_state_for_merge = apply_relation_policy(last_state, relation)

            final_state = StateResolver.resolve(
                last_state_for_merge,
                delta,
                source="ga4",
                prev_source=prev_source,
                question=question
            )

            final_state["analysis_plan"] = delta.get("analysis_plan", {})
            logging.info(f"[DEBUG] analysis_plan type after resolve: {type(final_state.get('analysis_plan'))}")
            
            # 🔥 Breakdown이면 이전 dimension 상속 금지
            if intent == "breakdown":
                logging.info("[Fix] Breakdown detected → reset dimensions")
                final_state["dimensions"] = delta.get("dimensions", [])

            
            
            # [Phase 7] State Pollution Prevention & Dimension Logic
            # 🔥 절대 보호 장치
            if not isinstance(final_state.get("analysis_plan"), dict):
                logging.warning("[FIX] analysis_plan corrupted → rebuilding safely")
                final_state["analysis_plan"] = {
                    "metrics": final_state.get("metrics", []),
                    "group_by": final_state.get("dimensions", []),
                    "trend": final_state.get("is_trend_query", False),
                    "comparison": len(final_state.get("periods", [])) > 1,
                    "analysis_depth": "short"
                }

            if len(delta.get("metrics", [])) > 1:
                final_state["metrics"] = delta["metrics"]

            metadata = self.metadata_cache.get(property_id)
            if not metadata:
                self.metadata_cache[property_id] = self._load_metadata(property_id)
                metadata = self.metadata_cache[property_id]

            orig_dims = final_state.get("dimensions", [])
            mapped_dims = []
            for d in orig_dims:
                mapped = self._map_custom_dimension(d["name"], metadata)
                if mapped:
                    mapped_dims.append({"name": mapped})
                else:
                    # 못 찾으면 그냥 원본 유지(= GA4 기본 dimension일 수 있음)
                    mapped_dims.append(d)

            final_state["dimensions"] = mapped_dims
            orig_metrics = final_state.get("metrics", [])
            mapped_metrics = []

            for m in orig_metrics:
                mapped = self._map_custom_metric(m["name"], metadata)
                if mapped:
                    mapped_metrics.append({"name": mapped})
                else:
                    mapped_metrics.append(m)

            final_state["metrics"] = mapped_metrics

            logging.info(f"[STEP 3] Final state: {final_state}")

            
            # STEP 4: Resolve Event & Dimensions
            final_state, event_context = self._resolve_event(property_id, final_state)

            logging.info(f"[STEP 4] Event Context Resolved: {event_context}")

            # STEP 5: Ambiguity Detection
            need_clarify, clarification = self._detect_ambiguity(last_state, delta)
            if need_clarify:
                return {
                    "message": f"🤔 {clarification}",
                    "status": "clarify",
                    "options": ["예", "아니오"],
                    "account": "Ambiguity Detector",
                    "period": "대기",
                    "plot_data": []
                }

            # [Execution Safeguard] Force Default Dates if missing
            if not final_state.get("start_date") and not final_state.get("periods"):
                logging.info("[GA4 Engine] No dates found. Applying default (Last 7 Days).")
                final_state["start_date"] = date_context["last_week"]["start_date"]
                final_state["end_date"] = date_context["today"]
            
           
            # 🔥 Scope 분리 실행 로직 추가
            metrics = final_state.get("metrics", [])
            scope_groups = self._split_metrics_by_scope(metrics)

            if len(scope_groups) > 1:
                logging.info(f"[MultiScope] Detected multiple scopes: {list(scope_groups.keys())}")

                result = self._execute_multi_scope_queries(property_id, final_state, scope_groups)

                # ✅ multi-scope여도 state 저장은 반드시 해야 함
                if conversation_id:
                    DBManager.save_success_state(conversation_id, "ga4", final_state)

                # ✅ multi-scope 결과도 last_result 저장해야 followup 가능
                if conversation_id:
                    DBManager.save_last_result(conversation_id, "ga4", result)

                # 캐시 저장도 가능하면 유지
                self._store_cache(property_id, question, result, conversation_id)

                return result


            # STEP 6: Run GA4 Query
            df, start_date, end_date = self._run_ga4_query(property_id, final_state)
            logging.info(f"[STEP 6] Rows: {len(df)}")

            # [Step 7] Transform to Structured Plot Data (v9.2) - with ChartSelector
            plot_data = self._transform_to_plot_data(df, final_state)
            logging.info(f"[STEP 7] Plot Data Type: {plot_data.get('type')}")

            # [Step 8] Update Event Registry
            self._update_event_registry(property_id, df)

            # [Step 9] Insight Generation
            insight_result = self._generate_insight(
                final_state, df, start_date, end_date, question, event_context, date_context
            )
            logging.info("[STEP 9] Insight Generated")

            if conversation_id: 
                DBManager.save_success_state(conversation_id, "ga4", final_state)

            if isinstance(insight_result, dict) and "structured" in insight_result:
                message = insight_result.get("fallback", "분석 완료")
                structured_insight = insight_result["structured"]
            else:
                message = insight_result
                structured_insight = None
            
            
            structured_insight = present_structured_insight(structured_insight)

            raw_data = df.where(pd.notnull(df), None).to_dict(orient='records')[:50]
            raw_data = present_raw_data(raw_data)

            response = {
                "message": message,
                "status": "ok",
                "account": property_id,
                "period": f"{start_date} ~ {end_date}",
                "plot_data": plot_data,
                "structured_insight": structured_insight,
                "raw_data": raw_data    
            }

            if conversation_id:
                DBManager.save_last_result(conversation_id, "ga4", response)

            self._store_cache(property_id, question, response, conversation_id)
            return response

        except Exception as e:
            logging.error(f"[GA4 Engine Error] {e}")
            return {
                "message": f"❌ 분석 중 오류가 발생했습니다: {str(e)}",
                "account": "Error System",
                "period": "Error",
                "plot_data": []
            }
  
    def _get_credentials(self):
        credentials = Credentials(
            token=session['credentials']['token'],
            refresh_token=session['credentials'].get('refresh_token'),
            token_uri=session['credentials']['token_uri'],
            client_id=session['credentials']['client_id'],
            client_secret=session['credentials']['client_secret']
        )
        if credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
        return credentials


    def _load_metadata(self, property_id):
        client = BetaAnalyticsDataClient(credentials=self._get_credentials())
        request = GetMetadataRequest(name=f"properties/{property_id}/metadata")
        response = client.get_metadata(request)

        return {
            "dimensions": [d.api_name for d in response.dimensions],
            "metrics": [m.api_name for m in response.metrics],
        }

    def _check_cache(self, property_id, question, conversation_id):
        norm_q = re.sub(r'[^가-힣a-zA-Z0-9\s]', '', question).lower()
        q_hash = hashlib.md5(norm_q.encode()).hexdigest()
        key = f"{conversation_id}:{property_id}:{q_hash}"
        if key in self.response_cache:
            return True, self.response_cache[key]
        return False, None

    def _store_cache(self, property_id, question, response, conversation_id):
        norm_q = re.sub(r'[^가-힣a-zA-Z0-9\s]', '', question).lower()
        q_hash = hashlib.md5(norm_q.encode()).hexdigest()
        key = f"{conversation_id}:{property_id}:{q_hash}"
        self.response_cache[key] = response

    def _resolve_event(self, property_id, state):
        context = None
        event_candidate = state.get("event_filter") or state.get("event_candidate")
        if event_candidate:
            try:
                events = DBManager.get_events(property_id)
                # [Phase 7] Threshold lowered to 0.5 for better matching
                matches = difflib.get_close_matches(event_candidate, events, n=1, cutoff=0.5)
                if matches:
                    matched = matches[0]
                    score = difflib.SequenceMatcher(None, event_candidate, matched).ratio()
                    if score >= 0.8:
                        state["event_filter"] = matched
                        context = [{"eventName": matched, "score": score}]
            except Exception as e:
                logging.error(f"[Engine] Event Resolve Error: {e}")
        return state, context

    def _transform_to_plot_data(self, df, state):
        """Transform DataFrame to structured JSON for ApexCharts consumption (v9.2/v10.0)"""
        if df is None or df.empty:
            return {"type": None, "labels": [], "series": []}

        df_cols = df.columns.tolist()
        dimensions = state.get("dimensions", [])
        periods = state.get("periods", [])
        
        actual_dims = [d["name"] for d in dimensions if d["name"] in df_cols]
        if not actual_dims and "date" in df_cols:
            actual_dims = ["date"]

        if actual_dims:
            time_dims = ["date", "week", "month", "yearMonth"]
            is_time_series = any(d in time_dims for d in actual_dims)
            
            # [Phase 7] Chart Selection Logic (ChartSelector Rules)
            plan = state.get("analysis_plan", {})

            # 🔥 안전 가드
            if not isinstance(plan, dict):
                logging.warning(f"[Safety] analysis_plan corrupted in plot transform: {type(plan)}")
                plan = {}


            is_trend = plan.get("trend", False)
            group_by = plan.get("group_by", [])
            metrics = plan.get("metrics", [])
            is_comparison = plan.get("comparison", False)

            # 기본값
            chart_type = "bar"

            # 1. Trend (Trend가 최우선)
            if is_trend and group_by:
                chart_type = "line"

            elif is_trend:
                chart_type = "line"

            # 2. Comparison
            elif is_comparison:
                chart_type = "bar_grouped"

            # 3. Multi metric
            elif len(metrics) > 1:
                chart_type = "bar_grouped"

            # 4. Breakdown
            elif group_by:
                row_count = len(df)
                chart_type = "bar_h" if row_count > 10 else "bar"


            
            logging.info(f"[ChartSelector] Selected Chart Type: {chart_type} (Plan: {plan})")

            
            if is_time_series:
                try:
                    df = df.copy()
                    df[actual_dims[0]] = pd.to_datetime(df[actual_dims[0]])
                    df = df.sort_values(by=actual_dims[0])
                    if actual_dims[0] == 'date':
                        df[actual_dims[0]] = df[actual_dims[0]].dt.strftime('%Y-%m-%d')
                except:
                    pass
            
            labels = df[actual_dims[0]].astype(str).tolist()
            series = []
            
            metrics = plan.get("metrics", [])
            for m in metrics:
                m_name = m["name"]
                if m_name in df_cols:
                    clean_data = [float(x) if pd.notnull(x) else 0 for x in df[m_name].tolist()]
                    series.append({"name": self._get_metric_display_name(m_name), "data": clean_data})
                
                for p in periods:
                    suffixed = f"{m_name}_{p['label']}"
                    if suffixed in df_cols:
                        clean_data = [float(x) if pd.notnull(x) else 0 for x in df[suffixed].tolist()]
                        series.append({"name": f"{self._get_metric_display_name(m_name)} ({p['label']})", "data": clean_data})
            
            return {"type": chart_type, "labels": labels, "series": series}

        return {"type": "bar", "labels": [], "series": []}

    def _get_metric_display_name(self, metric_key):
        return GA4_METRICS.get(metric_key, {}).get("ui_name", metric_key)

    def _detect_ambiguity(self, last, delta):
        if delta.get("event_candidate") and not delta.get("metrics"):
            return True, "이벤트 발생 횟수를 볼까요, 아니면 참여한 사용자 수를 볼까요?"
        return False, None

    def _split_metrics_by_scope(self, metrics):
        """
        Metric들을 GA4 scope 기준으로 그룹화
        scope 정보는 GA4_METRICS의 'scope' 필드를 사용
        default는 'event'
        """
        scope_groups = {}
        logging.info(f"[DEBUG] Raw metrics input: {metrics}")
        logging.info(f"[DEBUG] Type of metrics: {type(metrics)}")

        for m in metrics:
            logging.info(f"[DEBUG] GA4_METRICS type: {type(GA4_METRICS)}")
            logging.info(f"[DEBUG] m: {m}")
            logging.info(f"[DEBUG] m.get('name'): {m.get('name')}")
            logging.info(f"[DEBUG] GA4_METRICS.get(m['name']): {GA4_METRICS.get(m['name'])}")
            meta = GA4_METRICS.get(m["name"], {})
            scope = meta.get("scope", "event")

            scope_groups.setdefault(scope, []).append(m)

        return scope_groups

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

    def _filter_dimensions_by_scope(self, dimensions, target_scope):
        filtered = []
        for d in dimensions:
            meta = GA4_DIMENSIONS.get(d["name"], {})
            cat = meta.get("category")
            dim_scope = meta.get("scope") or self.CATEGORY_TO_SCOPE.get(cat, "event")

            if dim_scope == target_scope:
                filtered.append(d)
        return filtered


    def _get_default_dimension_for_scope(self, scope):
        candidates = []
        for dim_name, meta in GA4_DIMENSIONS.items():
            cat = meta.get("category")
            dim_scope = meta.get("scope") or self.CATEGORY_TO_SCOPE.get(cat, "event")
            if dim_scope == scope and meta.get("category") != "time":
                candidates.append((dim_name, meta.get("priority", 0)))

        if not candidates:
            return []

        candidates.sort(key=lambda x: x[1], reverse=True)
        return [{"name": candidates[0][0]}]


    def _execute_multi_scope_queries(self, property_id, final_state, scope_groups):
        blocks = []

        original_group_by = final_state.get("analysis_plan", {}).get("group_by", [])

        for scope, metrics in scope_groups.items():
            sub_state = copy.deepcopy(final_state)
            sub_state["metrics"] = metrics

            sub_plan = sub_state.get("analysis_plan", {}).copy()
            sub_plan["metrics"] = metrics

            scoped_group_by = self._filter_dimensions_by_scope(original_group_by, scope)

            if scope == "item":
                if not scoped_group_by:
                    scoped_group_by = [{"name": "itemName"}]

            sub_state["dimensions"] = scoped_group_by
            sub_plan["group_by"] = scoped_group_by
            sub_state["analysis_plan"] = sub_plan

            df, start_date, end_date = self._run_ga4_query(property_id, sub_state)
            if df.empty:
                continue

            if scope == "event":
                total_values = {}

                # 🔥 event scope는 항상 dimension 제거 후 단일 total 쿼리
                total_state = copy.deepcopy(sub_state)
                total_state["dimensions"] = []
                total_state["analysis_plan"]["group_by"] = []

                total_df, _, _ = self._run_ga4_query(property_id, total_state)

                for m in metrics:
                    key = m["name"]
                    if key in total_df.columns:
                        val = pd.to_numeric(total_df[key], errors="coerce").sum()
                        if pd.notnull(val):
                            total_values[key] = format_value(float(val))

                blocks.append({
                    "title": "전체 지표 요약",
                    "scope": scope,
                    "data": total_values
                })


            elif scope == "item":
                raw = df.where(pd.notnull(df), None).to_dict(orient="records")
                raw = present_raw_data(raw)

                blocks.append({
                    "title": "상품별 지표",
                    "scope": scope,
                    "data": raw
                })

        return {
            "message": "분리 분석 완료",
            "status": "ok",
            "account": property_id,
            "period": f"{final_state.get('start_date')} ~ {final_state.get('end_date')}",
            "blocks": blocks,
            "plot_data": []
        }




    def _run_ga4_query(self, property_id, state):

        plan = state.get("analysis_plan", {})

        # 🔥 안전 가드 추가
        if not isinstance(plan, dict):
            logging.warning(f"[Safety] analysis_plan corrupted: {type(plan)} → resetting to empty dict")
            plan = {}

        

        metrics = plan.get("metrics") or state.get("metrics") or [{"name": DEFAULT_METRIC}]
        dimensions = state.get("dimensions")

        if not dimensions:
            group_by = state.get("analysis_plan", {}).get("group_by", [])
            if group_by:
                dimensions = group_by
            else:
                dimensions = ([{"name": DEFAULT_TIME_DIMENSION}] if DEFAULT_TIME_DIMENSION else [])


        periods = state.get("periods", [])
        
        if not periods:
            periods = [{"label": "current", "start_date": state.get("start_date"), "end_date": state.get("end_date")}]
        
        # Comparison logic: Remove time dims if not trend
        plan = state.get("analysis_plan")
        if not isinstance(plan, dict):
            plan = {}

        if len(periods) > 1 and not plan.get("trend", False):
            time_dims = ["date", "week", "month", "yearMonth"]
            dimensions = [d for d in dimensions if d.get("name") not in time_dims]

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
        filter_ex = None
        if state.get('event_filter'):
            filter_ex = FilterExpression(filter=Filter(field_name="eventName", string_filter=Filter.StringFilter(value=state["event_filter"])))

        dfs = []
        for p in periods:

            # 🔥 Auto OrderBy for Top-N
            order_bys = None
            if state.get("limit") and metrics:
                order_metric = None
                for m in metrics:
                    if m["name"] in GA4_METRICS:
                        order_metric = m["name"]
                        break

                if order_metric:
                    order_bys = [
                        OrderBy(
                            metric=OrderBy.MetricOrderBy(metric_name=order_metric),
                            desc=True
                        )
                    ]


            request = RunReportRequest(
                property=f"properties/{property_id}",
                date_ranges=[{"start_date": p["start_date"], "end_date": p["end_date"]}],
                dimensions=dimensions,
                metrics=metrics,
                dimension_filter=filter_ex,
                order_bys=order_bys,
                limit=str(state.get("limit")) if state.get("limit") else None
            )


            response = client.run_report(request)
            headers = [h.name for h in response.dimension_headers] + [h.name for h in response.metric_headers]
            rows = [[d.value for d in r.dimension_values] + [m.value for m in r.metric_values] for r in response.rows]
            sub_df = pd.DataFrame(rows, columns=headers)
            for m in response.metric_headers:
                sub_df[m.name] = pd.to_numeric(sub_df[m.name], errors='coerce')
            if len(periods) > 1:
                sub_df.rename(columns={m.name: f"{m.name}_{p['label']}" for m in response.metric_headers}, inplace=True)
            dfs.append(sub_df)

        if len(dfs) == 1:
            return dfs[0], periods[0]["start_date"], periods[0]["end_date"]
        else:
            final_df = dfs[0]
            join_keys = [d["name"] for d in dimensions]
            for i in range(1, len(dfs)):
                if join_keys: final_df = pd.merge(final_df, dfs[i], on=join_keys, how='outer')
                else: final_df = pd.concat([final_df, dfs[i]], axis=1)
            final_df.fillna(0, inplace=True)
            return final_df, min(p["start_date"] for p in periods), max(p["end_date"] for p in periods)

    def _map_custom_dimension(self, dimension_name, metadata):
        # 이미 full API name이면 그대로 사용
        if dimension_name in metadata["dimensions"]:
            return dimension_name

        # customEvent 자동 매핑 시도
        candidate = f"customEvent:{dimension_name}"
        if candidate in metadata["dimensions"]:
            return candidate

        # customUser 자동 매핑
        candidate = f"customUser:{dimension_name}"
        if candidate in metadata["dimensions"]:
            return candidate

        # customItem 자동 매핑
        candidate = f"customItem:{dimension_name}"
        if candidate in metadata["dimensions"]:
            return candidate

        return None

    def _update_event_registry(self, property_id, df):
        if "eventName" in df.columns:
            events = df["eventName"].unique().tolist()
            DBManager.save_events(property_id, events)

    def _calculate_metrics(self, df, state):

        plan = state.get("analysis_plan", {})

        # 🔥 안전 가드
        if not isinstance(plan, dict):
            logging.warning(f"[Safety] analysis_plan corrupted in _calculate_metrics: {type(plan)}")
            plan = {}

        metrics = plan.get("metrics") or state.get("metrics") or [{"name": DEFAULT_METRIC}]

        dimensions = state.get("dimensions")
        if not isinstance(dimensions, list):
            dimensions = []

        if not dimensions and DEFAULT_TIME_DIMENSION:
            dimensions = [{"name": DEFAULT_TIME_DIMENSION}]

        periods = state.get("periods") or []


        result = {
            "metrics": {},
            "primary_metric": None
        }

        if df.empty:
            return result

        for idx, metric in enumerate(metrics):
            metric_key = metric["name"]
            ui_name = self._get_metric_display_name(metric_key)

            metric_data = {
                "label": ui_name,
                "current": None,
                "previous": None,
                "diff": None,
                "growth": None
            }

            # 비교 분석일 경우
            if len(periods) > 1:
                curr_label = periods[-1]["label"]
                prev_label = periods[-2]["label"]

                curr_col = f"{metric_key}_{curr_label}"
                prev_col = f"{metric_key}_{prev_label}"

                if curr_col in df.columns and prev_col in df.columns:
                    curr_val = df[curr_col].sum()
                    prev_val = df[prev_col].sum()

                    diff = curr_val - prev_val
                    growth = (diff / prev_val * 100) if prev_val != 0 else 0

                    metric_data.update({
                        "current": float(curr_val),
                        "previous": float(prev_val),
                        "diff": float(diff),
                        "growth": round(growth, 1)
                    })

            else:
                if metric_key in df.columns:
                    curr_val = df[metric_key].sum()
                    metric_data.update({
                        "current": float(curr_val)
                    })

            result["metrics"][metric_key] = metric_data

            # 첫 번째 metric을 primary로 지정
            if idx == 0:
                result["primary_metric"] = metric_key

        return result


    def _generate_insight(self, state, df, start_date, end_date, question, event_context, date_context=None):

        if df.empty:
            return {
                "structured": {
                    "title": "데이터 없음",
                    "main_metric": None,
                    "delta": None,
                    "comparison": None,
                    "insight_narrative": "선택한 기간에 해당하는 데이터가 존재하지 않습니다."
                },
                "fallback": "데이터 없음"
            }

        # 1️⃣ Deterministic Metric Calculation
        metric_info = self._calculate_metrics(df, state)

        metrics_dict = metric_info.get("metrics", {})
        primary_key = metric_info.get("primary_metric")

        if not primary_key and metrics_dict:
            primary_key = list(metrics_dict.keys())[0]

        primary_metric_data = metrics_dict.get(primary_key, {})

        # 2️⃣ LLM Payload 구성
        summary_payload = {
            "period": f"{start_date} ~ {end_date}",
            "analysis_plan": state.get("analysis_plan", {}),
            "metrics": metrics_dict,
            "row_count": len(df),
            "top_rows_preview": df.head(5).to_dict(orient="records")
        }


        from prompt_builder import GA4PromptBuilder

        prompt = GA4PromptBuilder.build(
            question=question,
            state=state,
            summary=summary_payload,
            event_context=event_context
        )

        try:
            res = openai.ChatCompletion.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2
            )

            raw_output = res["choices"][0]["message"]["content"].strip()

            # JSON 안전 처리
            if "```" in raw_output:
                raw_output = re.sub(r'```json?|```', '', raw_output).strip()

            parsed = json.loads(raw_output)

            structured = {
                "title": parsed.get("title", "분석 결과"),
                "main_metric": (
                    f"{int(primary_metric_data.get('current', 0)):,}"
                    if primary_metric_data.get("current") is not None
                    else None
                ),
                "delta": (
                    f"{'+' if primary_metric_data.get('diff', 0) >= 0 else ''}"
                    f"{int(primary_metric_data.get('diff', 0)):,} "
                    f"({primary_metric_data.get('growth', 0)}%)"
                    if primary_metric_data.get("diff") is not None
                    else None
                ),
                "comparison": {
                    "previous": (
                        f"{int(primary_metric_data.get('previous', 0)):,}"
                        if primary_metric_data.get("previous") is not None
                        else None
                    ),
                    "current": (
                        f"{int(primary_metric_data.get('current', 0)):,}"
                        if primary_metric_data.get("current") is not None
                        else None
                    )
                },
                "insight_narrative": parsed.get("insight_narrative", ""),
                "all_metrics": metrics_dict  # 🔥 multi-metric 전체 포함
            }

            return {
                "structured": structured,
                "fallback": structured["title"]
            }

        except Exception as e:
            logging.error(f"[Insight JSON Error] {e}")

            fallback_struct = {
                "title": "분석 결과",
                "main_metric": None,
                "delta": None,
                "comparison": None,
                "insight_narrative": "자동 해석 생성에 실패하여 기본 요약만 제공합니다.",
                "all_metrics": metrics_dict
            }

            return {
                "structured": fallback_struct,
                "fallback": fallback_struct["title"]
            }

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import GetMetadataRequest


ga4_engine = GA4AnalysisEngine()

class QueryRouter:
    GA4_KEYWORDS = ["사용자", "세션", "페이지뷰", "이벤트", "전환", "실적", "성과", "어제", "지난주", "지난달", "오늘", "ga4", "analytics", "트래픽"]
    FILE_KEYWORDS = ["파일", "문서", "업로드", "내용", "컬럼", "열", "행", "csv", "엑셀", "xlsx"]
    

    @classmethod
    def determine_route(cls, question, conversation_id):
        q = question.lower()
        if any(k in q for k in ["연결", "속성", "계정"]): return "system", True
        
        context = DBManager.load_conversation_context(conversation_id) if conversation_id else None
        has_ga4 = any(k in q for k in cls.GA4_KEYWORDS)
        has_file = any(k in q for k in cls.FILE_KEYWORDS)
        
        if has_ga4 and has_file: return "mixed", False
        if has_file: return "file", False
        if context and context.get("active_source"): return context["active_source"], False
        return "ga4", False

def handle_question(
    question,
    property_id=None,
    file_path=None,
    user_id="anonymous",
    conversation_id=None,
    semantic=None
):
    from flask import session

    if not conversation_id:
        conversation_id = "default_session"

    history = ConversationSummaryMemory.get_chat_history(user_id, conversation_id)

    # 🔥 Clarify Confirm 처리
    pending = session.get("pending_clarify")

    if pending:
        normalized = question.strip().lower()

        is_yes = any(tok in normalized for tok in ["예", "네", "응", "맞"])
        is_no  = any(tok in normalized for tok in ["아니", "아니오", "틀렸"])

        logging.info(f"[Clarify] pending={pending}")
        logging.info(f"[Clarify] raw_question='{question}' normalized='{normalized}'")
        logging.info(f"[Clarify] decision yes={is_yes} no={is_no}")

        if is_yes:
            confirmed_value = pending["value"]
            confirmed_type = pending["type"]

            session.pop("pending_clarify", None)

            if confirmed_type == "metric":
                question = confirmed_value
            elif confirmed_type == "dimension":
                question = f"{confirmed_value}별"

        elif is_no:
            session.pop("pending_clarify", None)
            return {
                "response": {
                    "message": "어떤 항목을 의미하셨는지 조금 더 구체적으로 말씀해 주세요.",
                    "plot_data": []
                },
                "route": "system"
            }

    history.add_user_message(question)

    route, is_explicit = QueryRouter.determine_route(question, conversation_id)

    prev_context = DBManager.load_conversation_context(conversation_id) or {}
    prev_source = prev_context.get("active_source")

    active_property = property_id or prev_context.get("property_id") or session.get("property_id")
    active_file = file_path or prev_context.get("file_path") or session.get("uploaded_file_path")

    if route != "system":
        DBManager.save_conversation_context(
            conversation_id,
            {
                "active_source": route,
                "property_id": active_property,
                "file_path": active_file
            }
        )

    try:

        # 1️⃣ SYSTEM
        if route == "system":
            msg = f"현재 GA4 속성 [{session.get('property_name', '없음')}]에 연결되어 있습니다."
            history.add_ai_message(msg)
            return {"response": {"message": msg, "plot_data": []}, "route": "system"}

        # 2️⃣ FILE
        elif route == "file":
            res = file_engine.process(question, active_file, conversation_id, prev_source)
            history.add_ai_message(str(res.get("message", "")))
            return {"response": res, "route": "file"}

        # 3️⃣ MIXED
        elif route == "mixed":
            mixed = MixedAnalysisEngine(ga4_engine, file_engine)
            res = mixed.process(question, active_property, active_file, conversation_id)
            history.add_ai_message(str(res.get("message", "")))
            return {"response": res, "route": "mixed"}

        # 4️⃣ GA4 기본 루트
        else:
            # ✅ followup detector는 intent와 분리되어야 함
            if is_followup_question(question):
                last_result = DBManager.load_last_result(conversation_id, "ga4")

                if last_result:
                    res = post_process(last_result, question)
                    history.add_ai_message(str(res.get("message", "")))
                    return {"response": res, "route": "ga4_followup"}

                logging.info("[Followup Detector] last_result 없음 → GA4 새 분석 실행")

            # 🔥 Use NEW PIPELINE instead of old engine
            from integration_wrapper import handle_ga4_question
            
            res = handle_ga4_question(
                question=question,
                property_id=active_property,
                conversation_id=conversation_id,
                semantic=semantic,
                user_name=session.get("user_name", "")
            )

            # 신형 파이프라인 응답도 follow-up에서 재사용 가능하도록 저장
            if conversation_id and isinstance(res, dict):
                DBManager.save_last_result(conversation_id, "ga4", res)

            history.add_ai_message(str(res.get("message", "")))
            return {"response": res, "route": "ga4"}

    except Exception as e:
        logging.error(f"[handle_question Error] {e}")
        return {
            "response": {"message": f"오류: {e}", "plot_data": []},
            "route": "error"
        }



   

def process_ga4_visualization(question):
    return {"message": "시각화 생성됨", "plot_data": []}

def is_followup_question(question: str) -> bool:
    q = question.strip().lower()

    # 1) 강한 followup 트리거 (이건 무조건 followup)
    strong_followup = [
        "아까", "방금", "이전", "전 내용", "전꺼", "그거", "이거",
        "다시", "재설명", "정리", "요약", "더 자세히", "왜", "원인"
    ]
    if any(k in q for k in strong_followup):
        return True

    # 2) "top10/랭킹" 같은 건 followup일 수도 있지만,
    #    아래 조건 중 하나라도 만족하면 "새 분석"으로 본다.

    # 새 분석을 의미하는 강한 키워드
    new_analysis_keywords = [
        "지난주", "이번주", "지난달", "이번달", "어제", "오늘",
        "기간", "부터", "까지", "전주 대비", "비교", "증감",
        "추이", "일별", "월별"
    ]

    # GA4 지표/차원 키워드 (이게 있으면 새 분석일 확률이 큼)
    metric_keywords = [
        "매출", "구매", "구입", "결제", "수익", "revenue",
        "사용자", "유저", "방문자", "세션", "전환", "클릭", "이벤트",
        "상품", "아이템", "item", "제품", "브랜드",
        "유입", "채널", "source", "medium", "campaign"
    ]

    # "top", "10개", "1등" 같은 ranking 키워드
    ranking_keywords = ["top", "top10", "top 10", "10등", "10개", "5개", "1등", "순위"]

    # ranking 요청이면서 metric 키워드가 같이 있으면 → 새 분석
    if any(k in q for k in ranking_keywords):
        if any(m in q for m in metric_keywords):
            return False

        # ranking만 있고 metric이 없으면 followup 가능성 높음
        return True

    # 3) metric/dimension 키워드가 있으면 기본적으로 새 분석
    if any(m in q for m in metric_keywords):
        return False

    # 4) 기간/비교 키워드가 있으면 새 분석
    if any(k in q for k in new_analysis_keywords):
        return False

    # 5) 기본은 followup 아님
    return False




def post_process(last_result, question):
    """
    Follow-up 질문 처리 (후처리 레이어)
    last_result: 이전 GA4 응답 전체 JSON
    question: 사용자의 추가 요청
    """

    try:
        structured = last_result.get("structured_insight")
        raw_data = last_result.get("raw_data")

        if not raw_data:
            return {
                "message": "이전 분석 결과가 존재하지 않습니다.",
                "plot_data": []
            }

        q = (question or "").lower()

        # 원인/왜 질문은 우선 규칙기반으로 답변 (데이터 기반 설명 + 실행 제안)
        if any(k in q for k in ["원인", "왜", "이유", "해석"]):
            if isinstance(raw_data, list) and raw_data and isinstance(raw_data[0], dict):
                sample = raw_data[0]
                keys = list(sample.keys())

                def _to_num(v):
                    try:
                        import re
                        t = re.sub(r"[^\d\.\-]", "", str(v))
                        return float(t) if t not in ("", "-", ".", "-.") else None
                    except Exception:
                        return None

                metric_key = None
                label_key = None
                for k in keys:
                    if metric_key is None and _to_num(sample.get(k)) is not None:
                        metric_key = k
                    if label_key is None and _to_num(sample.get(k)) is None:
                        label_key = k

                if metric_key:
                    vals = []
                    labels = []
                    for row in raw_data:
                        if not isinstance(row, dict):
                            continue
                        n = _to_num(row.get(metric_key))
                        if n is None:
                            continue
                        vals.append(n)
                        labels.append(str(row.get(label_key, "")) if label_key else "")

                    if vals:
                        total = sum(vals)
                        top1 = vals[0]
                        top3 = sum(vals[:3])
                        top1_share = (top1 / total * 100) if total else 0
                        top3_share = (top3 / total * 100) if total else 0
                        top_label = labels[0] if labels else "상위 항목"
                        concentration = "집중" if top1_share >= 40 else "분산"
                        msg = (
                            f"원인 분석 관점에서 보면 상위 성과는 **{concentration} 구조**입니다.\n"
                            f"- 1위 항목: **{top_label}**\n"
                            f"- 1위 비중: **{top1_share:.1f}%**\n"
                            f"- 상위 3개 비중: **{top3_share:.1f}%**\n\n"
                            "다음으로는 1위 항목을 채널/디바이스/랜딩페이지로 분해해 원인을 확정하는 것이 좋습니다."
                        )
                        return {
                            "message": msg,
                            "plot_data": last_result.get("plot_data", []),
                            "structured_insight": structured,
                            "raw_data": raw_data
                        }

        # 기본: LLM 재요약
        prompt = f"""
        아래는 이전 분석 결과입니다:

        {json.dumps(last_result, ensure_ascii=False, indent=2)}

        사용자의 추가 요청:
        {question}

        기존 데이터를 재활용해서 간결하게 답변하세요.
        """

        res = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2
        )

        message = res["choices"][0]["message"]["content"]

        return {
            "message": message,
            "plot_data": last_result.get("plot_data", []),
            "structured_insight": structured,
            "raw_data": raw_data
        }

    except Exception as e:
        logging.error(f"[post_process Error] {e}")
        return {
            "message": "후속 분석 중 오류가 발생했습니다.",
            "plot_data": []
        }
