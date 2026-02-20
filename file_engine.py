import logging
import pandas as pd
import os
import openai
import json
import re
import numpy as np
import warnings
from db_manager import DBManager

class FileAnalysisEngine:
    def __init__(self):
        pass

    def process(self, question, file_path, conversation_id=None, prev_source=None, beginner_mode=False):
        logging.info(f"📁 [File Engine] Analyzing: {file_path}")
        if not file_path or not os.path.exists(file_path):
             return {"message": "파일을 찾을 수 없습니다.", "status": "error"}
             
        try:
            # 1. Load Data
            df = pd.read_csv(file_path) if file_path.endswith('.csv') else pd.read_excel(file_path)
            file_name = os.path.basename(file_path)
            dataset_period = self._infer_dataset_period(df)
            
            # 2. Context / State Management
            state = {}
            if conversation_id:
                # [Phase 5] Restore last intent if follow-up
                state = DBManager.load_last_state(conversation_id, "file") or {}
                
            # 3. Analyze Query (Level 1-3 Strategy)
            result_df, message, intent, analysis_meta = self._analyze_query(df, question, state)
            if beginner_mode:
                message = self._make_beginner_message(message, intent, analysis_meta)
            
            # 4. Transform for Visualization
            plot_data = self._transform_to_plot_data(result_df, intent)
            followups = self._build_followups(df, intent, analysis_meta)
            raw_limit = self._raw_limit_by_intent(intent, analysis_meta)
            intent_plan = {
                "source": "file",
                "intent_type": intent.get("type"),
                "group_col": analysis_meta.get("group_col"),
                "metric_col": analysis_meta.get("metric_col"),
                "op": analysis_meta.get("op"),
                "date_col": analysis_meta.get("date_col"),
                "period": analysis_meta.get("period"),
                "row_count": int(len(df)),
                "column_count": int(len(df.columns))
            }
            
            # 5. Save State
            if result_df is not None and conversation_id:
                # [Phase 5] Save Intent for Follow-up
                state["last_intent"] = intent
                state["last_analysis_meta"] = analysis_meta or {}
                DBManager.save_success_state(conversation_id, "file", state)

            period_text = analysis_meta.get("period") or dataset_period
            return {
                "message": message,
                "status": "ok",
                "plot_data": plot_data,
                "raw_data": result_df.head(raw_limit).where(pd.notnull(result_df), None).to_dict(orient='records') if result_df is not None else [],
                "followup_suggestions": followups,
                "period": period_text,
                "file_name": file_name,
                "data_label": f"FILE · {file_name}",
                "collection_period": dataset_period,
                "intent_plan": intent_plan
            }
            
        except Exception as e:
            logging.error(f"[File Engine Error] {e}")
            return {"message": f"파일 분석 오류: {e}", "status": "error"}

    def _analyze_query(self, df, question, state):
        # [Phase 5] 3-Level Processing Strategy
        intent = self._detect_intent(question, state)
        col_count = self._detect_column_count(df, question)
        if col_count:
            intent["type"] = "column_count"
            intent["target_column"] = col_count
        q = str(question or "").lower()
        page_req = self._detect_page_request(question)
        if not page_req and isinstance(state, dict):
            last_meta = state.get("last_analysis_meta", {}) or {}
            if last_meta.get("show_unique") and last_meta.get("target_column"):
                if any(k in q for k in ["다음", "계속", "이어", "more", "next"]):
                    page_req = {"direction": "next", "size": int(last_meta.get("page_limit", 500) or 500)}
                elif any(k in q for k in ["이전", "앞", "prev", "previous"]):
                    page_req = {"direction": "prev", "size": int(last_meta.get("page_limit", 500) or 500)}
        if page_req and isinstance(state, dict):
            last_meta = state.get("last_analysis_meta", {}) or {}
            if last_meta.get("show_unique") and last_meta.get("target_column"):
                last_offset = int(last_meta.get("page_offset", 0))
                limit = int(last_meta.get("page_limit", 500))
                step = int(page_req.get("size") or limit)
                if page_req.get("direction") == "next":
                    new_offset = max(0, last_offset + step)
                else:
                    new_offset = max(0, last_offset - step)
                intent["type"] = "column_probe"
                intent["target_column"] = last_meta.get("target_column")
                intent["preview_count"] = step
                intent["show_unique"] = True
                intent["offset"] = new_offset
        elif isinstance(state, dict):
            last_meta = state.get("last_analysis_meta", {}) or {}
            if last_meta.get("show_unique") and last_meta.get("target_column"):
                if any(k in q for k in ["전체", "목록", "모두", "전체 보여", "다 보여"]):
                    intent["type"] = "column_probe"
                    intent["target_column"] = last_meta.get("target_column")
                    intent["preview_count"] = 500
                    intent["show_unique"] = True
                    intent["offset"] = 0

        if self._is_preview_more_request(question):
            last_intent = (state.get("last_intent") or {}).get("type") if isinstance(state, dict) else None
            last_meta = state.get("last_analysis_meta", {}) if isinstance(state, dict) else {}
            if last_intent in {"column_probe", "schema", "columns_summary", "overview", "preview"}:
                intent["type"] = "preview_more"
                intent["target_column"] = last_meta.get("target_column")
                intent["preview_count"] = 10

        col_probe = self._detect_column_probe(df, question)
        if col_probe:
            intent["type"] = "column_probe"
            intent["target_column"] = col_probe.get("target_column")
            intent["preview_count"] = int(col_probe.get("preview_count", 5))
            intent["show_unique"] = bool(col_probe.get("show_unique", False))
        analysis_meta = {}
        
        # 1. Level 1 & 2: Skip LLM (Exploration & Aggregation)
        if intent["type"] in ["schema", "preview", "overview", "groupby", "aggregate", "distribution", "filter", "count_users", "count_admin", "columns_summary", "explain", "trend", "compare", "guidance", "column_probe", "column_count", "preview_more"]:
            result_df, analysis_meta = self._execute_aggregation(df, intent, state)
            
            if intent["type"] == "schema":
                row_count = int(analysis_meta.get("row_count", len(df)))
                col_count = int(analysis_meta.get("col_count", len(df.columns)))
                numeric_count = int(analysis_meta.get("numeric_count", 0))
                cat_count = int(analysis_meta.get("categorical_count", 0))
                date_count = int(analysis_meta.get("date_count", 0))
                bool_count = int(analysis_meta.get("boolean_count", 0))
                id_count = int(analysis_meta.get("identifier_count", 0))
                sample_cols = analysis_meta.get("sample_columns", [])
                numeric_cols = analysis_meta.get("numeric_columns", [])
                categorical_cols = analysis_meta.get("categorical_columns", [])
                date_cols = analysis_meta.get("date_columns", [])
                bool_cols = analysis_meta.get("boolean_columns", [])
                id_cols = analysis_meta.get("identifier_columns", [])
                sample_cols_txt = ", ".join(sample_cols[:6]) if sample_cols else ""
                numeric_cols_txt = ", ".join([str(c) for c in numeric_cols[:3]]) if numeric_cols else "-"
                cat_cols_txt = ", ".join([str(c) for c in categorical_cols[:3]]) if categorical_cols else "-"
                date_cols_txt = ", ".join([str(c) for c in date_cols[:3]]) if date_cols else "-"
                bool_cols_txt = ", ".join([str(c) for c in bool_cols[:3]]) if bool_cols else "-"
                id_cols_txt = ", ".join([str(c) for c in id_cols[:3]]) if id_cols else "-"
                msg = (
                    f"파일 구조를 간단히 정리하면 **{row_count:,}행 / {col_count}컬럼**입니다. "
                    f"수치형 {numeric_count}개(계산용), 범주형 {cat_count}개(분류용), 날짜형 {date_count}개, 불리언 {bool_count}개, 식별자 {id_count}개입니다.\n"
                    f"- 수치형 예: `{numeric_cols_txt}`\n"
                    f"- 범주형 예: `{cat_cols_txt}`\n"
                    f"- 날짜형 예: `{date_cols_txt}`\n"
                    f"- 불리언 예: `{bool_cols_txt}`\n"
                    f"- 식별자 예: `{id_cols_txt}`\n"
                    f"- 대표 컬럼: `{sample_cols_txt}`"
                )
                msg += self._build_preview_tail(analysis_meta)
            elif intent["type"] == "preview":
                msg = f"파일의 상위 {len(result_df)}행 미리보기입니다."
            elif intent["type"] == "overview":
                msg = f"파일 전체 개요: 총 {len(df)}행, {len(df.columns)}개 컬럼"
                msg += self._build_preview_tail(analysis_meta)
            elif intent["type"] == "guidance":
                msg = analysis_meta.get("guide_text") or "파일 분석을 시작할 수 있는 추천 질문을 드릴게요."
            elif intent["type"] == "count_users":
                user_count = int(analysis_meta.get("user_count", len(df)))
                id_col = analysis_meta.get("id_column")
                if id_col:
                    msg = f"이 파일 기준 사용자 수는 **{user_count}명**입니다. (`{id_col}` 기준)"
                else:
                    msg = f"이 파일 기준 사용자 수는 **{user_count}명**입니다."
            elif intent["type"] == "count_admin":
                admin_count = int(analysis_meta.get("admin_count", 0))
                total_count = int(analysis_meta.get("total_count", len(df)))
                admin_cols = analysis_meta.get("admin_columns", [])
                ratio = (admin_count / total_count * 100) if total_count else 0.0
                col_text = f" ({', '.join(admin_cols)})" if admin_cols else ""
                msg = f"관리자(어드민) 수는 **{admin_count}명**입니다{col_text}. 전체 대비 **{ratio:.1f}%**입니다."
            elif intent["type"] == "columns_summary":
                numeric_count = int(analysis_meta.get("numeric_count", 0))
                categorical_count = int(analysis_meta.get("categorical_count", 0))
                date_count = int(analysis_meta.get("date_count", 0))
                bool_count = int(analysis_meta.get("boolean_count", 0))
                id_count = int(analysis_meta.get("identifier_count", 0))
                numeric_cols = analysis_meta.get("numeric_columns", [])
                categorical_cols = analysis_meta.get("categorical_columns", [])
                date_cols = analysis_meta.get("date_columns", [])
                bool_cols = analysis_meta.get("boolean_columns", [])
                id_cols = analysis_meta.get("identifier_columns", [])
                numeric_cols_txt = ", ".join([str(c) for c in numeric_cols[:3]]) if numeric_cols else "-"
                cat_cols_txt = ", ".join([str(c) for c in categorical_cols[:3]]) if categorical_cols else "-"
                date_cols_txt = ", ".join([str(c) for c in date_cols[:3]]) if date_cols else "-"
                bool_cols_txt = ", ".join([str(c) for c in bool_cols[:3]]) if bool_cols else "-"
                id_cols_txt = ", ".join([str(c) for c in id_cols[:3]]) if id_cols else "-"
                msg = (
                    f"이 파일에는 총 **{len(df.columns)}개 컬럼**이 있습니다. "
                    f"수치형 **{numeric_count}개**(합계/평균 계산용), "
                    f"범주형 **{categorical_count}개**(~별 비교용), "
                    f"날짜형 **{date_count}개**, 불리언 **{bool_count}개**, 식별자 **{id_count}개**입니다.\n"
                    f"- 수치형 예: `{numeric_cols_txt}`\n"
                    f"- 범주형 예: `{cat_cols_txt}`\n"
                    f"- 날짜형 예: `{date_cols_txt}`\n"
                    f"- 불리언 예: `{bool_cols_txt}`\n"
                    f"- 식별자 예: `{id_cols_txt}`"
                )
                msg += self._build_preview_tail(analysis_meta)
            elif intent["type"] == "column_probe":
                tc = intent.get("target_column")
                pc = int(intent.get("preview_count", 5))
                if analysis_meta.get("show_unique"):
                    total_unique = int(analysis_meta.get("total_unique", len(result_df)))
                    shown = int(analysis_meta.get("shown_unique", len(result_df)))
                    start_idx = int(analysis_meta.get("page_offset", 0)) + 1
                    end_idx = min(int(analysis_meta.get("page_offset", 0)) + shown, total_unique)
                    msg = f"`{tc}` 컬럼의 고유값 목록입니다. (총 {total_unique}개, 현재 {start_idx}~{end_idx})"
                else:
                    msg = f"`{tc}` 컬럼의 상위 {pc}행 값 미리보기입니다."
            elif intent["type"] == "column_count":
                tc = intent.get("target_column")
                c = int(analysis_meta.get("unique_count", 0))
                msg = f"`{tc}` 기준 고유 개수는 **{c:,}개**입니다."
            elif intent["type"] == "preview_more":
                tc = intent.get("target_column")
                if tc and tc in df.columns:
                    msg = f"`{tc}` 컬럼의 추가 미리보기(상위 10행)입니다."
                else:
                    msg = f"파일의 추가 미리보기(상위 {len(result_df)}행)입니다."
            elif intent["type"] == "explain":
                msg = analysis_meta.get("explain_text") or "직전 분석 결과를 기준으로 의미를 설명드렸습니다."
            elif intent["type"] == "trend":
                date_col = analysis_meta.get("date_col")
                metric_col = analysis_meta.get("metric_col")
                msg = f"일자 추이 결과입니다. ({date_col} 기준, 지표: {metric_col})"
            elif intent["type"] == "compare":
                group_col = analysis_meta.get("group_col")
                metric_col = analysis_meta.get("metric_col")
                msg = f"비교 결과입니다. ({group_col} 기준, 지표: {metric_col})"
            elif intent["type"] in ["groupby", "distribution"]:
                group_col = analysis_meta.get("group_col")
                metric_col = analysis_meta.get("metric_col")
                op = analysis_meta.get("op")
                if group_col:
                    msg = f"요청하신 `{group_col}` 기준 집계 결과입니다. ({op}: {metric_col})"
                else:
                    msg = f"요청하신 '{intent.get('keywords', ['그룹'])[0]}' 기준 집계 결과입니다."
            elif intent["type"] == "aggregate":
                if len(result_df) == 1 and result_df.shape[1] == 1:
                     val = result_df.iloc[0, 0]
                     msg = f"계산 결과: {val}"
                else:
                     msg = "요청하신 집계 결과입니다."
            else:
                msg = "분석 결과입니다."
            
            return result_df, msg, intent, analysis_meta 

        # 2. Level 3: Insight (LLM Required)
        elif intent["type"] == "insight":
            logging.info("[FileEngine] Intent is Insight. Calling LLM.")
            # Insight logic requires schema and data summary
            result_df = df # Default to full df for context
            insight = self._generate_insight(df, result_df, question, state)
            return result_df, insight, intent, analysis_meta
            
        # Default Fallback
        logging.info("[FileEngine] Intent Unclear or Default. Calling LLM for safety.")
        insight = self._generate_insight(df, df, question, state)
        return df, insight, intent, analysis_meta

    def _detect_intent(self, question, state):
        q_lower = question.lower()
        intent = {"type": "insight", "keywords": [], "raw_question": question}
        
        # [Phase 5] 3-Level Intent Detection
        
        # Level 1: Exploration (탐색)
        if any(kw in q_lower for kw in ['뭘 물어', '어떻게 질문', '뭐부터', '초보', '어렵', '잘 모르']):
            intent["type"] = "guidance"
        elif any(kw in q_lower for kw in ['추이', '트렌드', '일별', '월별', '변화']):
            intent["type"] = "trend"
        elif any(kw in q_lower for kw in ['비교', '대비', 'vs', '차이']):
            intent["type"] = "compare"
        elif any(kw in q_lower for kw in ['구조', '컬럼', '열', 'schema', 'structure']):
             intent["type"] = "schema"
        elif any(kw in q_lower for kw in ['어떤 데이터', '무슨 데이터', '또 어떤', '컬럼 뭐', '항목 뭐', '뭐가 들어', '무엇이 들어', '어떤게 있어']):
             intent["type"] = "columns_summary"
        elif any(kw in q_lower for kw in ['행', '샘플', '예시', 'preview', 'sample', '보여줘', 'raw data']):
            intent["type"] = "preview"
        elif any(kw in q_lower for kw in ['개요', '요약', 'overview', 'summary', '전체']):
            intent["type"] = "overview"
             
        # Level 2: Aggregation (집계)
        elif any(kw in q_lower for kw in ['별', '타입별', '종류별', '카테고리별', 'by ', '그룹']):
            intent["type"] = "groupby"
            intent["keywords"] = ["별"]
        elif any(kw in q_lower for kw in ['평균', 'average', 'avg', 'mean']):
            intent["type"] = "aggregate"
            intent["keywords"] = ["평균"]
        elif any(kw in q_lower for kw in ['합계', '총', 'sum', 'total']):
            intent["type"] = "aggregate"
            intent["keywords"] = ["합계"]
        elif any(kw in q_lower for kw in ['개수', 'count', '몇 개', '몇개']):
            intent["type"] = "aggregate"
            intent["keywords"] = ["개수"]

        # 파일 내 사용자/관리자 집계
        elif any(kw in q_lower for kw in ['사용자', '유저', '회원', '인원', '사람']) and any(kw in q_lower for kw in ['얼마나', '몇', '수', '명', '몇명', '몇 명']):
            intent["type"] = "count_users"
        elif any(kw in q_lower for kw in ['어드민', '관리자', 'admin']) and any(kw in q_lower for kw in ['얼마나', '몇', '수']):
            intent["type"] = "count_admin"
        elif any(kw in q_lower for kw in ['무슨 뜻', '뜻이', '의미', '그게 무슨']):
            intent["type"] = "explain"
            
        # Follow-up detection (Level 3 or Level 2 context)
        elif any(kw in q_lower for kw in ['응', '그래', '보여줘', '설명해줘']):
            if state.get("last_intent"):
                intent = state["last_intent"]
                intent["is_followup"] = True
            else:
                intent["type"] = "insight"
                
        return intent

    def _execute_aggregation(self, df, intent, state=None):
        state = state or {}
        meta = {}
        # Simplified aggregation for restoration
        if intent["type"] == "schema":
            profile = self._profile_columns(df)
            numeric_cols = [c for c, k in profile.items() if k == "numeric"]
            categorical_cols = [c for c, k in profile.items() if k == "categorical"]
            date_cols = [c for c, k in profile.items() if k == "date"]
            bool_cols = [c for c, k in profile.items() if k == "boolean"]
            id_cols = [c for c, k in profile.items() if k == "identifier"]
            meta["row_count"] = int(len(df))
            meta["col_count"] = int(len(df.columns))
            meta["numeric_count"] = int(len(numeric_cols))
            meta["categorical_count"] = int(len(categorical_cols))
            meta["date_count"] = int(len(date_cols))
            meta["boolean_count"] = int(len(bool_cols))
            meta["identifier_count"] = int(len(id_cols))
            meta["sample_columns"] = [str(c) for c in df.columns[:8]]
            meta["numeric_columns"] = [str(c) for c in numeric_cols]
            meta["categorical_columns"] = [str(c) for c in categorical_cols]
            meta["date_columns"] = [str(c) for c in date_cols]
            meta["boolean_columns"] = [str(c) for c in bool_cols]
            meta["identifier_columns"] = [str(c) for c in id_cols]
            meta["preview_rows"] = self._preview_rows(df, n=5)
            schema_df = pd.DataFrame({
                "column": df.columns,
                "dtype": [str(df[c].dtype) for c in df.columns],
                "null_count": [int(df[c].isna().sum()) for c in df.columns],
                "sample_value": [str(df[c].dropna().iloc[0]) if not df[c].dropna().empty else "" for c in df.columns],
            })
            return schema_df, meta
        elif intent["type"] == "preview":
            return df.head(10), meta
        elif intent["type"] == "overview":
            profile = self._profile_columns(df)
            numeric_cols = [c for c, k in profile.items() if k == "numeric"]
            categorical_cols = [c for c, k in profile.items() if k == "categorical"]
            date_cols = [c for c, k in profile.items() if k == "date"]
            bool_cols = [c for c, k in profile.items() if k == "boolean"]
            id_cols = [c for c, k in profile.items() if k == "identifier"]
            overview = pd.DataFrame({
                "metric": ["row_count", "column_count", "numeric_columns", "categorical_columns", "date_columns", "boolean_columns", "identifier_columns", "missing_cells"],
                "value": [
                    int(len(df)),
                    int(len(df.columns)),
                    int(len(numeric_cols)),
                    int(len(categorical_cols)),
                    int(len(date_cols)),
                    int(len(bool_cols)),
                    int(len(id_cols)),
                    int(df.isna().sum().sum()),
                ]
            })
            meta["preview_rows"] = self._preview_rows(df, n=5)
            return overview, meta
        elif intent["type"] == "columns_summary":
            profile = self._profile_columns(df)
            numeric_cols = [c for c, k in profile.items() if k == "numeric"]
            categorical_cols = [c for c, k in profile.items() if k == "categorical"]
            date_cols = [c for c, k in profile.items() if k == "date"]
            bool_cols = [c for c, k in profile.items() if k == "boolean"]
            id_cols = [c for c, k in profile.items() if k == "identifier"]
            meta["numeric_count"] = len(numeric_cols)
            meta["categorical_count"] = len(categorical_cols)
            meta["date_count"] = len(date_cols)
            meta["boolean_count"] = len(bool_cols)
            meta["identifier_count"] = len(id_cols)
            meta["numeric_columns"] = [str(c) for c in numeric_cols]
            meta["categorical_columns"] = [str(c) for c in categorical_cols]
            meta["date_columns"] = [str(c) for c in date_cols]
            meta["boolean_columns"] = [str(c) for c in bool_cols]
            meta["identifier_columns"] = [str(c) for c in id_cols]
            meta["preview_rows"] = self._preview_rows(df, n=5)
            summary_df = pd.DataFrame({
                "column": df.columns,
                "dtype": [str(df[c].dtype) for c in df.columns],
                "null_count": [int(df[c].isna().sum()) for c in df.columns],
            })
            return summary_df, meta
        elif intent["type"] == "count_users":
            id_col = self._find_user_id_column(df)
            if id_col:
                user_count = int(df[id_col].nunique(dropna=True))
            else:
                user_count = int(len(df))
            meta["user_count"] = user_count
            meta["id_column"] = id_col
            return pd.DataFrame({"user_count": [user_count]}), meta
        elif intent["type"] == "count_admin":
            admin_cols = self._find_admin_columns(df)
            admin_count = 0
            if admin_cols:
                admin_count = int(max(self._count_truthy(df[c]) for c in admin_cols))
            total_count = int(len(df))
            meta["admin_count"] = admin_count
            meta["total_count"] = total_count
            meta["admin_columns"] = admin_cols
            return pd.DataFrame({"admin_count": [admin_count], "total_count": [total_count]}), meta
        elif intent["type"] == "column_count":
            target_col = intent.get("target_column")
            if target_col and target_col in df.columns:
                c = int(df[target_col].dropna().astype(str).replace("", pd.NA).dropna().nunique())
                meta["target_column"] = target_col
                meta["unique_count"] = c
                return pd.DataFrame({"column": [target_col], "unique_count": [c]}), meta
            return pd.DataFrame({"column": [], "unique_count": []}), meta
        elif intent["type"] == "explain":
            last_meta = state.get("last_analysis_meta", {}) if isinstance(state, dict) else {}
            last_intent = (state.get("last_intent") or {}).get("type") if isinstance(state, dict) else None
            explain_text = self._build_explain_text(df, last_intent, last_meta)
            meta["explain_text"] = explain_text
            return pd.DataFrame({"explanation": [explain_text]}), meta
        elif intent["type"] == "guidance":
            guide_text = (
                "처음이라면 이렇게 물어보면 됩니다:\n"
                "1. 파일 구조 알려줘\n"
                "2. 핵심 지표 3개 요약해줘\n"
                "3. 채널별/유형별 매출 비교해줘\n"
                "4. 일별 추이와 전주 대비 보여줘\n"
                "5. 이상치나 급변 구간 찾아줘"
            )
            meta["guide_text"] = guide_text
            return pd.DataFrame({
                "recommended_question": [
                    "파일 구조 알려줘",
                    "핵심 지표 3개 요약해줘",
                    "채널별 매출 비교해줘",
                    "일별 매출 추이 보여줘",
                    "이상치 찾아줘"
                ]
            }), meta
        elif intent["type"] == "column_probe":
            target_col = intent.get("target_column")
            n = int(intent.get("preview_count", 5))
            show_unique = bool(intent.get("show_unique", False))
            if show_unique:
                vals = df[target_col].dropna().astype(str).drop_duplicates()
                total_unique = int(len(vals))
                offset = int(intent.get("offset", 0))
                limit = max(1, min(n, 500))
                vals = vals.iloc[offset: offset + limit]
                out = pd.DataFrame({"value": vals.tolist()})
                out.insert(0, "rank", list(range(1, len(out) + 1)))
                meta["target_column"] = target_col
                meta["show_unique"] = True
                meta["total_unique"] = total_unique
                meta["shown_unique"] = int(len(out))
                meta["page_offset"] = offset
                meta["page_limit"] = limit
                meta["has_next"] = (offset + len(out)) < total_unique
                meta["has_prev"] = offset > 0
                return out, meta
            out = df[[target_col]].head(max(1, min(n, 50))).copy()
            out.insert(0, "row_no", list(range(1, len(out) + 1)))
            meta["target_column"] = target_col
            meta["show_unique"] = False
            return out, meta
        elif intent["type"] == "preview_more":
            target_col = intent.get("target_column")
            n = int(intent.get("preview_count", 10))
            if target_col and target_col in df.columns:
                out = df[[target_col]].head(max(1, min(n, 50))).copy()
            else:
                out = df.head(max(1, min(n, 50))).copy()
            out.insert(0, "row_no", list(range(1, len(out) + 1)))
            meta["target_column"] = target_col
            return out, meta
        elif intent["type"] in {"groupby", "distribution", "compare"}:
            group_col = self._guess_group_column(df, intent.get("raw_question", ""))
            metric_col = self._guess_metric_column(df, intent.get("raw_question", ""))
            op = self._guess_op(intent.get("raw_question", ""))
            drop_missing = self._question_wants_drop_missing(intent.get("raw_question", ""))
            if not group_col:
                return df.head(10), meta
            grouped = self._group_aggregate(df, group_col, metric_col, op, drop_missing=drop_missing)
            meta["group_col"] = group_col
            meta["metric_col"] = metric_col or "row_count"
            meta["op"] = op
            meta["drop_missing"] = bool(drop_missing)
            return grouped, meta
        elif intent["type"] == "aggregate":
            metric_col = self._guess_metric_column(df, intent.get("raw_question", ""))
            op = self._guess_op(intent.get("raw_question", ""))
            agg_df = self._aggregate_single(df, metric_col, op)
            meta["metric_col"] = metric_col or "row_count"
            meta["op"] = op
            return agg_df, meta
        elif intent["type"] == "trend":
            date_col = self._guess_date_column(df, intent.get("raw_question", ""))
            metric_col = self._guess_metric_column(df, intent.get("raw_question", ""))
            op = self._guess_op(intent.get("raw_question", ""))
            if not date_col:
                return df.head(10), meta
            trend_df = self._trend_aggregate(df, date_col, metric_col, op)
            meta["date_col"] = date_col
            meta["metric_col"] = metric_col or "row_count"
            meta["op"] = op
            if not trend_df.empty and "date_key" in trend_df.columns:
                meta["period"] = f"{trend_df['date_key'].iloc[0]} ~ {trend_df['date_key'].iloc[-1]}"
            return trend_df, meta

        return df.head(10), meta

    def _transform_to_plot_data(self, df, intent):
        if df is None or df.empty or intent["type"] in ["schema", "preview"]:
            return {"type": None, "labels": [], "series": []}
        if not isinstance(df, pd.DataFrame) or df.shape[1] < 2:
            return {"type": None, "labels": [], "series": []}
        cols = list(df.columns)
        label_col = cols[0]
        numeric_cols = [c for c in cols[1:] if pd.api.types.is_numeric_dtype(df[c])]
        if not numeric_cols:
            return {"type": None, "labels": [], "series": []}
        series_col = numeric_cols[0]
        chart_type = "line" if intent.get("type") == "trend" else "bar"
        return {
            "type": chart_type,
            "labels": [str(v) for v in df[label_col].tolist()],
            "series": [{"name": str(series_col), "data": [float(v) if pd.notna(v) else 0 for v in df[series_col].tolist()]}]
        }

    def _generate_insight(self, df, result_df, question, state=None):
        # 가능한 경우 먼저 결정론적 요약 사용 (숫자 환각 방지)
        deterministic = self._deterministic_summary(df, question)
        if deterministic:
            return deterministic

        schema = df.columns.tolist()
        summary = df.describe().to_json()
        
        prompt = f"""
        Analyze this file data.
        Question: {question}
        Schema: {schema}
        Summary: {summary}
        
        Provide a concise Korean insight based on the data.
        """
        try:
            res = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}]
            )
            return res['choices'][0]['message']['content'].strip()
        except:
            return "파일 분석 결과를 확인해주세요."

    def _find_user_id_column(self, df):
        candidates = []
        for c in df.columns:
            cl = str(c).lower()
            if any(k in cl for k in ["user_id", "userid", "member_id", "moc_idx", "uid", "id"]):
                candidates.append(c)
        return candidates[0] if candidates else None

    def _find_admin_columns(self, df):
        cols = []
        for c in df.columns:
            cl = str(c).lower()
            if "admin" in cl or "관리자" in cl:
                cols.append(c)
        return cols

    def _count_truthy(self, series):
        def _truthy(v):
            if pd.isna(v):
                return False
            if isinstance(v, (int, float)):
                return float(v) > 0
            s = str(v).strip().lower()
            return s in {"1", "true", "y", "yes", "t"}
        return int(series.apply(_truthy).sum())

    def _build_explain_text(self, df, last_intent_type, last_meta):
        if last_intent_type == "count_admin":
            admin_count = int(last_meta.get("admin_count", 0))
            total_count = int(last_meta.get("total_count", len(df)))
            ratio = (admin_count / total_count * 100) if total_count else 0.0
            return f"즉, 전체 {total_count}명 중 관리자 권한 사용자는 {admin_count}명({ratio:.1f}%)이라는 의미입니다."
        if last_intent_type == "count_users":
            user_count = int(last_meta.get("user_count", len(df)))
            return f"즉, 파일에서 집계 가능한 사용자 수가 {user_count}명이라는 의미입니다."
        if last_intent_type in {"schema", "columns_summary", "overview"}:
            numeric_count = int(last_meta.get("numeric_count", 0))
            categorical_count = int(last_meta.get("categorical_count", 0))
            return (
                f"즉, 수치형({numeric_count}개)은 합계/평균/추이에 쓰고, "
                f"범주형({categorical_count}개)은 채널별/유형별처럼 그룹 비교에 쓰면 됩니다."
            )
        return "즉, 직전 응답은 파일의 현재 데이터 분포와 집계 결과를 요약한 것입니다."

    def _tokenize(self, text):
        return [t.lower() for t in re.findall(r"[a-zA-Z0-9가-힣_]+", str(text or "")) if len(t) >= 2]

    def _guess_group_column(self, df, question):
        q = str(question or "").lower()
        profile = self._profile_columns(df)
        candidates = []
        for c in df.columns:
            cl = str(c).lower()
            kind = profile.get(c, "categorical")
            if kind == "numeric":
                continue
            score = 0
            if cl in q:
                score += 3
            if "유형" in q and any(k in cl for k in ["type", "유형", "category", "카테고리"]):
                score += 3
            if "채널" in q and ("channel" in cl or "채널" in cl):
                score += 3
            if "국가" in q and ("country" in cl or "국가" in cl):
                score += 3
            if "후원" in q and ("donation" in cl or "후원" in cl):
                score += 2
            nunique = int(df[c].nunique(dropna=True))
            if 2 <= nunique <= min(200, len(df)):
                score += 1
            if score > 0:
                candidates.append((score, c))
        if not candidates:
            non_numeric = [c for c in df.columns if not pd.api.types.is_numeric_dtype(df[c])]
            return non_numeric[0] if non_numeric else None
        candidates.sort(key=lambda x: (-x[0], str(x[1])))
        return candidates[0][1]

    def _guess_metric_column(self, df, question):
        q = str(question or "").lower()
        profile = self._profile_columns(df)
        numeric = [c for c in df.columns if profile.get(c) == "numeric"]
        if not numeric:
            return None
        scored = []
        for c in numeric:
            cl = str(c).lower()
            score = 0
            if cl in q:
                score += 4
            if any(k in q for k in ["매출", "수익", "금액", "revenue", "sales"]) and any(k in cl for k in ["revenue", "amount", "sales", "매출", "수익", "금액", "price"]):
                score += 4
            if any(k in q for k in ["사용자", "유저", "후원자"]) and any(k in cl for k in ["user", "사용자", "유저", "member", "buyer", "purchaser"]):
                score += 3
            if any(k in q for k in ["클릭", "이벤트", "횟수", "count"]) and any(k in cl for k in ["count", "event", "click", "횟수", "수"]):
                score += 3
            scored.append((score, c))
        scored.sort(key=lambda x: (-x[0], str(x[1])))
        return scored[0][1] if scored else numeric[0]

    def _guess_date_column(self, df, question):
        profile = self._profile_columns(df)
        for c in df.columns:
            if profile.get(c) == "date":
                return c
        q = str(question or "").lower()
        for c in df.columns:
            cl = str(c).lower()
            if any(k in cl for k in ["date", "day", "일자", "날짜", "yearmonth", "month"]):
                return c
        # fallback: parseable object column
        for c in df.columns:
            if pd.api.types.is_object_dtype(df[c]):
                sample = df[c].dropna().astype(str).head(20)
                if sample.empty:
                    continue
                parsed = pd.to_datetime(sample, errors="coerce")
                if parsed.notna().sum() >= max(3, int(len(sample) * 0.5)):
                    return c
        return None

    def _guess_op(self, question):
        q = str(question or "").lower()
        if any(k in q for k in ["평균", "avg", "average", "mean"]):
            return "mean"
        if any(k in q for k in ["최대", "max", "가장 큰", "highest"]):
            return "max"
        if any(k in q for k in ["최소", "min", "가장 작은", "lowest"]):
            return "min"
        if any(k in q for k in ["개수", "count", "몇", "얼마나"]):
            return "count"
        return "sum"

    def _to_numeric_series(self, s):
        if pd.api.types.is_numeric_dtype(s):
            return s
        return pd.to_numeric(s.astype(str).str.replace(r"[^\d\.\-]", "", regex=True), errors="coerce")

    def _group_aggregate(self, df, group_col, metric_col, op, drop_missing=False):
        work = df.copy()
        if drop_missing and group_col in work.columns:
            s = work[group_col].astype(str).str.strip()
            mask = work[group_col].notna() & ~s.str.lower().isin({"", "(not set)", "not set", "none", "null", "nan"})
            work = work[mask]
        if metric_col:
            work[metric_col] = self._to_numeric_series(work[metric_col])
        g = work.groupby(group_col, dropna=False)
        if op == "count" or not metric_col:
            out = g.size().reset_index(name="count")
        elif op == "mean":
            out = g[metric_col].mean().reset_index(name=f"{metric_col}_mean")
        elif op == "max":
            out = g[metric_col].max().reset_index(name=f"{metric_col}_max")
        elif op == "min":
            out = g[metric_col].min().reset_index(name=f"{metric_col}_min")
        else:
            out = g[metric_col].sum().reset_index(name=f"{metric_col}_sum")
        val_col = out.columns[-1]
        out = out.sort_values(val_col, ascending=False).reset_index(drop=True)
        return out.head(200)

    def _profile_columns(self, df):
        profile = {}
        for c in df.columns:
            profile[c] = self._infer_col_kind(df, c)
        return profile

    def _infer_col_kind(self, df, col):
        s = df[col]
        cl = str(col).lower()
        non_null = s.dropna()
        if non_null.empty:
            return "categorical"

        if pd.api.types.is_bool_dtype(s):
            return "boolean"

        sample = non_null.astype(str).str.strip().head(2000)
        lowered = sample.str.lower()
        bool_vals = {"y", "n", "yes", "no", "true", "false", "0", "1", "t", "f"}
        if len(sample) >= 5 and lowered.isin(bool_vals).mean() >= 0.95:
            return "boolean"

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            parsed_dt = pd.to_datetime(sample, errors="coerce")
        if parsed_dt.notna().mean() >= 0.9:
            return "date"

        numeric_s = pd.to_numeric(sample.str.replace(r"[^\d\.\-]", "", regex=True), errors="coerce")
        numeric_ratio = numeric_s.notna().mean()
        if numeric_ratio >= 0.95:
            # identifier heuristic: column name + uniqueness + integer-like
            uniq_ratio = float(sample.nunique(dropna=True)) / max(1, len(sample))
            uniq_count = int(sample.nunique(dropna=True))
            integer_like = ((numeric_s.dropna() % 1) == 0).mean() >= 0.98 if numeric_s.dropna().shape[0] else False
            id_name = any(k in cl for k in ["id", "_id", "idx", "코드", "번호", "no", "seq", "key"])
            code_name = any(k in cl for k in ["route", "type", "category", "status", "grade", "level", "group", "구분", "유형", "등급", "상태", "경로"])
            low_card_code = integer_like and uniq_count <= 20 and uniq_ratio <= 0.4
            seq_like = False
            if integer_like and numeric_s.dropna().shape[0] >= 3:
                vals = np.sort(numeric_s.dropna().astype(float).values)
                diffs = np.diff(vals)
                seq_like = bool(len(diffs) > 0 and (diffs == 1).mean() >= 0.95)
            # 코드/라벨 성격 숫자는 계산 대상이 아니라 범주로 취급
            if code_name or low_card_code:
                return "categorical"
            if id_name or seq_like:
                return "identifier"
            return "numeric"
        return "categorical"

    def _question_wants_drop_missing(self, question):
        q = str(question or "").lower()
        return any(k in q for k in ["결측 제외", "결측치 제외", "null 제외", "not set 제외", "(not set) 제외", "빈값 제외", "누락 제외"])

    def _aggregate_single(self, df, metric_col, op):
        if not metric_col:
            return pd.DataFrame({"row_count": [int(len(df))]})
        s = self._to_numeric_series(df[metric_col])
        if op == "count":
            return pd.DataFrame({f"{metric_col}_count": [int(s.notna().sum())]})
        if op == "mean":
            return pd.DataFrame({f"{metric_col}_mean": [float(s.mean(skipna=True) if s.notna().any() else 0)]})
        if op == "max":
            return pd.DataFrame({f"{metric_col}_max": [float(s.max(skipna=True) if s.notna().any() else 0)]})
        if op == "min":
            return pd.DataFrame({f"{metric_col}_min": [float(s.min(skipna=True) if s.notna().any() else 0)]})
        return pd.DataFrame({f"{metric_col}_sum": [float(s.sum(skipna=True) if s.notna().any() else 0)]})

    def _trend_aggregate(self, df, date_col, metric_col, op):
        work = df.copy()
        work[date_col] = pd.to_datetime(work[date_col], errors="coerce")
        work = work[work[date_col].notna()].copy()
        if work.empty:
            return pd.DataFrame(columns=[date_col, "value"])
        work["date_key"] = work[date_col].dt.date.astype(str)
        if metric_col:
            work[metric_col] = self._to_numeric_series(work[metric_col])
        g = work.groupby("date_key", dropna=False)
        if op == "count" or not metric_col:
            out = g.size().reset_index(name="count")
        elif op == "mean":
            out = g[metric_col].mean().reset_index(name=f"{metric_col}_mean")
        elif op == "max":
            out = g[metric_col].max().reset_index(name=f"{metric_col}_max")
        elif op == "min":
            out = g[metric_col].min().reset_index(name=f"{metric_col}_min")
        else:
            out = g[metric_col].sum().reset_index(name=f"{metric_col}_sum")
        out = out.sort_values("date_key", ascending=True).reset_index(drop=True)
        return out.head(400)

    def _build_followups(self, df, intent, meta):
        t = intent.get("type")
        if t == "schema":
            return [
                "핵심 지표 3개를 먼저 요약해볼까요?",
                "컬럼별 결측치/이상치를 점검해볼까요?",
                "샘플 10행 더 보기"
            ]
        if t == "column_probe":
            f = []
            if meta.get("show_unique"):
                f.append("고유값 전체 목록 보기")
                if meta.get("has_next"):
                    f.append("다음 500개 보기")
                if meta.get("has_prev"):
                    f.append("이전 500개 보기")
                f += ["샘플 10행 더 보기", "다른 컬럼과 교차 집계해볼까요?"]
                return f[:5]
            return [
                "샘플 10행 더 보기",
                "고유값 전체 목록 보기",
                "해당 컬럼의 고유값 개수도 볼까요?",
                "빈값/이상값 비율도 점검해볼까요?",
                "다른 컬럼과 교차 집계해볼까요?"
            ]
        if t == "column_count":
            return [
                "고유값 목록도 보여줄까요?",
                "결측치를 제외하고 다시 볼까요?",
                "다른 컬럼과 교차 집계해볼까요?"
            ]
        if t in {"groupby", "distribution", "compare"}:
            return [
                "상위 10개 항목만 추려서 볼까요?",
                "이전 기간과 비교할 수 있게 추이로 바꿔볼까요?",
                "비중(%) 기준으로 다시 정리해볼까요?"
            ]
        if t == "trend":
            return [
                "전주/전월과 비교해 증감률을 볼까요?",
                "추이에서 급증/급감 구간만 뽑아볼까요?",
                "채널/유형으로 분해해서 추이를 볼까요?"
            ]
        return [
            "요약부터 볼까요?",
            "집계(합계/평균/개수)로 볼까요?",
            "카테고리별 비교로 볼까요?"
        ]

    def _is_preview_more_request(self, question):
        q = str(question or "").lower()
        return any(k in q for k in ["더 보기", "더보여", "추가로 보여", "샘플 10", "10행"])

    def _detect_page_request(self, question):
        q = str(question or "").lower()
        next_kw = ["다음", "계속", "이어", "more", "next"]
        prev_kw = ["이전", "앞", "prev", "previous"]
        all_kw = ["500", "개", "보기", "페이지", "전체", "목록"]

        if any(k in q for k in next_kw) and any(k in q for k in all_kw):
            return {"direction": "next", "size": 500}
        if any(k in q for k in prev_kw) and any(k in q for k in all_kw):
            return {"direction": "prev", "size": 500}
        return None

    def _preview_rows(self, df, n=5):
        n = max(1, min(int(n), 10))
        part = df.head(n).copy()
        # 가독성을 위해 문자열 길이 제한
        for c in part.columns:
            part[c] = part[c].apply(lambda v: (str(v)[:40] + "…") if len(str(v)) > 40 else v)
        return part.where(pd.notnull(part), None).to_dict(orient="records")

    def _build_preview_tail(self, analysis_meta):
        rows = analysis_meta.get("preview_rows") or []
        if not rows:
            return ""
        lines = ["\n샘플 1~5행 미리보기:"]
        for i, row in enumerate(rows[:5], 1):
            pairs = [f"{k}={row.get(k)}" for k in list(row.keys())[:4]]
            lines.append(f"{i}) " + ", ".join(pairs))
        return "\n" + "\n".join(lines)

    def _detect_column_probe(self, df, question):
        q = str(question or "").lower()
        ask_value = any(k in q for k in ["어떤 데이터", "어떤 값", "값이 뭐", "내용이 뭐", "샘플", "미리보기", "1-5", "1~5", "5행", "목록", "종류", "전체", "모두", "고유값"])
        if not ask_value:
            return None
        best = None
        for c in df.columns:
            cl = str(c).lower()
            if cl and (cl in q or str(c) in question):
                best = c
                break
        if not best:
            return None
        m = re.search(r"(\d+)\s*[-~]\s*(\d+)", q)
        preview_count = 5
        if m:
            try:
                s, e = int(m.group(1)), int(m.group(2))
                if e >= s:
                    preview_count = max(1, min(e - s + 1, 20))
            except Exception:
                pass
        if any(k in q for k in ["전체", "모두", "목록", "종류", "고유값"]):
            preview_count = 500
        show_unique = any(k in q for k in ["전체", "모두", "목록", "종류", "고유값"])
        return {"target_column": best, "preview_count": preview_count, "show_unique": show_unique}

    def _detect_column_count(self, df, question):
        q = str(question or "").lower()
        if not any(k in q for k in ["몇개", "몇 개", "개수", "고유값", "unique"]):
            return None
        best = None
        for c in df.columns:
            cl = str(c).lower()
            if cl and (cl in q or str(c) in question):
                best = c
                break
        if not best:
            alias_map = {"회원번호": ["회원번호", "member_no", "memberid", "member_id", "moc_idx", "user_id", "uid", "id"]}
            for _, aliases in alias_map.items():
                if any(a in q for a in aliases):
                    for c in df.columns:
                        cl = str(c).lower()
                        if any(a in cl for a in aliases):
                            best = c
                            break
                if best:
                    break
        return best

    def _make_beginner_message(self, message, intent, meta):
        t = (intent or {}).get("type", "")
        msg = str(message or "")
        if t == "groupby":
            g = meta.get("group_col")
            m = meta.get("metric_col")
            return msg + f"\n\n쉽게 말하면: `{g}`별로 `{m}`를 묶어서 비교한 결과입니다."
        if t == "compare":
            g = meta.get("group_col")
            m = meta.get("metric_col")
            return msg + f"\n\n쉽게 말하면: `{g}` 그룹끼리 `{m}` 값 차이를 비교한 결과입니다."
        if t == "aggregate":
            m = meta.get("metric_col")
            op = meta.get("op")
            return msg + f"\n\n쉽게 말하면: `{m}`에 `{op}` 계산을 적용한 단일 요약값입니다."
        if t == "trend":
            p = meta.get("period")
            return msg + f"\n\n쉽게 말하면: 시간 흐름에 따라 값이 어떻게 바뀌는지 본 것입니다. ({p})"
        if t in {"schema", "columns_summary", "overview"}:
            return msg + "\n\n팁: 수치형은 계산(합계/평균), 범주형은 비교(~별)에 사용하면 됩니다."
        return msg

    def _raw_limit_by_intent(self, intent, meta):
        t = (intent or {}).get("type")
        if t in {"schema", "columns_summary"}:
            return 200
        if t in {"preview_more", "column_probe"}:
            return 500
        if t in {"column_count"}:
            return 10
        if t in {"groupby", "distribution", "compare", "trend"}:
            return 300
        return 100

    def _infer_dataset_period(self, df):
        date_col = self._guess_date_column(df, "")
        if not date_col or date_col not in df.columns:
            return None
        ser = pd.to_datetime(df[date_col], errors="coerce")
        ser = ser[ser.notna()]
        if ser.empty:
            return None
        start = ser.min().date().isoformat()
        end = ser.max().date().isoformat()
        return f"{start} ~ {end}"

    def _deterministic_summary(self, df, question):
        q = (question or "").lower()
        if any(k in q for k in ["사용자", "유저", "회원", "인원", "사람"]) and any(k in q for k in ["얼마나", "몇", "수", "명", "몇명", "몇 명"]):
            id_col = self._find_user_id_column(df)
            user_count = int(df[id_col].nunique(dropna=True)) if id_col else int(len(df))
            return f"이 파일 기준 사용자 수는 **{user_count}명**입니다."
        if any(k in q for k in ["회원번호", "회원 번호", "id", "번호"]) and any(k in q for k in ["몇개", "몇 개", "개수", "고유"]):
            for c in df.columns:
                cl = str(c).lower()
                if any(t in cl for t in ["member", "회원", "moc_idx", "user_id", "uid", "id", "번호"]):
                    cnt = int(df[c].dropna().astype(str).replace("", pd.NA).dropna().nunique())
                    return f"`{c}` 기준 고유 개수는 **{cnt:,}개**입니다."
        if any(k in q for k in ["어드민", "관리자", "admin"]) and any(k in q for k in ["얼마나", "몇", "수"]):
            admin_cols = self._find_admin_columns(df)
            admin_count = int(max(self._count_truthy(df[c]) for c in admin_cols)) if admin_cols else 0
            total_count = int(len(df))
            ratio = (admin_count / total_count * 100) if total_count else 0.0
            return f"관리자(어드민) 수는 **{admin_count}명**이며, 전체 대비 **{ratio:.1f}%**입니다."
        if any(k in q for k in ["어떤 데이터", "무슨 데이터", "또 어떤"]):
            numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
            categorical_cols = [c for c in df.columns if c not in numeric_cols]
            sample_cols = ", ".join([str(c) for c in df.columns[:8]])
            return (
                f"이 파일은 총 **{len(df)}행, {len(df.columns)}개 컬럼**입니다. "
                f"수치형 {len(numeric_cols)}개, 범주형 {len(categorical_cols)}개이며, "
                f"대표 컬럼은 `{sample_cols}` 입니다."
            )
        return ""

file_engine = FileAnalysisEngine()
