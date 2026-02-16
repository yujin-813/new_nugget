import logging
import pandas as pd
import os
import openai
import json
from db_manager import DBManager

class FileAnalysisEngine:
    def __init__(self):
        pass

    def process(self, question, file_path, conversation_id=None, prev_source=None):
        logging.info(f"📁 [File Engine] Analyzing: {file_path}")
        if not file_path or not os.path.exists(file_path):
             return {"message": "파일을 찾을 수 없습니다.", "status": "error"}
             
        try:
            # 1. Load Data
            df = pd.read_csv(file_path) if file_path.endswith('.csv') else pd.read_excel(file_path)
            
            # 2. Context / State Management
            state = {}
            if conversation_id:
                # [Phase 5] Restore last intent if follow-up
                state = DBManager.load_last_state(conversation_id, "file") or {}
                
            # 3. Analyze Query (Level 1-3 Strategy)
            result_df, message, intent = self._analyze_query(df, question, state)
            
            # 4. Transform for Visualization
            plot_data = self._transform_to_plot_data(result_df, intent)
            
            # 5. Save State
            if result_df is not None and conversation_id:
                # [Phase 5] Save Intent for Follow-up
                state["last_intent"] = intent
                DBManager.save_success_state(conversation_id, "file", state)

            return {
                "message": message,
                "status": "ok",
                "plot_data": plot_data,
                "raw_data": result_df.head(50).where(pd.notnull(result_df), None).to_dict(orient='records') if result_df is not None else []
            }
            
        except Exception as e:
            logging.error(f"[File Engine Error] {e}")
            return {"message": f"파일 분석 오류: {e}", "status": "error"}

    def _analyze_query(self, df, question, state):
        # [Phase 5] 3-Level Processing Strategy
        intent = self._detect_intent(question, state)
        
        # 1. Level 1 & 2: Skip LLM (Exploration & Aggregation)
        if intent["type"] in ["schema", "preview", "overview", "groupby", "aggregate", "distribution", "filter"]:
            result_df = self._execute_aggregation(df, intent)
            
            if intent["type"] == "schema":
                msg = "요청하신 파일의 구조(컬럼 정보)입니다."
            elif intent["type"] == "preview":
                msg = f"파일의 상위 {len(result_df)}행 미리보기입니다."
            elif intent["type"] == "overview":
                msg = f"파일 전체 개요: 총 {len(df)}행, {len(df.columns)}개 컬럼"
            elif intent["type"] in ["groupby", "distribution"]:
                msg = f"요청하신 '{intent.get('keywords', ['그룹'])[0]}' 기준 집계 결과입니다."
            elif intent["type"] == "aggregate":
                if len(result_df) == 1 and result_df.shape[1] == 1:
                     val = result_df.iloc[0, 0]
                     msg = f"계산 결과: {val}"
                else:
                     msg = "요청하신 집계 결과입니다."
            else:
                msg = "분석 결과입니다."
            
            return result_df, msg, intent 

        # 2. Level 3: Insight (LLM Required)
        elif intent["type"] == "insight":
            logging.info("[FileEngine] Intent is Insight. Calling LLM.")
            # Insight logic requires schema and data summary
            result_df = df # Default to full df for context
            insight = self._generate_insight(df, result_df, question)
            return result_df, insight, intent
            
        # Default Fallback
        logging.info("[FileEngine] Intent Unclear or Default. Calling LLM for safety.")
        insight = self._generate_insight(df, df, question)
        return df, insight, intent

    def _detect_intent(self, question, state):
        q_lower = question.lower()
        intent = {"type": "insight", "keywords": []}
        
        # [Phase 5] 3-Level Intent Detection
        
        # Level 1: Exploration (탐색)
        if any(kw in q_lower for kw in ['구조', '컬럼', '열', 'schema', 'structure']):
             intent["type"] = "schema"
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
        elif any(kw in q_lower for kw in ['개수', 'count', '몇 개']):
            intent["type"] = "aggregate"
            intent["keywords"] = ["개수"]
            
        # Follow-up detection (Level 3 or Level 2 context)
        elif any(kw in q_lower for kw in ['응', '그래', '보여줘', '설명해줘']):
            if state.get("last_intent"):
                intent = state["last_intent"]
                intent["is_followup"] = True
            else:
                intent["type"] = "insight"
                
        return intent

    def _execute_aggregation(self, df, intent):
        # Simplified aggregation for restoration
        if intent["type"] == "schema":
            return pd.DataFrame({"Column": df.columns, "Type": df.dtypes.astype(str)})
        elif intent["type"] == "preview":
            return df.head(10)
        elif intent["type"] == "overview":
            return df.describe(include='all')
        # Placeholder for more complex groupby/agg
        return df.head(10)

    def _transform_to_plot_data(self, df, intent):
        if df is None or df.empty or intent["type"] in ["schema", "preview"]:
            return {"type": None, "labels": [], "series": []}
        # Simple bar for now
        return {"type": "bar", "labels": [], "series": []}

    def _generate_insight(self, df, result_df, question):
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

file_engine = FileAnalysisEngine()
