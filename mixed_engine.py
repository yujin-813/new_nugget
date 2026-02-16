import logging
import json
import openai

class MixedAnalysisEngine:
    def __init__(self, ga4_engine, file_engine):
        self.ga4 = ga4_engine
        self.file = file_engine

    def process(self, question, property_id, file_path, conversation_id=None):
        logging.info("⚖️ [Mixed Engine] Starting Comparative Analysis")
        
        # 1. Fetch GA4 Data
        ga4_res = self.ga4.process(question, property_id, conversation_id)
        
        # 2. Fetch File Data
        file_res = self.file.process(question, file_path, conversation_id)
        
        # 3. Aggregate Insight (LLM)
        ga4_summary = ga4_res.get("message", "GA4 데이터 없음")
        file_summary = file_res.get("message", "파일 데이터 없음")
        
        prompt = f"""
        Compare the following data sources for the user query: "{question}"
        
        [Source A: GA4]
        {ga4_summary}
        
        [Source B: Uploaded File]
        {file_summary}
        
        Provide a unified strategic insight in Korean. Focus on discrepancies or common trends.
        """
        
        try:
            res = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}]
            )
            unified_msg = res['choices'][0]['message']['content'].strip()
        except:
            unified_msg = f"GA4와 파일 데이터를 모두 분석했습니다.\n\n[GA4 인사이트]\n{ga4_summary}\n\n[파일 분석]\n{file_summary}"

        return {
            "message": unified_msg,
            "status": "ok",
            "plot_data": ga4_res.get("plot_data", []),
            "ga4_details": ga4_res,
            "file_details": file_res
        }
