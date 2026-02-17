import re
import logging
from datetime import date, datetime, timedelta
from ga4_metadata import GA4_METRICS, GA4_DIMENSIONS
from ml_module import parse_dates

class DateParser:
    """Isolated logic for extracting start_date and end_date from natural language"""
    
    @staticmethod
    def parse(question, last_state=None, date_context=None):
        delta_dates = {"start_date": None, "end_date": None, "is_relative_shift": False}
        q = question.lower()
        
        # 1. Relative Shift ("그 전주", "전주")
        if date_context and ("그 전주" in q or ("전주" in q and "지난주" not in q and "이번주" not in q)):
            if last_state and last_state.get("start_date") and last_state.get("end_date"):
                try:
                    ls_start = datetime.strptime(last_state["start_date"], "%Y-%m-%d")
                    ls_end = datetime.strptime(last_state["end_date"], "%Y-%m-%d")
                    
                    delta_dates["start_date"] = (ls_start - timedelta(days=7)).strftime("%Y-%m-%d")
                    delta_dates["end_date"] = (ls_end - timedelta(days=7)).strftime("%Y-%m-%d")
                    delta_dates["is_relative_shift"] = True
                    logging.info(f"[DateParser] Relative shift detected: {delta_dates['start_date']} ~ {delta_dates['end_date']}")
                    return delta_dates
                except Exception as e:
                    logging.error(f"[DateParser] Relative date shift error: {e}")

        # 2. Standard period phrases
        period_phrases = []
        if "지난주" in q: period_phrases.append("지난주")
        if "이번주" in q: period_phrases.append("이번주")
        if "지난달" in q: period_phrases.append("지난달")
        if "이번달" in q: period_phrases.append("이번달")
        if "어제" in q: period_phrases.append("어제")
        if "오늘" in q: period_phrases.append("오늘")

        if period_phrases:
            phrase = period_phrases[0] # Pick first for single period
            s_date, e_date = DateParser._phrase_to_range(phrase)
            delta_dates["start_date"] = s_date
            delta_dates["end_date"] = e_date
            return delta_dates

        # 3. Explicit date parsing
        s, e = parse_dates(question)
        if s and e:
            delta_dates["start_date"], delta_dates["end_date"] = s, e

        return delta_dates

    @staticmethod
    def _phrase_to_range(phrase):
        today = date.today()
        if phrase == "오늘": s = e = today
        elif phrase == "어제": s = e = today - timedelta(days=1)
        elif phrase == "지난주":
            s = today - timedelta(days=today.weekday() + 7)
            e = s + timedelta(days=6)
        elif phrase == "이번주":
            s = today - timedelta(days=today.weekday())
            e = today
        elif phrase == "지난달":
            first_this_month = today.replace(day=1)
            e = first_this_month - timedelta(days=1)
            s = e.replace(day=1)
        elif phrase == "이번달":
            s = today.replace(day=1)
            e = today
        else:
            s = today - timedelta(days=7)
            e = today
        return s.strftime("%Y-%m-%d"), e.strftime("%Y-%m-%d")

    @staticmethod
    def get_default_context():
        """Centralized date math for the Parsing Layer (v9.0)"""
        today = date.today()
        yesterday = today - timedelta(days=1)
        
        # This week (ISO: Monday start)
        this_week_start = today - timedelta(days=today.weekday())
        this_week_end = today
        
        # Last week
        last_week_start = today - timedelta(days=today.weekday() + 7)
        last_week_end = last_week_start + timedelta(days=6)
        
        return {
            "today": today.strftime("%Y-%m-%d"),
            "yesterday": yesterday.strftime("%Y-%m-%d"),
            "this_week": {
                "start_date": this_week_start.strftime("%Y-%m-%d"),
                "end_date": this_week_end.strftime("%Y-%m-%d")
            },
            "last_week": {
                "start_date": last_week_start.strftime("%Y-%m-%d"),
                "end_date": last_week_end.strftime("%Y-%m-%d")
            },
            "reference_date": today.strftime("%Y-%m-%d")
        }

class MetricParser:
    """Isolated logic for matching metrics"""

    HIGH_THRESHOLD = 0.4
    MID_THRESHOLD = 0.25
    
    @staticmethod
    def _calc_score(q, meta):
        best_k, best_s = None, 0
        for k, info in meta.items():
            score = 0

            # API key 매칭
            if k.lower() in q:
                score += 50

            # UI name 매칭
            if info.get("ui_name", "").lower() in q:
                score += 60

            # alias 매칭
            for alias in info.get("aliases", []):
                if alias.lower() in q:
                    score += 40
                    break

            if score > best_s:
                best_k, best_s = k, score

        return best_k, best_s

    @staticmethod
    def parse(question, semantic=None):
        q = question.lower()

        # 1️⃣ Explicit substring matching
        best_m, m_score = MetricParser._calc_score(q, GA4_METRICS)
        if m_score > 30:
            return [{"name": best_m, "matched_by": "explicit"}]

        # 2️⃣ Semantic Fallback
        if semantic:
            sem_list = semantic.match_metric(question)

            if sem_list:
                top = sem_list[0]  # 가장 높은 유사도
                name = top.get("name")
                score = top.get("confidence", 0)

                if score >= MetricParser.HIGH_THRESHOLD:
                    return [{
                        "name": name,
                        "matched_by": "semantic_high",
                        "confidence": score
                    }]

                elif score >= MetricParser.MID_THRESHOLD:
                    return [{
                        "name": name,
                        "matched_by": "semantic_mid",
                        "confidence": score,
                        "needs_clarify": True
                    }]

        return []



class DimensionParser:
    """Isolated logic for matching dimensions"""

    HIGH_THRESHOLD = 0.4
    MID_THRESHOLD = 0.25

    @staticmethod
    def _calc_score(q, meta):
        best_k, best_s = None, 0
        for k, info in meta.items():
            score = 0

            # API key 매칭
            if k.lower() in q:
                score += 50

            # UI name 매칭
            if info.get("ui_name", "").lower() in q:
                score += 60

            # alias 매칭
            for alias in info.get("aliases", []):
                if alias.lower() in q:
                    score += 40
                    break

            if score > best_s:
                best_k, best_s = k, score

        return best_k, best_s


    @staticmethod
    def parse(question, semantic=None):
        q = question.lower()

        # 1️⃣ Explicit substring matching
        best_d, d_score = DimensionParser._calc_score(q, GA4_DIMENSIONS)
        if d_score > 30:
            return [{"name": best_d, "matched_by": "explicit"}]

        # 2️⃣ Semantic Fallback
        if semantic:
            sem_list = semantic.match_dimension(question)

            if sem_list:
                top = sem_list[0]
                name = top.get("name")
                score = top.get("confidence", 0)

                if score >= DimensionParser.HIGH_THRESHOLD:
                    return [{
                        "name": name,
                        "matched_by": "semantic_high",
                        "confidence": score
                    }]

                elif score >= DimensionParser.MID_THRESHOLD:
                    return [{
                        "name": name,
                        "matched_by": "semantic_mid",
                        "confidence": score,
                        "needs_clarify": True
                    }]

        return []



class IntentClassifier:
    """Classify the intent of the question (v9.0)"""
    @staticmethod
    def classify(question):
        q = question.lower()

      

        # 1. Category List (Highest Priority)
        if "종류" in q or "무슨 이벤트" in q or "어떤 이벤트" in q:
            return "category_list"

        # 2. Trend
        if any(k in q for k in ["추이", "흐름", "일별", "변화", "trend", "daily"]):
            return "trend"

        # 3. Comparison
        if any(k in q for k in ["전주 대비", "비교", "차이", "증감", "compare", "vs"]):
            return "comparison"

        # 4. Breakdown (Dimension Force)
        if any(k in q for k in ["별", "기준", "따라", "by "]):
            return "breakdown"

        # Default
        return "metric_single"


class QueryParser:
    """Orchestrator for all parsing logic"""
    
    @staticmethod
    def parse_to_delta(question, last_state=None, date_context=None, semantic=None):
        delta = {
            "start_date": None,
            "end_date": None,
            "metrics": [],
            "dimensions": [],
            "is_relative_shift": False,
            "event_candidate": None,
            "intent": "metric_single",  # ← 여기 쉼표 필수
            "clarify_candidates": []
        }

        
        # 0. Detect Intent
        intent = IntentClassifier.classify(question)
        delta["intent"] = intent
        logging.info(f"[QueryParser] Detected Intent: {intent}")

        

        # 1. Parse Dates
        dates = DateParser.parse(question, last_state, date_context)
        delta.update(dates)
        
        # 2. Parse Metrics (Multi-metric support)
        # Using a simple splitter for "와/과/및" could be risky for words containing them, 
        # but for a prototype, we check existence of multiple metrics in the whole sentence.
        # Better approach: Iterative extraction.
        
       
        # Basic parsing (extract all possible metrics)
        # We need a method to extract ALL valid metrics, not just the best one.
        # Reusing MetricParser but let's mod it slightly or iterate.
        
        # For now, we will try to find *all* metrics in the sentence.
        # This requires a new method in MetricParser or a loop here.
        # Let's do a simple loop over known metrics for now (O(N) scan).
        # 2. Parse Metrics (Multi-metric support)

        found_metrics = []
        q_lower = question.lower()

        for m_key, m_info in GA4_METRICS.items():

            # 1. API key 매칭
            if m_key.lower() in q_lower:
                found_metrics.append(m_key)
                continue

            # 2. UI 이름 매칭
            ui_name = m_info.get("ui_name", "")
            if ui_name and ui_name.lower() in q_lower:
                found_metrics.append(m_key)
                continue

            # 3. alias 매칭
            for alias in m_info.get("aliases", []):
                if alias.lower() in q_lower:
                    found_metrics.append(m_key)
                    break


       
        # ML Fallback if none found
        if not found_metrics:
            main_m = MetricParser.parse(question, semantic=semantic)

            if main_m:
                metric_obj = main_m[0]
                found_metrics.append(metric_obj["name"])

                if metric_obj.get("needs_clarify"):
                    delta["clarify_candidates"].append({
                        "type": "metric",
                        "candidate": metric_obj["name"],
                        "confidence": metric_obj.get("confidence")
                    })

            else:
                # 🔥🔥🔥 LLM Fallback 시작
                from ollama_intent_parser import extract_intent

                llm_result = extract_intent(question)

                logging.info(f"[LLM Intent Fallback] {llm_result}")

                llm_metrics = llm_result.get("metrics", [])
                llm_dims = llm_result.get("dimensions", [])
                llm_intent = llm_result.get("intent")
                llm_limit = llm_result.get("limit")

                if llm_metrics:
                    found_metrics.extend(llm_metrics)

                if llm_dims:
                    delta["dimensions"] = [{"name": d} for d in llm_dims]

                if llm_intent:
                    delta["intent"] = llm_intent

                if llm_limit:
                    delta["limit"] = llm_limit

            

            
       
        # Dedup but keep order
        found_metrics = list(dict.fromkeys(found_metrics))

        # 🔥 상품/아이템 키워드가 있으면 itemRevenue 자동 포함
        # 🔥 metric scope 기반 dimension 정합
        if delta["metrics"]:
            metric_scopes = set(
                GA4_METRICS.get(m["name"], {}).get("scope", "event")
                for m in delta["metrics"]
            )

            if "item" in metric_scopes:
                # item scope metric 존재 시 item scope dimension만 허용
                valid_item_dims = [
                    d_key for d_key, d_meta in GA4_DIMENSIONS.items()
                    if d_meta.get("scope") == "item"
                ]

                if delta["dimensions"]:
                    delta["dimensions"] = [
                        d for d in delta["dimensions"]
                        if d["name"] in valid_item_dims
                    ]

        # Multi-metric auto intent override
        if len(found_metrics) > 1:
            delta["intent"] = "metric_multi"

        delta["metrics"] = [{"name": m} for m in found_metrics]
        
        # 3. Parse Dimensions
        # Logic: If breakdown intent ("별"), we MUST find a dimension.
        # Similar all-scan strategy for dimensions
        found_dims = []
        for d_key, d_info in GA4_DIMENSIONS.items():
            if d_key == "date": continue # Handle date separately via intent/DateParser
            score = 0
            if d_key.lower() in question.lower(): score = 100
            for alias in d_info.get("aliases", []):
                if alias.lower() in question.lower():
                    score = 100
                    break
            if score == 100:
                found_dims.append(d_key)
        
        # Dedup but keep order
        found_dims = list(dict.fromkeys(found_dims))
        delta["dimensions"] = [{"name": d} for d in found_dims]
        
        # ML Fallback for Dimensions
        # ML / Semantic Fallback for Dimensions (breakdown intent)
        if not delta["dimensions"] and intent == "breakdown":
            # Try ML + semantic inside DimensionParser
            ml_d = DimensionParser.parse(question, semantic=semantic)

            if ml_d:
                dim_obj = ml_d[0]  # {"name": "...", "confidence": ..., "needs_clarify": ...}
                # ✅ dimensions는 GA4 request 스펙에 맞게 name만 남겨야 안전함
                delta["dimensions"] = [{"name": dim_obj["name"]}]

                # 🔥 mid-confidence면 clarify 후보로 등록
                if dim_obj.get("needs_clarify"):
                    delta.setdefault("clarify_candidates", [])
                    delta["clarify_candidates"].append({
                        "type": "dimension",
                        "candidate": dim_obj["name"],
                        "confidence": dim_obj.get("confidence", 0)
                    })


      
        # 🔥 Category-based Auto Dimension Repair
        candidate_dims = []

        if not delta["dimensions"]:
            metric_categories = set()
            metric_scopes = set()

            for m in delta.get("metrics", []):
                m_key = m["name"]
                m_meta = GA4_METRICS.get(m_key, {})
                if "category" in m_meta:
                    metric_categories.add(m_meta["category"])
                metric_scopes.add(m_meta.get("scope", "event"))

            for d_key, d_meta in GA4_DIMENSIONS.items():
                dim_category = d_meta.get("category")
                dim_scope = d_meta.get("scope", "event")

                if (
                    dim_category in metric_categories
                    and dim_scope in metric_scopes
                    and dim_category != "time"
                ):
                    candidate_dims.append((d_key, d_meta.get("priority", 0)))

        if not delta["dimensions"] and candidate_dims:
            candidate_dims.sort(key=lambda x: x[1], reverse=True)
            best_dim = candidate_dims[0][0]
            delta["dimensions"] = [{"name": best_dim}]

        # 4. Event Candidate
        event_patterns = [r'([a-zA-Z0-9_]+)\s*이벤트', r'([a-zA-Z0-9_]+)\s*event']
        for p in event_patterns:
            m = re.search(p, question.lower())
            if m:
                delta["event_candidate"] = m.group(1)
                break
        # 🔥 Top-N Detection
        top_n_match = re.search(r'(\d+)\s*(위|까지)', question)
        if top_n_match:
            delta["limit"] = int(top_n_match.group(1))
            logging.info(f"[TopN] Detected limit: {delta['limit']}")
        else:
            delta["limit"] = None
                
        # ---------------------------------
        # 🔥 Domain Logic based on Intent
        # ---------------------------------
        
        # [Rule 1] Category List Intent -> Force Schema
        if intent == "category_list":
            delta["dimensions"] = [{"name": "eventName"}]
            if not any(m["name"] in ["eventCount", "totalUsers"] for m in delta["metrics"]):
                 delta["metrics"] = [{"name": "eventCount"}] # Default
            # Remove date dimension (handled in engine, but here we explicitly don't add it)
            delta["is_trend_query"] = False 
            
        # [Rule 2] Trend Intent -> Force Date Dimension
        if intent == "trend":
            # Engine will ensure date dimension is added
            delta["is_trend_query"] = True
            
        # [Rule 3] Breakdown -> Ensure we have a dimension (Priority over Date)
        if intent == "breakdown" and delta["dimensions"]:
            # Logic: If user asks "by device", we prioritize device over date
            pass

        return delta
