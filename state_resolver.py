import logging
from ga4_metadata import GA4_METRICS

class StateResolver:
    @staticmethod
    def resolve(last_state, delta, source="ga4", prev_source=None, question=""):
        """
        Sophisticated State Management
        Handles context inheritance, intent-based overwriting, and entity memory.
        """
        if not isinstance(last_state, dict):
            last_state = {}

        if not isinstance(delta, dict):
            delta = {}

        intent = delta.get("intent", "metric_single")

        # Source Change Guard
        if prev_source and prev_source != source:
            logging.info(f"[StateResolver] Source changed ({prev_source} -> {source}). Resetting state.")
            last_state = {}

        def detect_scope(metrics):
            scopes = set()
            for m in metrics:
                meta = GA4_METRICS.get(m["name"], {})
                scopes.add(meta.get("scope", "event"))

            if len(scopes) == 1:
                return list(scopes)[0]
            return "mixed"

        # 🔥 Followup은 state 상속을 최소화해야 함 (특히 metrics/dimensions 오염 방지)
        if intent == "followup":
            logging.info("[StateResolver] Followup intent detected → skip state inheritance for metrics/dimensions")
            last_state = {
                "start_date": last_state.get("start_date"),
                "end_date": last_state.get("end_date"),
                "periods": last_state.get("periods"),
                "event_filter": last_state.get("event_filter"),
                "entity_filter": last_state.get("entity_filter"),
                "last_entity": last_state.get("last_entity"),
                "is_trend_query": last_state.get("is_trend_query", False),
                "scope_type": last_state.get("scope_type")
            }

        # 1. Entity Memory
        if "그 " in question and last_state.get("last_entity"):
            ent = last_state["last_entity"]
            delta["dimensions"] = [{"name": ent["dimension"]}]
            delta["entity_filter"] = ent["value"]
            logging.info(f"[StateResolver] Applied Entity Memory: {ent['value']}")

        # 2. Metric Resolution
        metrics = delta.get("metrics") or []
        if not metrics and last_state.get("metrics"):
            metrics = last_state["metrics"]

        if intent == "metric_multi" and delta.get("metrics"):
            metrics = delta["metrics"]

        # ✅ 현재 scope 계산 (중요: 여기서 current_scope가 정의됨)
        current_scope = detect_scope(metrics)

        # 3. Dimension Resolution
        dimensions = delta.get("dimensions") or []

        if last_state.get("scope_type") and last_state.get("scope_type") != current_scope:
            logging.info(f"[StateResolver] Scope changed ({last_state.get('scope_type')} -> {current_scope}) → block inheritance")

        else:
            if not dimensions and last_state.get("dimensions"):
                dimensions = last_state["dimensions"]

            if intent == "breakdown" and delta.get("dimensions"):
                dimensions = delta["dimensions"]


        
        # 4. Date/Period Resolution
        start_date = delta.get("start_date") or last_state.get("start_date")
        end_date = delta.get("end_date") or last_state.get("end_date")
        periods = delta.get("periods") or last_state.get("periods") or []

        if periods:
            start_date = None
            end_date = None

        # 5. Filter Resolution
        event_filter = delta.get("event_filter") or last_state.get("event_filter")
        entity_filter = delta.get("entity_filter") or last_state.get("entity_filter")

        # ✅ scope 변경 시 필터/엔티티 오염 방지 (필요 최소만 리셋)
        if last_state.get("scope_type") and last_state.get("scope_type") != current_scope:
            event_filter = delta.get("event_filter")
            entity_filter = delta.get("entity_filter")

        final_state = {
            "metrics": metrics,
            "dimensions": dimensions,
            "start_date": start_date,
            "end_date": end_date,
            "periods": periods,
            "event_filter": event_filter,
            "entity_filter": entity_filter,
            "active_source": source,
            "last_entity": last_state.get("last_entity"),
            "is_trend_query": delta.get("is_trend_query", last_state.get("is_trend_query", False)),
            "scope_type": current_scope
        }

        logging.info(f"[StateResolver] Resolved State: {final_state}")
        return final_state
