# relation_classifier.py
import json
import requests
import logging

OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL = "qwen2.5:3b-instruct"

ALLOWED_RELATIONS = {"refine", "new_topic", "metric_switch", "dimension_switch"}

def classify_relation(question: str, last_state: dict, delta: dict, timeout: int = 10) -> str:
    last_state = last_state or {}
    delta = delta or {}

    last_metrics = [m.get("name") for m in last_state.get("metrics", []) if isinstance(m, dict)]
    last_dims = [d.get("name") for d in last_state.get("dimensions", []) if isinstance(d, dict)]
    delta_metrics = [m.get("name") for m in delta.get("metrics", []) if isinstance(m, dict)]
    delta_dims = [d.get("name") for d in delta.get("dimensions", []) if isinstance(d, dict)]

    prompt = (
        "문맥 관계 분류기.\n"
        "JSON으로만 {\"relation\":\"refine|new_topic|metric_switch|dimension_switch\"} 형태로 출력.\n\n"
        f"last_state metrics={last_metrics} dims={last_dims}\n"
        f"delta metrics={delta_metrics} dims={delta_dims}\n"
        f"question={question}"
    )

    payload = {
        "model": MODEL,
        "prompt": prompt,
        "stream": False,
        "format": "json",
        "options": {
            "temperature": 0,
            "num_predict": 30
        }
    }

    try:
        r = requests.post(OLLAMA_URL, json=payload, timeout=30)
        r.raise_for_status()
        out = r.json().get("response", "").strip()
        obj = json.loads(out)

        relation = obj.get("relation")
        if relation not in ALLOWED_RELATIONS:
            raise ValueError(f"Invalid relation: {relation}")

        return relation

    except Exception as e:
        logging.warning(f"[RelationClassifier] fallback: {e}")
        return "new_topic"
