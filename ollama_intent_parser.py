# ollama_intent_parser.py

import requests
import json
import logging
import os
from ga4_metadata import GA4_METRICS, GA4_DIMENSIONS

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434/api/generate")
MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:3b-instruct")
TIMEOUT_SEC = float(os.getenv("OLLAMA_TIMEOUT_SEC", "6"))


def extract_intent(question: str):

    available_metrics = list(GA4_METRICS.keys())
    available_dims = list(GA4_DIMENSIONS.keys())

    prompt = f"""
너는 GA4 질의 파서다.
반드시 JSON으로만 응답해라.

형식:
{{
  "intent": "metric_single|metric_multi|breakdown|comparison|trend",
  "metrics": ["metric1", "metric2"],
  "dimensions": ["dimension1"],
  "limit": null or number
}}

사용 가능한 metrics:
{available_metrics}

사용 가능한 dimensions:
{available_dims}

질문:
{question}
"""

    payload = {
        "model": MODEL,
        "prompt": prompt,
        "stream": False,
        "format": "json",
        "options": {
            "temperature": 0
        }
    }

    try:
        r = requests.post(OLLAMA_URL, json=payload, timeout=TIMEOUT_SEC)
        r.raise_for_status()
        result = r.json().get("response", "{}")
        return json.loads(result)
    except Exception as e:
        logging.warning(f"[OllamaIntentParser] fallback: {e}")
        return {}
