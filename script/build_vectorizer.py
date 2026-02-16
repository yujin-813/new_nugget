import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import joblib
from sklearn.feature_extraction.text import CountVectorizer
from ga4_metadata import GA4_METRICS, GA4_DIMENSIONS

def meta_to_doc(key, meta):
    ui = meta.get("ui_name", "")
    aliases = meta.get("aliases", []) or []
    return " ".join([key, ui, *aliases])

docs = []

for k, info in GA4_METRICS.items():
    docs.append(meta_to_doc(k, info))

for k, info in GA4_DIMENSIONS.items():
    docs.append(meta_to_doc(k, info))

vectorizer = CountVectorizer()
vectorizer.fit(docs)

joblib.dump(vectorizer, "vectorizer.pkl")

print("vectorizer.pkl 생성 완료")
