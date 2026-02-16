# semantic_matcher.py
import joblib
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

class SemanticMatcher:
    """
    Explicit 매칭이 실패했을 때,
    질문과 (metric/dimension 메타 텍스트) 사이의 cosine similarity로 후보를 뽑는다.
    """

    def __init__(self, vectorizer_path="/mnt/data/vectorizer.pkl"):
        # CountVectorizer (기존 학습 산출물 재사용)
        self.vectorizer = joblib.load(vectorizer_path)

        self.metric_keys, self.metric_X = [], None
        self.dim_keys, self.dim_X = [], None

    @staticmethod
    def _meta_to_doc(key: str, meta: dict) -> str:
        ui = meta.get("ui_name", "")
        aliases = meta.get("aliases", []) or []
        # key + ui_name + aliases를 하나의 “문서”로 구성
        return " ".join([key, ui, *aliases]).strip()

    def build_metric_index(self, ga4_metrics: dict):
        docs, keys = [], []
        for k, info in ga4_metrics.items():
            keys.append(k)
            docs.append(self._meta_to_doc(k, info))
        self.metric_keys = keys
        self.metric_X = self.vectorizer.transform(docs)

    def build_dimension_index(self, ga4_dimensions: dict):
        docs, keys = [], []
        for k, info in ga4_dimensions.items():
            keys.append(k)
            docs.append(self._meta_to_doc(k, info))
        self.dim_keys = keys
        self.dim_X = self.vectorizer.transform(docs)

    def match_metric(self, question: str, top_k=5, min_sim=0.20):
        if self.metric_X is None:
            return []
        qv = self.vectorizer.transform([question])
        sims = cosine_similarity(qv, self.metric_X).ravel()
        idx = np.argsort(-sims)[:top_k]
        out = []
        for i in idx:
            if sims[i] >= min_sim:
                out.append({
                    "name": self.metric_keys[i],
                    "confidence": float(sims[i]),
                    "score": float(sims[i]),   # 🔥 compatibility
                        "matched_by": "semantic"
                })
        return out


    def match_dimension(self, question: str, top_k=5, min_sim=0.20):
        if self.dim_X is None:
            return []
        qv = self.vectorizer.transform([question])
        sims = cosine_similarity(qv, self.dim_X).ravel()
        idx = np.argsort(-sims)[:top_k]
        out = []
        for i in idx:
            if sims[i] >= min_sim:
                out.append({
                    "name": self.dim_keys[i],
                    "confidence": float(sims[i]),
                    "score": float(sims[i]),   # 🔥 compatibility
                    "matched_by": "semantic"
                })
        return out

