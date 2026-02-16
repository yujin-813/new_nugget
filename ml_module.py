import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional, Tuple, Any

import joblib
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.linear_model import LogisticRegression

# =============================================================================
# Configuration
# =============================================================================

BASE_DIR = Path(__file__).resolve().parent

DEFAULT_TRAINING_FILE = BASE_DIR / "training_data.csv"
DEFAULT_FEEDBACK_FILE = BASE_DIR / "feedback.csv"

DEFAULT_VECTORIZER_FILE = BASE_DIR / "vectorizer.pkl"
DEFAULT_MODEL_METRIC_FILE = BASE_DIR / "model_metric.pkl"
DEFAULT_MODEL_DIMENSION_FILE = BASE_DIR / "model_dimension.pkl"

logging.getLogger(__name__).setLevel(logging.INFO)


# =============================================================================
# Helpers / Data Structures
# =============================================================================

@dataclass
class Prediction:
    value: str
    confidence: float


@dataclass
class PredictionResult:
    metric: Prediction
    dimension: Prediction


def _safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception as e:
        logging.error(f"[ml_module] Failed to read csv: {path} / {e}")
        return pd.DataFrame()


def _normalize_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    s = s.strip()
    return s


# =============================================================================
# Training Data Loader (training + feedback merge)
# =============================================================================

def load_training_data(
    training_file: Path = DEFAULT_TRAINING_FILE,
    feedback_file: Path = DEFAULT_FEEDBACK_FILE
) -> pd.DataFrame:
    """
    training_data.csv + feedback.csv를 합쳐서 학습용 데이터로 반환.

    기대 스키마(최소):
      - question
      - metric
      - dimension
    """
    training_df = _safe_read_csv(training_file)
    feedback_df = load_feedback_data(feedback_file)

    # 최소 컬럼 보정
    for col in ["question", "metric", "dimension"]:
        if col not in training_df.columns:
            training_df[col] = None
        if col not in feedback_df.columns:
            feedback_df[col] = None

    # training + feedback append 방식으로 병합 (안전/명확)
    merged = pd.concat(
        [
            training_df[["question", "metric", "dimension"]].copy(),
            feedback_df[["question", "metric", "dimension"]].copy()
        ],
        ignore_index=True
    )

    # 정리
    merged["question"] = merged["question"].astype(str).map(_normalize_text)
    merged["metric"] = merged["metric"].astype(str).map(_normalize_text)
    merged["dimension"] = merged["dimension"].astype(str).map(_normalize_text)

    merged = merged.dropna(subset=["question"])
    merged = merged[merged["question"].str.len() > 0]

    # 라벨이 비어있는 행 제거
    merged = merged[(merged["metric"].str.len() > 0) & (merged["dimension"].str.len() > 0)]

    merged = merged.reset_index(drop=True)
    return merged


# =============================================================================
# Model Training (sklearn only)
# =============================================================================

def train_models(
    training_file: Path = DEFAULT_TRAINING_FILE,
    feedback_file: Path = DEFAULT_FEEDBACK_FILE,
    vectorizer_file: Path = DEFAULT_VECTORIZER_FILE,
    model_metric_file: Path = DEFAULT_MODEL_METRIC_FILE,
    model_dimension_file: Path = DEFAULT_MODEL_DIMENSION_FILE,
    min_samples: int = 20
) -> Dict[str, Any]:
    """
    질문 -> metric / dimension 분류 모델 학습 후 pkl 저장.

    반환:
      {"ok": True/False, "rows": int, "metrics_classes": [...], "dimensions_classes": [...]}
    """
    data = load_training_data(training_file, feedback_file)
    if data.empty or len(data) < min_samples:
        return {
            "ok": False,
            "rows": int(len(data)),
            "error": f"Not enough training samples. need >= {min_samples}"
        }

    X_text = data["question"].tolist()
    y_metric = data["metric"].tolist()
    y_dimension = data["dimension"].tolist()

    vectorizer = CountVectorizer(
        ngram_range=(1, 2),
        min_df=1
    )
    X = vectorizer.fit_transform(X_text)

    model_metric = LogisticRegression(max_iter=2000, n_jobs=None)
    model_dimension = LogisticRegression(max_iter=2000, n_jobs=None)

    model_metric.fit(X, y_metric)
    model_dimension.fit(X, y_dimension)

    joblib.dump(vectorizer, vectorizer_file)
    joblib.dump(model_metric, model_metric_file)
    joblib.dump(model_dimension, model_dimension_file)

    return {
        "ok": True,
        "rows": int(len(data)),
        "metrics_classes": list(model_metric.classes_),
        "dimensions_classes": list(model_dimension.classes_)
    }


# =============================================================================
# Prediction
# =============================================================================

def predict_command_with_confidence(
    question: str,
    vectorizer_file: Path = DEFAULT_VECTORIZER_FILE,
    model_metric_file: Path = DEFAULT_MODEL_METRIC_FILE,
    model_dimension_file: Path = DEFAULT_MODEL_DIMENSION_FILE
) -> Optional[PredictionResult]:
    """
    v6.0: 신뢰도 점수를 포함하여 예측 결과 반환
    """
    try:
        if not vectorizer_file.exists() or not model_metric_file.exists() or not model_dimension_file.exists():
            logging.error("[ml_module] Model files not found. Train models first.")
            return None

        vectorizer: CountVectorizer = joblib.load(vectorizer_file)
        model_metric: LogisticRegression = joblib.load(model_metric_file)
        model_dimension: LogisticRegression = joblib.load(model_dimension_file)

        q = _normalize_text(question)
        X = vectorizer.transform([q])

        # metric
        m_probs = model_metric.predict_proba(X)[0]
        m_idx = int(m_probs.argmax())
        predicted_metric = str(model_metric.classes_[m_idx])
        m_confidence = float(m_probs[m_idx])

        # dimension
        d_probs = model_dimension.predict_proba(X)[0]
        d_idx = int(d_probs.argmax())
        predicted_dimension = str(model_dimension.classes_[d_idx])
        d_confidence = float(d_probs[d_idx])

        return PredictionResult(
            metric=Prediction(value=predicted_metric, confidence=m_confidence),
            dimension=Prediction(value=predicted_dimension, confidence=d_confidence)
        )

    except Exception as e:
        logging.error(f"[ml_module] ML Prediction Error: {e}")
        return None


def predict_command(question: str) -> Tuple[Optional[str], Optional[str]]:
    """
    기존 코드 호환용(구버전 호출 대비):
    metric, dimension만 리턴.
    """
    r = predict_command_with_confidence(question)
    if not r:
        return None, None
    return r.metric.value, r.dimension.value


# =============================================================================
# Date Parsing (Korean-friendly rules, no spacy)
# =============================================================================

_DATE_PATTERNS = [
    (r'(\d{4})-(\d{2})-(\d{2})', "%Y-%m-%d"),
    (r'(\d{4})/(\d{2})/(\d{2})', "%Y/%m/%d"),
    (r'(\d{4})\.(\d{2})\.(\d{2})', "%Y.%m.%d"),
]

_RELATIVE_N_DAYS = [
    (r"지난\s*(\d+)\s*일", "days"),
    (r"최근\s*(\d+)\s*일", "days"),
    (r"(\d+)\s*일\s*전", "days_ago"),
    (r"지난\s*(\d+)\s*주", "weeks"),
    (r"최근\s*(\d+)\s*주", "weeks"),
    (r"(\d+)\s*주\s*전", "weeks_ago"),
]


def parse_dates(question: str, now: Optional[datetime] = None) -> Tuple[str, str]:
    """
    한국어 질의에서 날짜 범위를 추출.
    - 명시 날짜: 2025-01-01 / 2025.01.01 / 2025/01/01
    - 상대 표현: 이번주/지난주/이번달/지난달/어제/오늘
    - N일/주: 지난 7일, 최근 14일, 2주 전 등

    반환: (start_date, end_date) in YYYY-MM-DD
    """
    if now is None:
        now = datetime.today()

    q = _normalize_text(question)
    today = now.date()

    # 1) explicit date
    for pattern, fmt in _DATE_PATTERNS:
        match = re.search(pattern, q)
        if match:
            dt = datetime.strptime(match.group(0), fmt).date()
            start_date = dt.strftime("%Y-%m-%d")
            end_date = today.strftime("%Y-%m-%d")
            return start_date, end_date

    # 2) "YYYY-MM-DD ~ YYYY-MM-DD" range (확장)
    range_match = re.search(r"(\d{4}[-/.]\d{2}[-/.]\d{2})\s*(~|부터|to)\s*(\d{4}[-/.]\d{2}[-/.]\d{2})", q)
    if range_match:
        left = range_match.group(1).replace(".", "-").replace("/", "-")
        right = range_match.group(3).replace(".", "-").replace("/", "-")
        # 간단 검증
        try:
            sdt = datetime.strptime(left, "%Y-%m-%d").date()
            edt = datetime.strptime(right, "%Y-%m-%d").date()
            if edt < sdt:
                sdt, edt = edt, sdt
            return sdt.strftime("%Y-%m-%d"), edt.strftime("%Y-%m-%d")
        except:
            pass

    # 3) relative week/month keywords
    if any(phrase in q for phrase in ["이번 주", "이번주", "이번 주간", "이번주간"]):
        start = (today - timedelta(days=today.weekday()))
        end = today
        return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

    if any(phrase in q for phrase in ["지난 주", "저번 주", "지난주", "저번주"]):
        # 지난주 월~일
        this_week_start = (today - timedelta(days=today.weekday()))
        last_week_end = this_week_start - timedelta(days=1)
        last_week_start = last_week_end - timedelta(days=6)
        return last_week_start.strftime("%Y-%m-%d"), last_week_end.strftime("%Y-%m-%d")

    if any(phrase in q for phrase in ["이번 달", "이번달", "이달", "요번달"]):
        start = today.replace(day=1)
        end = today
        return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

    if any(phrase in q for phrase in ["지난 달", "저번달", "지난달"]):
        first_of_this_month = today.replace(day=1)
        last_month_last_day = first_of_this_month - timedelta(days=1)
        last_month_first_day = last_month_last_day.replace(day=1)
        return last_month_first_day.strftime("%Y-%m-%d"), last_month_last_day.strftime("%Y-%m-%d")

    if "어제" in q:
        d = today - timedelta(days=1)
        return d.strftime("%Y-%m-%d"), d.strftime("%Y-%m-%d")

    if "오늘" in q:
        d = today
        return d.strftime("%Y-%m-%d"), d.strftime("%Y-%m-%d")

    # 4) N days/weeks patterns
    for pat, kind in _RELATIVE_N_DAYS:
        m = re.search(pat, q)
        if not m:
            continue
        n = int(m.group(1))

        if kind == "days":
            start = today - timedelta(days=n)
            end = today
            return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

        if kind == "days_ago":
            # "7일 전" = 단일 날짜로 볼지, 7일 전 ~ 오늘로 볼지 정책 필요
            # 기존 코드 스타일에 맞춰 "7일 전 ~ 오늘"
            start = today - timedelta(days=n)
            end = today
            return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

        if kind == "weeks":
            start = today - timedelta(days=7 * n)
            end = today
            return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

        if kind == "weeks_ago":
            start = today - timedelta(days=7 * n)
            end = today
            return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

    # 5) legacy shortcuts
    if "지난 7일" in q:
        start = today - timedelta(days=7)
        end = today
        return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

    if "지난 30일" in q:
        start = today - timedelta(days=30)
        end = today
        return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

    # 6) default
    start = today - timedelta(days=7)
    end = today
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


# =============================================================================
# Feedback Handling
# =============================================================================

def load_feedback_data(feedback_file: Path = DEFAULT_FEEDBACK_FILE) -> pd.DataFrame:
    """
    feedback.csv 스키마:
      - question
      - metric
      - dimension
      - start_date (optional)
      - end_date (optional)
    """
    df = _safe_read_csv(feedback_file)
    if df.empty:
        return pd.DataFrame(columns=["question", "metric", "dimension", "start_date", "end_date"])
    return df


def add_feedback(
    question: str,
    expected_metric: str,
    expected_dimension: str,
    expected_start_date: Optional[str] = None,
    expected_end_date: Optional[str] = None,
    feedback_file: Path = DEFAULT_FEEDBACK_FILE
) -> bool:
    """
    pandas 2.x 호환: append 대신 concat 사용
    """
    try:
        feedback_df = load_feedback_data(feedback_file)

        row = pd.DataFrame([{
            "question": _normalize_text(question),
            "metric": _normalize_text(expected_metric),
            "dimension": _normalize_text(expected_dimension),
            "start_date": expected_start_date,
            "end_date": expected_end_date
        }])

        feedback_df = pd.concat([feedback_df, row], ignore_index=True)
        feedback_df.to_csv(feedback_file, index=False)
        return True

    except Exception as e:
        logging.error(f"[ml_module] add_feedback error: {e}")
        return False


# =============================================================================
# CLI / Local test
# =============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # 1) Train (optional)
    train_result = train_models(min_samples=5)
    logging.info(f"[Train] {train_result}")

    # 2) Predict
    q = "이번 주 사용자수"
    pred = predict_command_with_confidence(q)
    start_date, end_date = parse_dates(q)

    logging.info(f"[Q] {q}")
    logging.info(f"[Dates] {start_date} ~ {end_date}")

    if pred:
        logging.info(f"[Pred] metric={pred.metric.value} ({pred.metric.confidence:.3f})")
        logging.info(f"[Pred] dimension={pred.dimension.value} ({pred.dimension.confidence:.3f})")
        add_feedback(q, pred.metric.value, pred.dimension.value, start_date, end_date)
    else:
        logging.info("[Pred] No model output (train models first).")
