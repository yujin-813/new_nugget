import math
import re
import statistics
import uuid
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple


SIGNAL_TYPES = [
    "kpi_drop",
    "kpi_rise",
    "efficiency_distortion",
    "contribution_shift",
    "funnel_break",
    "opportunity_segment",
]


def _to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, bool):
        return float(int(v))
    if isinstance(v, (int, float)):
        if math.isnan(float(v)) or math.isinf(float(v)):
            return None
        return float(v)
    s = str(v).strip()
    if not s:
        return None
    s = re.sub(r"[^\d\.\-]", "", s)
    if s in {"", "-", ".", "-."}:
        return None
    try:
        x = float(s)
        if math.isnan(x) or math.isinf(x):
            return None
        return x
    except Exception:
        return None


def _format_num(v: Optional[float]) -> str:
    if v is None:
        return "-"
    if abs(v) >= 1000:
        return f"{v:,.0f}"
    if abs(v) >= 100:
        return f"{v:,.1f}"
    return f"{v:,.2f}"


def _safe_pct(cur: float, prev: float) -> float:
    if prev == 0:
        return 0.0 if cur == 0 else 100.0
    return ((cur - prev) / abs(prev)) * 100.0


def _is_boolish(values: List[float]) -> bool:
    uniq = {int(v) for v in values if v in (0.0, 1.0)}
    return len(values) > 0 and len(uniq) <= 2 and len(uniq) > 0 and all(v in (0.0, 1.0) for v in values)


def _looks_like_id_column(col: str, raw_values: List[Any], numeric_values: List[float]) -> bool:
    c = str(col or "").lower()
    id_name = any(tok in c for tok in ["_id", "id_", " id", "idx", "code", "번호", "key"])
    if id_name:
        return True
    metric_name_hints = ["user", "session", "revenue", "amount", "매출", "수익", "count", "rate", "ratio", "pct", "percent", "전환"]
    if any(h in c for h in metric_name_hints):
        return False
    # 일반 숫자 칼럼을 과도하게 id로 오인하지 않도록 이름 기반에 더 강하게 의존
    if any(tok in c for tok in ["no", "num", "seq"]):
        if numeric_values and all(abs(v - int(v)) < 1e-9 for v in numeric_values):
            return True
    return False


def _detect_time_column(rows: List[Dict[str, Any]]) -> Optional[str]:
    if not rows:
        return None
    keys = list(rows[0].keys())
    candidates = []
    for k in keys:
        kl = str(k).lower()
        if any(tok in kl for tok in ["date", "day", "dt", "time", "month", "ym", "yearmonth", "일자", "날짜"]):
            candidates.append(k)
    for c in candidates:
        ok = 0
        for r in rows[:50]:
            v = r.get(c)
            if v is None:
                continue
            if re.match(r"^\d{4}-\d{2}-\d{2}$", str(v)):
                ok += 1
            elif re.match(r"^\d{8}$", str(v)):
                ok += 1
            elif re.match(r"^\d{6}$", str(v)):  # yearMonth
                ok += 1
        if ok >= 3:
            return c
    return None


def _sort_rows_by_time(rows: List[Dict[str, Any]], time_col: str) -> List[Dict[str, Any]]:
    def parse_key(v: Any) -> Tuple[int, str]:
        s = str(v or "")
        if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
            return (0, s)
        if re.match(r"^\d{8}$", s):
            return (1, f"{s[:4]}-{s[4:6]}-{s[6:8]}")
        if re.match(r"^\d{6}$", s):
            return (2, f"{s[:4]}-{s[4:6]}-01")
        return (9, s)

    return sorted(rows, key=lambda r: parse_key(r.get(time_col)))


def _split_current_previous(
    current_rows: List[Dict[str, Any]],
    previous_rows: Optional[List[Dict[str, Any]]] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    if previous_rows is not None and len(previous_rows) > 0:
        return current_rows, previous_rows
    if not current_rows:
        return [], []
    time_col = _detect_time_column(current_rows)
    rows = current_rows
    if time_col:
        rows = _sort_rows_by_time(current_rows, time_col)
    n = len(rows)
    half = max(1, n // 2)
    prev = rows[:half]
    cur = rows[half:]
    if not cur:
        cur = rows[-half:]
        prev = rows[:-half]
    return cur, prev


def _detect_columns(rows: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    if not rows:
        return {"metrics": [], "dimensions": []}
    keys = list(rows[0].keys())
    metrics: List[str] = []
    dimensions: List[str] = []
    for k in keys:
        raw_values = [r.get(k) for r in rows]
        numeric_values = [x for x in (_to_float(v) for v in raw_values) if x is not None]
        numeric_ratio = len(numeric_values) / max(1, len(raw_values))
        if numeric_ratio >= 0.75 and not _looks_like_id_column(k, raw_values, numeric_values):
            if _is_boolish(numeric_values):
                dimensions.append(k)
            else:
                metrics.append(k)
        else:
            dimensions.append(k)
    return {"metrics": metrics, "dimensions": dimensions}


def _sum_metric(rows: List[Dict[str, Any]], metric: str) -> float:
    total = 0.0
    for r in rows:
        v = _to_float(r.get(metric))
        if v is not None:
            total += v
    return total


def _series_metric(rows: List[Dict[str, Any]], metric: str) -> List[float]:
    out = []
    for r in rows:
        v = _to_float(r.get(metric))
        if v is not None:
            out.append(v)
    return out


def _moving_average(values: List[float], window: int = 7) -> float:
    if not values:
        return 0.0
    w = max(1, min(window, len(values)))
    return sum(values[-w:]) / w


def _z_score(current_value: float, values: List[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = statistics.mean(values)
    std = statistics.pstdev(values)
    if std == 0:
        return 0.0
    return (current_value - mean) / std


def _confidence(abs_change_pct: float, volume: float, z: float) -> float:
    c = 0.2
    c += min(0.4, abs_change_pct / 100.0)
    c += min(0.2, volume / 100000.0)
    c += min(0.2, abs(z) / 5.0)
    return max(0.0, min(1.0, c))


def _find_metric(metrics: List[str], aliases: List[str]) -> Optional[str]:
    lower_map = {m.lower(): m for m in metrics}
    for a in aliases:
        for mk, mv in lower_map.items():
            if a in mk:
                return mv
    return None


class MarketingIntelligenceEngine:
    def __init__(self):
        self.default_weights = {
            "weight_change": 1.0,
            "weight_contribution": 60.0,
            "weight_confidence": 20.0,
            "quality_threshold": 20.0,
            "change_threshold_pct": 8.0,
            "min_volume": 30.0,
        }

    def build_feature_layer(
        self,
        current_rows: List[Dict[str, Any]],
        previous_rows: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        cur_rows, prev_rows = _split_current_previous(current_rows, previous_rows)
        all_rows = list(prev_rows) + list(cur_rows)
        cols = _detect_columns(all_rows)
        metrics = cols["metrics"]
        dimensions = cols["dimensions"]
        if not metrics:
            raise ValueError("No numeric metric columns detected")

        metric_totals_cur = {m: _sum_metric(cur_rows, m) for m in metrics}
        metric_totals_prev = {m: _sum_metric(prev_rows, m) for m in metrics}
        total_cur_sum = sum(metric_totals_cur.values()) or 1.0

        features: List[Dict[str, Any]] = []
        for m in metrics:
            cur = metric_totals_cur.get(m, 0.0)
            prev = metric_totals_prev.get(m, 0.0)
            change_pct = _safe_pct(cur, prev)
            series_all = _series_metric(all_rows, m)
            ma = _moving_average(series_all)
            z = _z_score(cur, series_all)
            feat = {
                "feature_type": "metric",
                "metric": m,
                "current": cur,
                "previous": prev,
                "change_pct": change_pct,
                "moving_average": ma,
                "contribution_ratio": cur / total_cur_sum if total_cur_sum else 0.0,
                "z_score": z,
                "volume": cur,
            }
            features.append(feat)

        # efficiency_ratio / per_user_metrics
        user_metric = _find_metric(metrics, ["activeuser", "user", "구매자", "후원자"])
        revenue_metric = _find_metric(metrics, ["revenue", "매출", "수익", "amount"])
        if user_metric and revenue_metric:
            cur_users = metric_totals_cur.get(user_metric, 0.0)
            prev_users = metric_totals_prev.get(user_metric, 0.0)
            cur_rev = metric_totals_cur.get(revenue_metric, 0.0)
            prev_rev = metric_totals_prev.get(revenue_metric, 0.0)
            cur_eff = cur_rev / cur_users if cur_users else 0.0
            prev_eff = prev_rev / prev_users if prev_users else 0.0
            features.append(
                {
                    "feature_type": "efficiency_ratio",
                    "metric": f"efficiency_ratio::{revenue_metric}/{user_metric}",
                    "related_metric": revenue_metric,
                    "base_metric": user_metric,
                    "current": cur_eff,
                    "previous": prev_eff,
                    "change_pct": _safe_pct(cur_eff, prev_eff),
                    "moving_average": cur_eff,
                    "contribution_ratio": 0.0,
                    "z_score": 0.0,
                    "volume": cur_users,
                }
            )

        # segment share
        top_dimension = None
        for d in dimensions:
            card = len(set(str(r.get(d) or "") for r in all_rows))
            if 2 <= card <= 15 and not any(tok in str(d).lower() for tok in ["date", "day", "month", "year"]):
                top_dimension = d
                break
        if top_dimension:
            primary_metric = revenue_metric or metrics[0]
            cur_group: Dict[str, float] = {}
            prev_group: Dict[str, float] = {}
            for r in cur_rows:
                key = str(r.get(top_dimension) or "(not set)")
                cur_group[key] = cur_group.get(key, 0.0) + (_to_float(r.get(primary_metric)) or 0.0)
            for r in prev_rows:
                key = str(r.get(top_dimension) or "(not set)")
                prev_group[key] = prev_group.get(key, 0.0) + (_to_float(r.get(primary_metric)) or 0.0)
            cur_total = sum(cur_group.values()) or 1.0
            prev_total = sum(prev_group.values()) or 1.0
            for k, cur_v in sorted(cur_group.items(), key=lambda x: x[1], reverse=True)[:10]:
                prev_v = prev_group.get(k, 0.0)
                share_cur = (cur_v / cur_total) * 100.0
                share_prev = (prev_v / prev_total) * 100.0
                features.append(
                    {
                        "feature_type": "segment_share",
                        "metric": f"{top_dimension}:{k}",
                        "related_metric": primary_metric,
                        "current": share_cur,
                        "previous": share_prev,
                        "change_pct": _safe_pct(share_cur, share_prev),
                        "moving_average": share_cur,
                        "contribution_ratio": cur_v / (sum(metric_totals_cur.values()) or 1.0),
                        "z_score": 0.0,
                        "volume": cur_v,
                        "dimension": top_dimension,
                        "segment": k,
                    }
                )

        # funnel drop (if available)
        funnel_order = [
            "page_view",
            "session",
            "visit",
            "view_item",
            "add_to_cart",
            "begin_checkout",
            "purchase",
            "conversion",
        ]
        funnel_metrics = []
        lower_metrics = {m.lower(): m for m in metrics}
        for step in funnel_order:
            for mk, mv in lower_metrics.items():
                if step in mk and mv not in funnel_metrics:
                    funnel_metrics.append(mv)
                    break
        if len(funnel_metrics) >= 2:
            first, last = funnel_metrics[0], funnel_metrics[-1]
            cur_first = metric_totals_cur.get(first, 0.0)
            cur_last = metric_totals_cur.get(last, 0.0)
            prev_first = metric_totals_prev.get(first, 0.0)
            prev_last = metric_totals_prev.get(last, 0.0)
            cur_drop = 100.0 - ((cur_last / cur_first) * 100.0) if cur_first > 0 else 0.0
            prev_drop = 100.0 - ((prev_last / prev_first) * 100.0) if prev_first > 0 else 0.0
            features.append(
                {
                    "feature_type": "funnel_drop_rate",
                    "metric": f"funnel_drop_rate::{first}->{last}",
                    "related_metric": f"{first}->{last}",
                    "current": cur_drop,
                    "previous": prev_drop,
                    "change_pct": _safe_pct(cur_drop, prev_drop),
                    "moving_average": cur_drop,
                    "contribution_ratio": 0.0,
                    "z_score": 0.0,
                    "volume": cur_first,
                }
            )

        return {
            "metrics": metrics,
            "dimensions": dimensions,
            "current_rows_count": len(cur_rows),
            "previous_rows_count": len(prev_rows),
            "features": features,
        }

    def build_signal_layer(self, feature_output: Dict[str, Any]) -> List[Dict[str, Any]]:
        features = feature_output.get("features") or []
        change_threshold = float(self.default_weights["change_threshold_pct"])
        min_volume = float(self.default_weights["min_volume"])

        signals: List[Dict[str, Any]] = []
        for f in features:
            change_pct = float(f.get("change_pct") or 0.0)
            volume = float(f.get("volume") or 0.0)
            z = float(f.get("z_score") or 0.0)
            abs_change = abs(change_pct)
            ftype = str(f.get("feature_type") or "metric")

            if ftype in {"metric", "efficiency_ratio", "funnel_drop_rate"}:
                if abs_change < change_threshold:
                    continue
                if volume < min_volume and ftype != "efficiency_ratio":
                    continue

            signal_type = "kpi_rise"
            if ftype == "metric":
                signal_type = "kpi_drop" if change_pct < 0 else "kpi_rise"
            elif ftype == "efficiency_ratio":
                signal_type = "efficiency_distortion"
            elif ftype == "segment_share":
                if abs_change >= change_threshold:
                    signal_type = "contribution_shift"
                elif change_pct > change_threshold and float(f.get("current") or 0.0) >= 15.0:
                    signal_type = "opportunity_segment"
                else:
                    continue
            elif ftype == "funnel_drop_rate":
                signal_type = "funnel_break"
            else:
                continue

            conf = _confidence(abs_change, volume, z)
            signals.append(
                {
                    "signal_type": signal_type,
                    "metric": str(f.get("metric") or ""),
                    "related_metric": str(f.get("related_metric") or ""),
                    "change_pct": change_pct,
                    "contribution_ratio": float(f.get("contribution_ratio") or 0.0),
                    "confidence_score": conf,
                    "current": float(f.get("current") or 0.0),
                    "previous": float(f.get("previous") or 0.0),
                    "volume": volume,
                    "z_score": z,
                    "dimension": f.get("dimension"),
                    "segment": f.get("segment"),
                    "feature_type": ftype,
                }
            )
        return signals

    def score_signals(
        self,
        signals: List[Dict[str, Any]],
        company_signal_weights: Dict[str, float],
    ) -> List[Dict[str, Any]]:
        scored = []
        for s in signals:
            signal_type = str(s.get("signal_type") or "")
            abs_change = abs(float(s.get("change_pct") or 0.0))
            contribution_ratio = float(s.get("contribution_ratio") or 0.0)
            confidence_score = float(s.get("confidence_score") or 0.0)
            org_preference_weight = float(company_signal_weights.get(signal_type, 0.0))
            score = (
                abs_change * float(self.default_weights["weight_change"])
                + contribution_ratio * float(self.default_weights["weight_contribution"])
                + org_preference_weight
                + confidence_score * float(self.default_weights["weight_confidence"])
            )
            s2 = dict(s)
            s2["score"] = score
            s2["org_preference_weight"] = org_preference_weight
            scored.append(s2)
        scored.sort(key=lambda x: float(x.get("score") or 0.0), reverse=True)
        return scored

    def build_action_card(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        signal_type = str(signal.get("signal_type") or "")
        metric = str(signal.get("metric") or "metric")
        current = float(signal.get("current") or 0.0)
        previous = float(signal.get("previous") or 0.0)
        change_pct = float(signal.get("change_pct") or 0.0)
        abs_change = abs(change_pct)
        score = float(signal.get("score") or 0.0)
        confidence = float(signal.get("confidence_score") or 0.0)

        def risk() -> str:
            if abs_change >= 20 or confidence >= 0.85:
                return "High"
            if abs_change >= 12 or confidence >= 0.65:
                return "Medium"
            return "Low"

        title_prefix = {
            "kpi_drop": "핵심 지표 하락",
            "kpi_rise": "성과 상승 신호",
            "efficiency_distortion": "효율 왜곡 신호",
            "contribution_shift": "구성비 급변 신호",
            "funnel_break": "퍼널 단절 신호",
            "opportunity_segment": "기회 세그먼트 발견",
        }.get(signal_type, "핵심 실행 신호")

        title = f"{title_prefix}: {metric} ({change_pct:+.1f}%)"
        evidence = [
            f"현재값 {_format_num(current)} / 이전값 {_format_num(previous)} ({change_pct:+.1f}%)",
            f"신뢰도 {confidence:.2f}, 우선순위 점수 {score:.1f}",
        ]
        if signal.get("segment"):
            evidence.append(f"세그먼트: {signal.get('segment')}")

        recommendations_map = {
            "kpi_drop": ["유입-전환 단계별 병목 점검", "상위 채널 랜딩페이지 A/B 테스트", "전환 마찰 구간 UI 개선"],
            "kpi_rise": ["상승 요인 재현 실험", "상위 세그먼트 예산 확장", "증가 원인 채널 집중 투자"],
            "efficiency_distortion": ["고비용 저효율 구간 차단", "고효율 세그먼트 재타겟팅", "CPA/ROAS 기준 리밸런싱"],
            "contribution_shift": ["기여도 급변 세그먼트 검증", "집중도 리스크 분산 실험", "하락 세그먼트 복구 캠페인"],
            "funnel_break": ["퍼널 단계별 이탈 원인 분석", "폼/결제 단계 단축 실험", "퍼널 단계 이벤트 정합성 점검"],
            "opportunity_segment": ["상승 세그먼트 확장 실험", "세그먼트 전용 메시지 최적화", "캠페인 예산 우선 배정"],
        }
        recs = recommendations_map.get(signal_type, ["상세 원인 분석", "우선순위 실험 설계"])
        expected = "+3~8%"
        if abs_change >= 20:
            expected = "+8~15%"
        elif abs_change >= 12:
            expected = "+5~12%"

        return {
            "card_id": str(uuid.uuid4()),
            "title": title,
            "problem_summary": f"{metric} 지표의 변화 폭이 커 우선 대응이 필요합니다.",
            "evidence": evidence,
            "risk_level": risk(),
            "expected_impact_range": expected,
            "recommended_executions": recs[:3],
            "signal": signal,
        }

    def analyze(
        self,
        current_rows: List[Dict[str, Any]],
        previous_rows: Optional[List[Dict[str, Any]]],
        company_signal_weights: Dict[str, float],
        source: str = "unknown",
    ) -> Dict[str, Any]:
        feature_output = self.build_feature_layer(current_rows, previous_rows)
        signals = self.build_signal_layer(feature_output)
        scored = self.score_signals(signals, company_signal_weights)
        threshold = float(self.default_weights["quality_threshold"])
        top = None
        for s in scored:
            if float(s.get("score") or 0.0) >= threshold:
                top = s
                break
        if not top and scored:
            top = scored[0]
        card = self.build_action_card(top) if top else None
        return {
            "source": source,
            "feature_layer": feature_output,
            "signals": scored,
            "top_action_card": card,
            "quality_threshold": threshold,
            "signal_count": len(scored),
        }

    def build_execution_plan(
        self,
        action_card: Dict[str, Any],
        primary_metric: Optional[str] = None,
        related_dimensions: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        signal = action_card.get("signal") or {}
        metric = primary_metric or str(signal.get("metric") or "")
        baseline = float(signal.get("current") or 0.0)
        signal_type = str(signal.get("signal_type") or "")
        expected_direction = "increase"
        if signal_type in {"kpi_rise"}:
            expected_direction = "maintain_or_increase"
        evaluation_date = (date.today() + timedelta(days=14)).isoformat()
        dims = related_dimensions or ([str(signal.get("dimension"))] if signal.get("dimension") else [])
        dims = [d for d in dims if d]
        hypothesis = f"{metric} 지표 저하 원인은 상위 트래픽/세그먼트 병목 가능성이 높다."
        if signal_type == "kpi_rise":
            hypothesis = f"{metric} 지표 상승 원인을 재현하면 추가 성장 가능성이 높다."
        checklist = [
            "실험 대상 세그먼트/채널 확정",
            "기준선(baseline) 재확인",
            "실험군/대조군 정의",
            "2주 측정 기간 고정",
            "부작용 지표 모니터링",
        ]
        return {
            "experiment_id": str(uuid.uuid4()),
            "primary_metric": metric,
            "baseline_value": baseline,
            "expected_direction": expected_direction,
            "evaluation_date": evaluation_date,
            "related_dimensions": dims,
            "hypothesis": hypothesis,
            "success_criteria": f"{metric}가 baseline 대비 유의미하게 개선",
            "expected_uplift": action_card.get("expected_impact_range", "+3~8%"),
            "risk_factors": [action_card.get("risk_level", "Medium"), "데이터 계절성", "채널 믹스 변화"],
            "execution_checklist": checklist,
        }

    def evaluate_experiment(
        self,
        baseline_value: float,
        current_value: float,
        expected_direction: str,
        side_effect_metric_changes: Optional[Dict[str, float]] = None,
    ) -> Dict[str, Any]:
        baseline = float(baseline_value or 0.0)
        current = float(current_value or 0.0)
        uplift_pct = _safe_pct(current, baseline)

        success = False
        if expected_direction == "increase":
            success = current > baseline
        elif expected_direction == "decrease":
            success = current < baseline
        else:  # maintain_or_increase
            success = current >= baseline

        side_effect = None
        side_effect_metric_changes = side_effect_metric_changes or {}
        for k, v in side_effect_metric_changes.items():
            if float(v) <= -5.0:
                side_effect = f"{k} {float(v):+.1f}%"
                break

        return {
            "uplift_pct": uplift_pct,
            "side_effect": side_effect,
            "success": bool(success),
        }
