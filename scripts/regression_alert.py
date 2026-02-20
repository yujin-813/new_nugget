#!/usr/bin/env python3
import argparse
import json
import os
import sys
from typing import Dict, Any

import requests


def fetch_snapshot(base_url: str, admin_token: str, days: int, limit: int, user_id: str = None) -> Dict[str, Any]:
    params = {"days": days, "limit": limit}
    if user_id:
        params["user_id"] = user_id
    headers = {"X-Admin-Token": admin_token}
    url = f"{base_url.rstrip('/')}/admin/regression_snapshot"
    r = requests.get(url, params=params, headers=headers, timeout=20)
    r.raise_for_status()
    data = r.json()
    if not data.get("success"):
        raise RuntimeError(f"snapshot fetch failed: {data}")
    return data.get("snapshot") or {}


def build_summary(snapshot: Dict[str, Any], bad_rate_threshold: float) -> str:
    total = int(snapshot.get("total", 0) or 0)
    good_rate = float(snapshot.get("good_rate", 0.0) or 0.0)
    bad_rate = float(snapshot.get("bad_rate", 0.0) or 0.0)
    routes = snapshot.get("route_summary") or []
    top_bad = snapshot.get("top_bad_questions") or []

    lines = []
    lines.append("회귀 점검 결과")
    lines.append(f"- 전체: {total}")
    lines.append(f"- Good Rate: {good_rate:.2f}%")
    lines.append(f"- Bad Rate: {bad_rate:.2f}% (임계치 {bad_rate_threshold:.2f}%)")
    if routes:
        lines.append("- Route별 Bad Rate:")
        for r in routes[:5]:
            lines.append(f"  - {r.get('route')}: {r.get('bad_rate', 0)}% ({r.get('bad', 0)}/{r.get('total', 0)})")
    if top_bad:
        lines.append("- 상위 Bad 질문:")
        for item in top_bad[:5]:
            lines.append(f"  - {item.get('question')} ({item.get('count')})")
    return "\n".join(lines)


def parse_route_thresholds(raw: str) -> Dict[str, float]:
    """
    Format:
      "file=30,ga4=15,ga4_followup=20"
    """
    out: Dict[str, float] = {}
    txt = str(raw or "").strip()
    if not txt:
        return out
    for part in txt.split(","):
        p = part.strip()
        if not p or "=" not in p:
            continue
        k, v = p.split("=", 1)
        key = k.strip()
        try:
            out[key] = float(v.strip())
        except Exception:
            continue
    return out


def evaluate_route_thresholds(snapshot: Dict[str, Any], route_thresholds: Dict[str, float]):
    exceeded = []
    route_rows = snapshot.get("route_summary") or []
    route_map = {str(r.get("route")): float(r.get("bad_rate", 0.0) or 0.0) for r in route_rows}
    for route, th in (route_thresholds or {}).items():
        val = float(route_map.get(route, 0.0))
        if val >= float(th):
            exceeded.append({"route": route, "bad_rate": val, "threshold": float(th)})
    return exceeded


def send_jandi(webhook_url: str, text: str):
    payload = {"body": text, "connectColor": "#ff3b30"}
    r = requests.post(webhook_url, json=payload, timeout=12)
    r.raise_for_status()


def main():
    ap = argparse.ArgumentParser(description="Fetch regression snapshot and alert on threshold")
    ap.add_argument("--base-url", default=os.getenv("APP_BASE_URL", "").strip())
    ap.add_argument("--admin-token", default=os.getenv("ADMIN_API_TOKEN", "").strip())
    ap.add_argument("--jandi-webhook", default=os.getenv("JANDI_WEBHOOK_URL", "").strip())
    ap.add_argument("--days", type=int, default=int(os.getenv("REGRESSION_DAYS", "14")))
    ap.add_argument("--limit", type=int, default=int(os.getenv("REGRESSION_LIMIT", "100")))
    ap.add_argument("--bad-rate-threshold", type=float, default=float(os.getenv("REGRESSION_BAD_RATE_THRESHOLD", "20")))
    ap.add_argument("--route-thresholds", default=os.getenv("REGRESSION_ROUTE_THRESHOLDS", "").strip())
    ap.add_argument("--user-id", default=os.getenv("REGRESSION_USER_ID", "").strip() or None)
    ap.add_argument("--always-notify", action="store_true")
    args = ap.parse_args()

    if not args.base_url or not args.admin_token:
        print("base-url/admin-token required", file=sys.stderr)
        return 2

    snapshot = fetch_snapshot(
        base_url=args.base_url,
        admin_token=args.admin_token,
        days=max(1, args.days),
        limit=max(10, args.limit),
        user_id=args.user_id,
    )

    bad_rate = float(snapshot.get("bad_rate", 0.0) or 0.0)
    route_thresholds = parse_route_thresholds(args.route_thresholds)
    route_exceeded = evaluate_route_thresholds(snapshot, route_thresholds)
    over_global = bad_rate >= float(args.bad_rate_threshold)
    over_route = len(route_exceeded) > 0
    over = over_global or over_route
    text = build_summary(snapshot, bad_rate_threshold=float(args.bad_rate_threshold))
    if route_thresholds:
        text += "\n- Route 임계치 설정: " + ", ".join([f"{k}>={v}%" for k, v in route_thresholds.items()])
    if route_exceeded:
        text += "\n- Route 임계치 초과:"
        for r in route_exceeded:
            text += f"\n  - {r['route']}: {r['bad_rate']:.2f}% (threshold {r['threshold']:.2f}%)"

    print(json.dumps({
        "over_threshold": over,
        "over_global": over_global,
        "over_route": over_route,
        "bad_rate": bad_rate,
        "route_exceeded": route_exceeded,
        "summary": text
    }, ensure_ascii=False, indent=2))

    if (over or args.always_notify) and args.jandi_webhook:
        send_jandi(args.jandi_webhook, text)
        print("JANDI notified")

    # CI에서 임계치 초과 시 실패 처리
    return 1 if over else 0


if __name__ == "__main__":
    raise SystemExit(main())
