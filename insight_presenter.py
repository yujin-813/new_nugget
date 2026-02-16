import math

DISPLAY_NAME_MAP = {
    "purchaseRevenue": "총 매출",
    "itemRevenue": "상품 매출",
    "itemsPurchased": "판매 수량",
    "itemName": "상품명",
    "newVsReturning": "신규/재방문",
}

METRIC_FORMAT_MAP = {
    "purchaseRevenue": "currency",
    "itemRevenue": "currency",
    "totalRevenue": "currency",
    "itemsPurchased": "count",
}

def format_currency_krw(value):
    if value is None:
        return None
    try:
        return f"{round(float(value)):,}원"
    except:
        return str(value)

def format_count(value):
    if value is None:
        return None
    try:
        return f"{int(float(value)):,}개"
    except:
        return str(value)

def format_default(value):
    if value is None:
        return None
    try:
        v = float(value)
        if math.isfinite(v):
            if abs(v) >= 1000:
                return f"{round(v):,}"
            return str(round(v, 2))
    except:
        pass
    return str(value)

def format_value(metric_key, value):
    fmt = METRIC_FORMAT_MAP.get(metric_key)

    if fmt == "currency":
        return format_currency_krw(value)
    if fmt == "count":
        return format_count(value)

    return format_default(value)

def display_name(key):
    return DISPLAY_NAME_MAP.get(key, key)

def present_structured_insight(structured):
    if not structured:
        return None

    # main_metric, delta는 이미 string이라면 그대로 유지
    # all_metrics 내부는 numeric이라 formatting 적용
    all_metrics = structured.get("all_metrics", {})

    presented_metrics = {}
    for metric_key, metric_data in all_metrics.items():
        presented_metrics[metric_key] = {
            "label": display_name(metric_key),
            "current": format_value(metric_key, metric_data.get("current")),
            "previous": format_value(metric_key, metric_data.get("previous")),
            "diff": format_value(metric_key, metric_data.get("diff")),
            "growth": metric_data.get("growth"),
        }

    structured["all_metrics_presented"] = presented_metrics
    return structured

def present_raw_data(raw_data):
    """raw_data 내부 key를 display_name으로 바꾸고 값도 포맷 적용"""
    if not raw_data:
        return raw_data

    presented = []
    for row in raw_data:
        new_row = {}
        for k, v in row.items():
            # dimension은 그냥 문자열, metric은 포맷 적용
            if k in METRIC_FORMAT_MAP:
                new_row[display_name(k)] = format_value(k, v)
            else:
                new_row[display_name(k)] = v
        presented.append(new_row)

    return presented
