# state_policy.py
import copy

def apply_relation_policy(last_state: dict, relation: str) -> dict:
    """
    relation 결과에 따라 last_state 상속 여부를 결정한다.
    """
    last_state = last_state or {}
    new_last = copy.deepcopy(last_state)

    # 기본값
    inherit_metrics = True
    inherit_dimensions = True
    inherit_dates = True

    if relation == "refine":
        inherit_metrics = True
        inherit_dimensions = True

    elif relation == "metric_switch":
        inherit_metrics = False
        inherit_dimensions = True

    elif relation == "dimension_switch":
        inherit_metrics = True
        inherit_dimensions = False

    elif relation == "new_topic":
        inherit_metrics = False
        inherit_dimensions = False

    if not inherit_metrics:
        new_last.pop("metrics", None)

    if not inherit_dimensions:
        new_last.pop("dimensions", None)

    if not inherit_dates:
        new_last.pop("start_date", None)
        new_last.pop("end_date", None)
        new_last.pop("periods", None)

    return new_last
