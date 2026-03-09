"""T254 - 策略/交易状态机守卫与回滚"""

from typing import Any, Optional

# 策略状态机
STRATEGY_STATES = {
    "draft": ["submitted", "deleted"],
    "submitted": ["running", "rejected", "draft"],
    "running": ["paused", "completed", "failed"],
    "paused": ["running", "stopped"],
    "completed": ["draft"],
    "failed": ["draft"],
    "rejected": ["draft"],
    "stopped": ["draft"],
    "deleted": [],
}

# 订单状态机
ORDER_STATES = {
    "pending": ["submitted", "cancelled"],
    "submitted": ["partial_filled", "filled", "rejected", "cancelled"],
    "partial_filled": ["filled", "cancelled"],
    "filled": [],
    "rejected": [],
    "cancelled": [],
}


def validate_transition(
    state_machine: dict[str, list[str]],
    current_state: str,
    target_state: str,
) -> dict[str, Any]:
    """校验状态转换是否合法"""
    if current_state not in state_machine:
        return {"valid": False, "reason": f"unknown state: {current_state}"}
    allowed = state_machine[current_state]
    if target_state not in allowed:
        return {
            "valid": False,
            "reason": f"transition from '{current_state}' to '{target_state}' not allowed. Allowed: {allowed}",
        }
    return {"valid": True, "from": current_state, "to": target_state}


async def validate_strategy_transition(
    current: str, target: str
) -> dict[str, Any]:
    """校验策略状态转换"""
    return validate_transition(STRATEGY_STATES, current, target)


async def validate_order_transition(
    current: str, target: str
) -> dict[str, Any]:
    """校验订单状态转换"""
    return validate_transition(ORDER_STATES, current, target)


async def get_available_transitions(
    entity_type: str, current_state: str
) -> list[str]:
    """获取可用转换"""
    machine = STRATEGY_STATES if entity_type == "strategy" else ORDER_STATES
    return machine.get(current_state, [])


# 状态快照用于回滚
_state_snapshots: list[dict[str, Any]] = []


async def save_state_snapshot(
    entity_type: str,
    entity_id: str,
    state: str,
    data: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """保存状态快照"""
    from datetime import datetime

    snapshot = {
        "entity_type": entity_type,
        "entity_id": entity_id,
        "state": state,
        "data": data or {},
        "created_at": datetime.utcnow().isoformat(),
    }
    _state_snapshots.append(snapshot)
    return snapshot


async def rollback_state(
    entity_type: str, entity_id: str
) -> Optional[dict[str, Any]]:
    """回滚到上一个状态"""
    snapshots = [
        s for s in _state_snapshots
        if s["entity_type"] == entity_type and s["entity_id"] == entity_id
    ]
    if len(snapshots) >= 2:
        return snapshots[-2]
    return None
