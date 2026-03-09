"""T227 - 上下文规则与联合索引优化"""

from typing import Any, Optional

_context_rules: list[dict[str, Any]] = []


async def create_rule(
    tenant_id: str,
    rule_name: str,
    condition: dict[str, Any],
    action: dict[str, Any],
    priority: int = 0,
) -> dict[str, Any]:
    """创建上下文规则"""
    rule = {
        "id": f"cr-{len(_context_rules) + 1:04d}",
        "tenant_id": tenant_id,
        "rule_name": rule_name,
        "condition": condition,
        "action": action,
        "priority": priority,
        "enabled": True,
    }
    _context_rules.append(rule)
    return rule


async def list_rules(tenant_id: str) -> list[dict[str, Any]]:
    """列出上下文规则"""
    return [r for r in _context_rules if r["tenant_id"] == tenant_id]


async def evaluate_rules(
    tenant_id: str, context: dict[str, Any]
) -> list[dict[str, Any]]:
    """评估上下文规则"""
    matched = []
    rules = await list_rules(tenant_id)
    for rule in sorted(rules, key=lambda r: r["priority"], reverse=True):
        if rule["enabled"]:
            # 简单匹配：检查条件键是否在上下文中
            if all(k in context for k in rule["condition"]):
                matched.append(rule)
    return matched


async def toggle_rule(tenant_id: str, rule_id: str, enabled: bool) -> dict[str, Any]:
    """启用/禁用规则"""
    for r in _context_rules:
        if r["id"] == rule_id and r["tenant_id"] == tenant_id:
            r["enabled"] = enabled
            return r
    return {"error": "rule not found"}
