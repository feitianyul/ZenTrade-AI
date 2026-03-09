"""T243 - 社区可见性与敏感信息脱敏"""

from typing import Any, Optional

# 敏感字段脱敏规则
SENSITIVE_FIELDS = ["phone", "email", "id_card", "real_name", "account_balance"]
VISIBILITY_LEVELS = ["public", "friends_only", "private"]


async def apply_visibility_filter(
    post_data: dict[str, Any],
    viewer_id: str,
    author_id: str,
    viewer_relation: str = "stranger",
    post_visibility: str = "public",
) -> dict[str, Any]:
    """根据可见性规则过滤帖子内容"""
    if post_visibility == "private" and viewer_id != author_id:
        return {"visible": False, "reason": "private_post"}
    if post_visibility == "friends_only" and viewer_relation == "stranger":
        return {"visible": False, "reason": "friends_only"}
    # 脱敏敏感信息
    filtered = dict(post_data)
    for field in SENSITIVE_FIELDS:
        if field in filtered:
            filtered[field] = _mask_field(field, str(filtered[field]))
    return {"visible": True, "data": filtered}


async def set_post_visibility(
    tenant_id: str,
    post_id: str,
    visibility: str,
) -> dict[str, Any]:
    """设置帖子可见性"""
    if visibility not in VISIBILITY_LEVELS:
        return {"error": f"invalid visibility: {visibility}"}
    return {"post_id": post_id, "visibility": visibility, "updated": True}


async def mask_community_content(
    content: str,
) -> str:
    """脱敏社区内容中的敏感信息"""
    import re
    # 手机号脱敏
    content = re.sub(r'1[3-9]\d{9}', lambda m: m.group()[:3] + '****' + m.group()[-4:], content)
    # 邮箱脱敏
    content = re.sub(r'(\w{2})\w+(@\w+)', r'\1****\2', content)
    return content


def _mask_field(field: str, value: str) -> str:
    if field == "phone" and len(value) >= 7:
        return value[:3] + "****" + value[-4:]
    if field == "email" and "@" in value:
        parts = value.split("@")
        return parts[0][:2] + "****@" + parts[1]
    if field in ("id_card", "real_name", "account_balance"):
        return "***"
    return value
