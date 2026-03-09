"""数据动态脱敏服务。

支持按字段类型自动脱敏，覆盖以下场景：
  - 手机号：隐藏中间4位（138****1234）
  - 银行卡号：隐藏中间4位（6225****5678）
  - 邮箱：隐藏局部（ab***@example.com）
  - 身份证：隐藏中间（110101****1234）
  - 姓名：隐藏中间字符（张*三）
  - 地址：隐藏详细部分
  - 交易金额：显示区间（如 1000-5000 元）
"""

from __future__ import annotations

import math
from typing import Any, Dict, List

from src.schemas.masking import MaskingRule


# ---------------------------------------------------------------------------
# 基础脱敏函数
# ---------------------------------------------------------------------------

def mask_phone(value: str, char: str = "*") -> str:
    """手机号脱敏：隐藏中间4位。例 138****1234"""
    if not value or len(value) < 7:
        return value
    return value[:3] + char * 4 + value[-4:]


def mask_bank_card(value: str, char: str = "*") -> str:
    """银行卡号脱敏：隐藏中间4位。例 6225****5678"""
    if not value or len(value) < 8:
        return value
    return value[:4] + char * 4 + value[-4:]


def mask_email(value: str, char: str = "*") -> str:
    """邮箱脱敏。例 ab***@example.com"""
    if not value or "@" not in value:
        return value
    local, domain = value.split("@", 1)
    if len(local) <= 2:
        return f"{char * len(local)}@{domain}"
    return f"{local[:2]}{char * (len(local) - 2)}@{domain}"


def mask_id_card(value: str, char: str = "*") -> str:
    """身份证脱敏。例 110101****1234"""
    if not value or len(value) < 10:
        return value
    return value[:6] + char * (len(value) - 10) + value[-4:]


def mask_name(value: str, char: str = "*") -> str:
    """姓名脱敏。例 张*三"""
    if not value:
        return value
    if len(value) == 2:
        return value[0] + char
    return value[0] + char * (len(value) - 2) + value[-1]


def mask_address(value: str, char: str = "*") -> str:
    """地址脱敏：隐藏详细部分。"""
    if not value or len(value) < 6:
        return value
    return value[:6] + char * 6


def mask_trade_amount(value: str | float | int, **_kwargs: Any) -> str:
    """交易金额区间脱敏：隐藏精确金额，仅展示区间。

    规则：将金额归入最近的区间段。
    例：3500 → "1000-5000元"  |  12345 → "10000-50000元"

    区间定义：
      0-100, 100-500, 500-1000, 1000-5000, 5000-10000,
      10000-50000, 50000-100000, 100000-500000, 500000+
    """
    try:
        amount = float(value)
    except (ValueError, TypeError):
        return str(value)

    boundaries = [0, 100, 500, 1000, 5000, 10000, 50000, 100000, 500000]

    if amount < 0:
        return "负值"

    for i in range(len(boundaries) - 1):
        if boundaries[i] <= amount < boundaries[i + 1]:
            return f"{boundaries[i]}-{boundaries[i + 1]}元"

    return f"{boundaries[-1]}元以上"


# ---------------------------------------------------------------------------
# 脱敏函数注册表
# ---------------------------------------------------------------------------

_MASKERS = {
    "phone": mask_phone,
    "bank_card": mask_bank_card,
    "email": mask_email,
    "id_card": mask_id_card,
    "name": mask_name,
    "address": mask_address,
    "trade_amount": mask_trade_amount,
}


# ---------------------------------------------------------------------------
# 统一应用脱敏
# ---------------------------------------------------------------------------

def apply_masking(data: Dict[str, Any], rules: List[MaskingRule]) -> Dict[str, Any]:
    """对数据字典按规则逐字段应用脱敏。"""
    masked_data = data.copy()
    for rule in rules:
        if rule.field not in masked_data:
            continue
        raw = masked_data[rule.field]
        masker = _MASKERS.get(rule.type)
        if masker:
            if rule.type == "trade_amount":
                masked_data[rule.field] = masker(raw)
            else:
                masked_data[rule.field] = masker(str(raw), rule.mask_char)
        # 未知类型保持原值
    return masked_data


def apply_role_masking(
    data: Dict[str, Any],
    rules: List[MaskingRule],
    user_role: str,
) -> Dict[str, Any]:
    """按用户角色动态脱敏：管理员看全量，普通用户看脱敏版。"""
    if user_role == "admin":
        # 管理员：管理员不可查看用户交易记录（PRD规定）
        # 因此管理员也需要对交易相关字段脱敏
        trade_rules = [r for r in rules if r.type == "trade_amount"]
        if trade_rules:
            return apply_masking(data, trade_rules)
        return data
    return apply_masking(data, rules)
