"""触发条件管理与低频调度服务。

FR-028: 通俗化触发条件配置（预设10+常用条件，支持组合逻辑）
FR-029: 低频触发时机控制（日级/周级）
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# 触发条件类型
# ---------------------------------------------------------------------------

class TriggerType(str, Enum):
    """预设触发条件类型（FR-028：预设10+常用条件）。"""

    PRICE_BELOW_COST = "price_below_cost"           # 股价跌破成本价
    PRICE_ABOVE_HIGH = "price_above_high"           # 股价突破前期高点
    PRICE_BELOW_LOW = "price_below_low"             # 股价跌破前期低点
    STOP_LOSS_PERCENT = "stop_loss_percent"         # 跌破成本价X%止损
    TAKE_PROFIT_PERCENT = "take_profit_percent"     # 持仓收益达到X%止盈
    MA_CROSS_UP = "ma_cross_up"                     # 均线金叉
    MA_CROSS_DOWN = "ma_cross_down"                 # 均线死叉
    VOLUME_SURGE = "volume_surge"                   # 成交量异常放大
    MACD_GOLDEN = "macd_golden"                     # MACD金叉
    MACD_DEAD = "macd_dead"                         # MACD死叉
    KDJ_OVERSOLD = "kdj_oversold"                   # KDJ超卖
    RSI_OVERBOUGHT = "rsi_overbought"               # RSI超买


# 预设条件的通俗化描述
TRIGGER_DESCRIPTIONS: Dict[str, Dict[str, str]] = {
    TriggerType.PRICE_BELOW_COST: {
        "label": "股价跌破成本价",
        "description": "当股价低于你的买入成本价时触发",
        "category": "止损类",
    },
    TriggerType.PRICE_ABOVE_HIGH: {
        "label": "股价突破前期高点",
        "description": "当股价超过最近一段时间的最高价时触发",
        "category": "突破类",
    },
    TriggerType.PRICE_BELOW_LOW: {
        "label": "股价跌破前期低点",
        "description": "当股价低于最近一段时间的最低价时触发",
        "category": "止损类",
    },
    TriggerType.STOP_LOSS_PERCENT: {
        "label": "跌破成本价止损",
        "description": "当股价跌破你的买入成本价一定比例时止损",
        "category": "止损类",
        "default_params": '{"percent": 8}',
    },
    TriggerType.TAKE_PROFIT_PERCENT: {
        "label": "持仓收益止盈",
        "description": "当你的持仓收益达到一定比例时止盈卖出",
        "category": "止盈类",
        "default_params": '{"percent": 10}',
    },
    TriggerType.MA_CROSS_UP: {
        "label": "均线金叉买入",
        "description": "当短期均线从下方穿过长期均线时触发买入信号",
        "category": "技术类",
    },
    TriggerType.MA_CROSS_DOWN: {
        "label": "均线死叉卖出",
        "description": "当短期均线从上方穿过长期均线时触发卖出信号",
        "category": "技术类",
    },
    TriggerType.VOLUME_SURGE: {
        "label": "成交量放大",
        "description": "当成交量超过近期平均的一定倍数时触发",
        "category": "量价类",
    },
    TriggerType.MACD_GOLDEN: {
        "label": "MACD金叉",
        "description": "MACD指标出现金叉时触发买入信号",
        "category": "技术类",
    },
    TriggerType.MACD_DEAD: {
        "label": "MACD死叉",
        "description": "MACD指标出现死叉时触发卖出信号",
        "category": "技术类",
    },
    TriggerType.KDJ_OVERSOLD: {
        "label": "KDJ超卖",
        "description": "KDJ指标进入超卖区域时触发（可能反弹买入）",
        "category": "技术类",
    },
    TriggerType.RSI_OVERBOUGHT: {
        "label": "RSI超买",
        "description": "RSI指标进入超买区域时触发（可能回调卖出）",
        "category": "技术类",
    },
}


# 新手推荐触发条件
NOVICE_RECOMMENDED: List[str] = [
    TriggerType.STOP_LOSS_PERCENT,
    TriggerType.TAKE_PROFIT_PERCENT,
    TriggerType.PRICE_BELOW_COST,
    TriggerType.MA_CROSS_UP,
    TriggerType.MA_CROSS_DOWN,
]


# ---------------------------------------------------------------------------
# 组合逻辑
# ---------------------------------------------------------------------------

class CombineLogic(str, Enum):
    AND = "and"  # 且：所有条件同时满足
    OR = "or"    # 或：任一条件满足


# ---------------------------------------------------------------------------
# 触发频率
# ---------------------------------------------------------------------------

class TriggerFrequency(str, Enum):
    """低频触发时机（FR-029）。"""

    DAILY = "daily"    # 日级：收盘后10分钟执行
    WEEKLY = "weekly"  # 周级：周五收盘后10分钟执行


FREQUENCY_DESCRIPTIONS: Dict[str, str] = {
    TriggerFrequency.DAILY: "每个交易日收盘后10分钟（约15:10）执行",
    TriggerFrequency.WEEKLY: "每周五收盘后10分钟（约15:10）执行",
}


# ---------------------------------------------------------------------------
# 触发条件定义
# ---------------------------------------------------------------------------

@dataclass
class TriggerCondition:
    """单个触发条件。"""

    trigger_type: str
    params: Dict[str, Any] = field(default_factory=dict)
    condition_id: str = field(default_factory=lambda: f"cond_{uuid.uuid4().hex[:8]}")


@dataclass
class TriggerRule:
    """触发规则（支持组合条件）。"""

    trigger_id: str = field(default_factory=lambda: f"trg_{uuid.uuid4().hex[:8]}")
    strategy_id: str = ""
    symbol: str = ""
    conditions: List[TriggerCondition] = field(default_factory=list)
    combine_logic: str = CombineLogic.AND.value
    frequency: str = TriggerFrequency.DAILY.value
    expire_at: Optional[str] = None  # 条件有效期（ISO datetime 或 None=永不过期）
    notify_channels: List[str] = field(
        default_factory=lambda: ["app"],
    )  # APP/微信/短信
    active: bool = True


# ---------------------------------------------------------------------------
# 服务函数
# ---------------------------------------------------------------------------

async def list_preset_triggers(novice_only: bool = False) -> List[Dict[str, Any]]:
    """列出预设触发条件（新手推荐板块）。"""
    if novice_only:
        return [
            {"type": t, **TRIGGER_DESCRIPTIONS.get(t, {})}
            for t in NOVICE_RECOMMENDED
        ]
    return [
        {"type": k, **v}
        for k, v in TRIGGER_DESCRIPTIONS.items()
    ]


async def build_trigger(payload: Dict[str, Any]) -> TriggerRule:
    """根据用户输入构建触发规则。"""
    conditions = []
    for c in payload.get("conditions", []):
        conditions.append(
            TriggerCondition(
                trigger_type=c.get("type", ""),
                params=c.get("params", {}),
            )
        )

    return TriggerRule(
        strategy_id=payload.get("strategy_id", ""),
        symbol=payload.get("symbol", ""),
        conditions=conditions,
        combine_logic=payload.get("combine_logic", CombineLogic.AND.value),
        frequency=payload.get("frequency", TriggerFrequency.DAILY.value),
        expire_at=payload.get("expire_at"),
        notify_channels=payload.get("notify_channels", ["app"]),
    )


async def evaluate_trigger(
    rule: TriggerRule,
    market_snapshot: Dict[str, Any],
) -> Dict[str, Any]:
    """评估触发规则是否满足。

    Args:
        rule: 触发规则
        market_snapshot: 当前行情快照

    Returns:
        评估结果，包含 triggered (bool) 和 detail
    """
    results: List[bool] = []
    details: List[Dict[str, Any]] = []

    for cond in rule.conditions:
        triggered, detail = _evaluate_single(cond, market_snapshot)
        results.append(triggered)
        details.append({"condition": cond.trigger_type, "triggered": triggered, **detail})

    if rule.combine_logic == CombineLogic.AND.value:
        overall = all(results) if results else False
    else:
        overall = any(results) if results else False

    return {
        "trigger_id": rule.trigger_id,
        "triggered": overall,
        "combine_logic": rule.combine_logic,
        "details": details,
    }


def _evaluate_single(
    cond: TriggerCondition,
    snapshot: Dict[str, Any],
) -> tuple[bool, Dict[str, Any]]:
    """评估单个触发条件。"""
    current_price = snapshot.get("price", 0.0)
    cost_price = snapshot.get("cost_price", 0.0)

    if cond.trigger_type == TriggerType.PRICE_BELOW_COST:
        triggered = current_price < cost_price if cost_price > 0 else False
        return triggered, {"current": current_price, "cost": cost_price}

    if cond.trigger_type == TriggerType.STOP_LOSS_PERCENT:
        pct = cond.params.get("percent", 8) / 100
        threshold = cost_price * (1 - pct)
        triggered = current_price < threshold if cost_price > 0 else False
        return triggered, {"current": current_price, "threshold": round(threshold, 2)}

    if cond.trigger_type == TriggerType.TAKE_PROFIT_PERCENT:
        pct = cond.params.get("percent", 10) / 100
        threshold = cost_price * (1 + pct)
        triggered = current_price > threshold if cost_price > 0 else False
        return triggered, {"current": current_price, "threshold": round(threshold, 2)}

    if cond.trigger_type == TriggerType.PRICE_ABOVE_HIGH:
        high = snapshot.get("period_high", 0.0)
        triggered = current_price > high if high > 0 else False
        return triggered, {"current": current_price, "period_high": high}

    if cond.trigger_type == TriggerType.PRICE_BELOW_LOW:
        low = snapshot.get("period_low", 0.0)
        triggered = current_price < low if low > 0 else False
        return triggered, {"current": current_price, "period_low": low}

    # 其他技术指标条件 - 需外部数据支持
    return False, {"reason": f"条件 {cond.trigger_type} 需要额外的技术指标数据"}


def format_trigger_failure(
    rule: TriggerRule,
    eval_result: Dict[str, Any],
) -> str:
    """生成通俗化的触发失败提醒（FR-028）。

    Returns:
        面向用户的通俗化提醒文本。
    """
    parts: List[str] = ["你的策略未触发："]
    for detail in eval_result.get("details", []):
        cond_type = detail.get("condition", "")
        desc = TRIGGER_DESCRIPTIONS.get(cond_type, {})
        label = desc.get("label", cond_type)

        if cond_type == TriggerType.STOP_LOSS_PERCENT:
            threshold = detail.get("threshold", "N/A")
            parts.append(f"股价没跌到你设置的{threshold}元止损价哦")
        elif cond_type == TriggerType.TAKE_PROFIT_PERCENT:
            threshold = detail.get("threshold", "N/A")
            parts.append(f"股价没涨到你设置的{threshold}元止盈价哦")
        elif cond_type == TriggerType.PRICE_BELOW_COST:
            cost = detail.get("cost", "N/A")
            parts.append(f"股价没跌到你设置的{cost}元成本价哦")
        else:
            parts.append(f"「{label}」条件暂未达到")

    return "".join(parts)
