"""交易模式管理与全自动准入校验服务。

支持三种交易模式：
  - manual（纯手动）：用户完全手动执行交易
  - semi_auto（半自动）：系统触发信号，用户确认后执行
  - full_auto（全自动）：系统自动执行，需满足准入条件

FR-027: 交易模式切换（切换需二次验证）
VR-016: 全自动模式准入条件
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# 模式枚举
# ---------------------------------------------------------------------------

class TradeMode(str, Enum):
    MANUAL = "manual"
    SEMI_AUTO = "semi_auto"
    FULL_AUTO = "full_auto"


VALID_MODES = {m.value for m in TradeMode}


# ---------------------------------------------------------------------------
# 用户等级枚举
# ---------------------------------------------------------------------------

class UserLevel(str, Enum):
    NOVICE = "novice"       # 新手
    ADVANCED = "advanced"   # 进阶
    SENIOR = "senior"       # 资深


# ---------------------------------------------------------------------------
# 准入条件配置
# ---------------------------------------------------------------------------

@dataclass
class FullAutoAdmission:
    """全自动模式准入条件（FR-027）。"""

    min_win_rate: float = 0.70            # 策略胜率≥70%
    max_drawdown_novice: float = 0.20     # 新手最大回撤≤20%
    max_drawdown_advanced: float = 0.25   # 进阶最大回撤≤25%
    max_drawdown_senior: float = 0.25     # 资深同进阶
    min_backtest_trades: int = 20         # 回测交易次数≥20次
    min_sim_experience_days: int = 90     # 需3个月(90天)模拟交易经验
    max_single_trade_ratio: float = 0.30  # 单次交易金额上限30%
    allowed_risk_levels: List[str] = field(
        default_factory=lambda: ["conservative", "moderate"],
    )  # 仅低/中风险账户可开启


DEFAULT_ADMISSION = FullAutoAdmission()


# ---------------------------------------------------------------------------
# 校验结果
# ---------------------------------------------------------------------------

@dataclass
class AdmissionResult:
    """准入校验结果。"""

    passed: bool
    violations: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# 模式列表与基础校验
# ---------------------------------------------------------------------------

async def list_trade_modes(user_level: str | None = None) -> List[str]:
    """根据用户等级返回可用交易模式。

    - basic (初级散户): 仅 manual
    - intermediate (进阶散户): manual + semi_auto
    - advanced (资深散户): 全部模式
    - 未指定等级时返回全部（向后兼容）
    """
    if user_level is None:
        return [m.value for m in TradeMode]

    level_modes = {
        "basic": [TradeMode.MANUAL.value],
        "intermediate": [TradeMode.MANUAL.value, TradeMode.SEMI_AUTO.value],
        "advanced": [m.value for m in TradeMode],
    }
    return level_modes.get(user_level, [TradeMode.MANUAL.value])


async def validate_trade_mode(mode: str) -> bool:
    """校验模式名称是否合法。"""
    return mode in VALID_MODES


# ---------------------------------------------------------------------------
# 全自动准入校验
# ---------------------------------------------------------------------------

async def check_full_auto_admission(
    user_level: str,
    risk_level: str,
    strategy_win_rate: float,
    strategy_max_drawdown: float,
    backtest_trade_count: int,
    sim_experience_days: int,
    admission: FullAutoAdmission | None = None,
) -> AdmissionResult:
    """校验用户是否满足全自动模式开启条件（VR-016）。

    Returns:
        AdmissionResult，passed=True 时可开启全自动，否则 violations 包含原因。
    """
    cfg = admission or DEFAULT_ADMISSION
    violations: List[str] = []

    # 风险等级
    if risk_level not in cfg.allowed_risk_levels:
        violations.append(
            f"不满足全自动模式开启条件：当前风险等级为「{risk_level}」，"
            "仅低/中风险账户可开启全自动模式"
        )

    # 策略胜率
    if strategy_win_rate < cfg.min_win_rate:
        violations.append(
            f"不满足全自动模式开启条件：策略胜率为{strategy_win_rate:.0%}，"
            f"要求≥{cfg.min_win_rate:.0%}"
        )

    # 最大回撤（按用户等级区分阈值）
    max_dd = cfg.max_drawdown_novice
    if user_level == UserLevel.ADVANCED.value:
        max_dd = cfg.max_drawdown_advanced
    elif user_level == UserLevel.SENIOR.value:
        max_dd = cfg.max_drawdown_senior

    if strategy_max_drawdown > max_dd:
        violations.append(
            f"不满足全自动模式开启条件：策略最大回撤为{strategy_max_drawdown:.0%}，"
            f"当前等级要求≤{max_dd:.0%}"
        )

    # 回测交易次数
    if backtest_trade_count < cfg.min_backtest_trades:
        violations.append(
            f"不满足全自动模式开启条件：回测交易次数为{backtest_trade_count}次，"
            f"要求≥{cfg.min_backtest_trades}次"
        )

    # 模拟交易经验
    if sim_experience_days < cfg.min_sim_experience_days:
        violations.append(
            f"不满足全自动模式开启条件：模拟交易经验为{sim_experience_days}天，"
            f"要求≥{cfg.min_sim_experience_days}天（约3个月）"
        )

    return AdmissionResult(passed=len(violations) == 0, violations=violations)


# ---------------------------------------------------------------------------
# 单次交易金额校验
# ---------------------------------------------------------------------------

async def validate_single_trade_amount(
    trade_amount: float,
    total_capital: float,
    max_ratio: float | None = None,
) -> Dict[str, Any]:
    """校验单次交易金额是否超过总资金上限比例（FR-027）。"""
    ratio = max_ratio or DEFAULT_ADMISSION.max_single_trade_ratio
    actual_ratio = trade_amount / total_capital if total_capital > 0 else 1.0
    passed = actual_ratio <= ratio
    return {
        "passed": passed,
        "actual_ratio": round(actual_ratio, 4),
        "max_ratio": ratio,
        "message": (
            None
            if passed
            else f"单次交易金额占比{actual_ratio:.0%}超过上限{ratio:.0%}"
        ),
    }


# ---------------------------------------------------------------------------
# 模式切换（需二次验证）
# ---------------------------------------------------------------------------

async def request_mode_switch(
    current_mode: str,
    target_mode: str,
    mfa_verified: bool,
) -> Dict[str, Any]:
    """请求切换交易模式，切换需二次验证（FR-027）。

    Args:
        current_mode: 当前模式
        target_mode: 目标模式
        mfa_verified: 是否已完成二次验证

    Returns:
        切换结果字典
    """
    if target_mode not in VALID_MODES:
        return {"allowed": False, "reason": f"无效的交易模式: {target_mode}"}

    if current_mode == target_mode:
        return {"allowed": False, "reason": "目标模式与当前模式相同，无需切换"}

    if not mfa_verified:
        return {
            "allowed": False,
            "reason": "交易模式切换需要二次验证，请先完成短信/邮件/微信验证",
            "require_mfa": True,
        }

    # 切换到全自动需额外准入校验（调用方应先调用 check_full_auto_admission）
    if target_mode == TradeMode.FULL_AUTO.value:
        return {
            "allowed": True,
            "reason": "已通过二次验证，请确认已满足全自动准入条件",
            "require_admission_check": True,
        }

    return {"allowed": True, "reason": f"模式切换成功: {current_mode} → {target_mode}"}
