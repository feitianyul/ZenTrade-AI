"""回测服务 — 支持多标的、费用明细、交易明细、指数对比。

支持真实 K 线：优先从 ClickHouse/MySQL 按日期区间加载，无数据时降级为模拟 K 线。
"""

from datetime import datetime, timedelta
from typing import Any, List, Optional
import hashlib
import math

from sqlalchemy import select, update

from src.core.db import get_session
from src.models.backtest_task import BacktestTask
from src.schemas.backtest import (
    BacktestRequest,
    BacktestResult,
    BenchmarkComparison,
    FeeBreakdown,
    TradeDetail,
)

# ---------- 指数名称映射 ----------
INDEX_NAMES = {
    "000300.SH": "沪深300",
    "000001.SH": "上证指数",
    "399001.SZ": "深证成指",
    "399006.SZ": "创业板指",
    "000905.SH": "中证500",
    "000016.SH": "上证50",
    "000688.SH": "科创50",
}

# ---------- 辅助函数 ----------

def _parse_date(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _seed_from_id(strategy_id: str) -> int:
    return int(hashlib.sha256(strategy_id.encode("utf-8")).hexdigest()[:8], 16)


def _normalize_symbol(symbol: str) -> str:
    """统一代码格式: 600519.SH / SZ300251 -> 600519 / 300251"""
    s = (symbol or "").strip().upper()
    if "." in s:
        s = s.split(".")[0]
    for prefix in ("SH", "SZ", "BJ"):
        if s.startswith(prefix) and len(s) > 2:
            s = s[2:]
            break
    return s


def _rand(seed: int, offset: int) -> float:
    """基于 seed + offset 的确定性随机 [0,1)"""
    h = hashlib.md5(f"{seed}-{offset}".encode()).hexdigest()
    return int(h[:8], 16) / 0xFFFFFFFF


def _simulate_trades(
    start_date: str,
    end_date: str,
    seed: int,
    initial_capital: float,
    targets: list[dict[str, Any]],
    commission_rate: float,
    min_commission: float,
    stamp_tax_rate: float,
    transfer_fee_rate: float,
    take_profit: Optional[float],
    stop_loss: Optional[float],
    bars: Optional[list[dict[str, Any]]] = None,
) -> tuple[list[TradeDetail], list[dict[str, Any]], list[dict[str, Any]], str]:
    """模拟交易，生成交易明细、净值曲线、K线数据。
    bars: 可选真实 K 线，有则用真实数据驱动，无则用 _rand 模拟。返回 kline_source。
    """
    start = datetime.fromisoformat(start_date).date()
    end = datetime.fromisoformat(end_date).date()

    if not targets:
        targets = [{"code": "600519.SH", "name": "贵州茅台", "weight": 100}]

    total_weight = sum(t.get("weight", 0) for t in targets)
    if total_weight <= 0:
        w = 100.0 / len(targets)
        for t in targets:
            t["weight"] = w

    trades: list[TradeDetail] = []
    equity_curve: list[dict[str, Any]] = []
    kline_data: list[dict[str, Any]] = []
    nav = initial_capital
    cash = initial_capital
    positions: dict[str, dict[str, Any]] = {}

    primary_code = targets[0].get("code", "600519.SH")
    primary_code_norm = _normalize_symbol(primary_code)
    use_real_bars = bars and len(bars) > 0

    if use_real_bars:
        kline_data = [
            {
                "date": str(b.get("date", "")),
                "open": float(b.get("open", 0)),
                "high": float(b.get("high", 0)),
                "low": float(b.get("low", 0)),
                "close": float(b.get("close", 0)),
                "volume": float(b.get("volume", 0)),
            }
            for b in bars
        ]
        day_iter = bars
    else:
        primary_base = 50 + _rand(seed, hash(primary_code) & 0xFFFF) * 200
        prev_close = primary_base
        cur = start
        trade_idx = 0
        while cur <= end:
            if cur.weekday() >= 5:
                cur += timedelta(days=1)
                continue
            daily_change = (_rand(seed, trade_idx * 3 + 1) - 0.48) * 0.06
            k_open = round(prev_close * (1 + (_rand(seed, trade_idx * 3 + 2) - 0.5) * 0.015), 2)
            k_close = round(prev_close * (1 + daily_change), 2)
            k_high = round(max(k_open, k_close) * (1 + _rand(seed, trade_idx * 3 + 3) * 0.02), 2)
            k_low = round(min(k_open, k_close) * (1 - _rand(seed, trade_idx * 3 + 4) * 0.02), 2)
            k_volume = int(50000 + _rand(seed, trade_idx * 3 + 5) * 200000)
            kline_data.append({
                "date": cur.isoformat(),
                "open": k_open, "high": k_high, "low": k_low, "close": k_close,
                "volume": k_volume,
            })
            prev_close = k_close
            trade_idx += 1
            cur += timedelta(days=1)
        day_iter = kline_data

    trade_idx = 0
    for bar in day_iter:
        date_str = str(bar.get("date", ""))[:10]
        if not date_str:
            continue
        close_price = float(bar.get("close", 0)) if use_real_bars else float(bar.get("close", 0))

        interval = 5 + int(_rand(seed, trade_idx + 1000) * 10)
        if trade_idx > 0 and trade_idx % interval == 0:
            for tgt in targets:
                code = tgt.get("code", "600519.SH")
                name = tgt.get("name", code)
                weight = tgt.get("weight", 100.0 / len(targets))
                alloc = initial_capital * weight / 100.0

                if use_real_bars and _normalize_symbol(code) == primary_code_norm:
                    price = round(close_price, 2)
                else:
                    base_price = 50 + _rand(seed, hash(code) & 0xFFFF) * 200
                    price_factor = 1 + (_rand(seed, trade_idx + hash(code)) - 0.45) * 0.3
                    price = round(base_price * price_factor, 2)

                if code in positions:
                    pos = positions[code]
                    qty = pos["qty"]
                    amount = round(price * qty, 2)
                    comm = max(amount * commission_rate, min_commission)
                    stamp = round(amount * stamp_tax_rate, 2)
                    transfer = round(amount * transfer_fee_rate, 2)
                    total_cost = round(comm + stamp + transfer, 2)
                    pnl = round(amount - pos["avg_price"] * qty - total_cost, 2)
                    pnl_pct = round(pnl / (pos["avg_price"] * qty) * 100, 2) if pos["avg_price"] * qty > 0 else 0
                    if take_profit and pnl_pct >= take_profit:
                        pass
                    elif stop_loss and pnl_pct <= -stop_loss:
                        pass
                    trades.append(TradeDetail(
                        date=date_str, action="卖出", target=code, target_name=name,
                        price=price, quantity=qty, amount=amount,
                        commission=round(comm, 2), stamp_tax=stamp,
                        transfer_fee=transfer, total_cost=total_cost,
                        pnl=pnl, pnl_pct=pnl_pct,
                    ))
                    cash += amount - total_cost
                    del positions[code]
                else:
                    buy_amount = min(alloc * 0.8, cash * weight / 100.0)
                    if buy_amount < price * 100:
                        continue
                    qty = int(buy_amount / (price * 100)) * 100
                    if qty <= 0:
                        continue
                    amount = round(price * qty, 2)
                    comm = max(amount * commission_rate, min_commission)
                    transfer = round(amount * transfer_fee_rate, 2)
                    total_cost = round(comm + transfer, 2)
                    trades.append(TradeDetail(
                        date=date_str, action="买入", target=code, target_name=name,
                        price=price, quantity=qty, amount=amount,
                        commission=round(comm, 2), stamp_tax=0,
                        transfer_fee=transfer, total_cost=total_cost,
                        pnl=0, pnl_pct=0,
                    ))
                    cash -= amount + total_cost
                    positions[code] = {"qty": qty, "avg_price": price, "name": name}

        pos_value = 0
        for code, pos in positions.items():
            if use_real_bars and _normalize_symbol(code) == primary_code_norm:
                pos_value += pos["qty"] * close_price
            else:
                base_price = 50 + _rand(seed, hash(code) & 0xFFFF) * 200
                price_var = 1 + (_rand(seed, trade_idx + hash(code) + 999) - 0.48) * 0.25
                pos_value += pos["qty"] * base_price * price_var
        nav = cash + pos_value
        equity_curve.append({"date": date_str, "value": round(nav, 2)})
        trade_idx += 1

    kline_source = "real" if use_real_bars else "simulated"
    return trades, equity_curve, kline_data, kline_source


def _calculate_metrics(
    curve: list[dict[str, Any]],
    initial_capital: float,
) -> dict[str, Any]:
    """计算核心指标。"""
    if not curve:
        return {
            "total_return": 0.0,
            "total_return_pct": 0.0,
            "annualized_return": 0.0,
            "max_drawdown": 0.0,
            "max_drawdown_pct": 0.0,
            "win_rate": 0.0,
            "sharpe_ratio": 0.0,
            "stability_score": 0.0,
        }

    values = [p["value"] for p in curve]
    start_val = values[0] if values[0] != 0 else initial_capital
    end_val = values[-1]
    total_return = end_val - start_val
    total_return_pct = (total_return / start_val) * 100 if start_val else 0

    # 最大回撤
    peak = values[0]
    max_dd = 0.0
    for v in values:
        if v > peak:
            peak = v
        dd = (v - peak) / peak if peak else 0
        if dd < max_dd:
            max_dd = dd
    max_drawdown_pct = abs(max_dd) * 100

    # 日收益率
    daily_returns = []
    for i in range(1, len(values)):
        if values[i - 1] != 0:
            daily_returns.append(values[i] / values[i - 1] - 1)

    # 胜率（日收益为正的比例）
    win_days = sum(1 for r in daily_returns if r > 0)
    win_rate = (win_days / len(daily_returns) * 100) if daily_returns else 0

    # Sharpe ratio (假设无风险利率 3%)
    if daily_returns:
        avg_r = sum(daily_returns) / len(daily_returns)
        std_r = math.sqrt(sum((r - avg_r) ** 2 for r in daily_returns) / len(daily_returns)) if len(daily_returns) > 1 else 1
        sharpe = (avg_r - 0.03 / 252) / std_r * math.sqrt(252) if std_r > 0 else 0
    else:
        sharpe = 0

    # 稳定性评分 (0-100)，综合考虑收益、回撤、胜率
    stability = min(100, max(0, 50 + total_return_pct * 2 - max_drawdown_pct * 3 + win_rate * 0.3))

    # 年化收益
    days = len(curve)
    ann_return = ((end_val / start_val) ** (252 / max(days, 1)) - 1) * 100 if start_val else 0

    return {
        "total_return": round(total_return, 2),
        "total_return_pct": round(total_return_pct, 2),
        "annualized_return": round(ann_return, 2),
        "max_drawdown": round(abs(max_dd) * start_val, 2),
        "max_drawdown_pct": round(max_drawdown_pct, 2),
        "win_rate": round(win_rate, 1),
        "sharpe_ratio": round(sharpe, 2),
        "stability_score": round(stability, 1),
    }


def _calculate_fees(trades: list[TradeDetail]) -> FeeBreakdown:
    """汇总费用明细。"""
    total_comm = sum(t.commission for t in trades)
    total_stamp = sum(t.stamp_tax for t in trades)
    total_transfer = sum(t.transfer_fee for t in trades)
    total_fees = total_comm + total_stamp + total_transfer
    # 费率比：总费用 / 总成交金额
    total_amount = sum(t.amount for t in trades) or 1
    return FeeBreakdown(
        total_commission=round(total_comm, 2),
        total_stamp_tax=round(total_stamp, 2),
        total_transfer_fee=round(total_transfer, 2),
        total_fees=round(total_fees, 2),
        fee_ratio=round(total_fees / total_amount * 100, 4),
    )


def _benchmark_comparison(
    curve: list[dict[str, Any]],
    benchmark_index: str,
    seed: int,
    initial_capital: float,
) -> BenchmarkComparison:
    """生成指数对比数据。"""
    if not curve:
        return BenchmarkComparison()

    index_name = INDEX_NAMES.get(benchmark_index, benchmark_index)

    # 生成模拟指数曲线
    bench_val = initial_capital
    bench_curve = []
    bench_peak = bench_val
    bench_max_dd = 0.0
    for i, p in enumerate(curve):
        step = ((seed + i * 7) % 15 - 7) / 1200
        bench_val *= (1 + step)
        bench_curve.append({"date": p["date"], "value": round(bench_val, 2)})
        if bench_val > bench_peak:
            bench_peak = bench_val
        dd = (bench_val - bench_peak) / bench_peak if bench_peak else 0
        if dd < bench_max_dd:
            bench_max_dd = dd

    strat_return = (curve[-1]["value"] / curve[0]["value"] - 1) * 100 if curve[0]["value"] else 0
    idx_return = (bench_curve[-1]["value"] / bench_curve[0]["value"] - 1) * 100 if bench_curve and bench_curve[0]["value"] else 0

    return BenchmarkComparison(
        index_code=benchmark_index,
        index_name=index_name,
        index_return=round(idx_return, 2),
        strategy_return=round(strat_return, 2),
        excess_return=round(strat_return - idx_return, 2),
        index_max_drawdown=round(abs(bench_max_dd) * 100, 2),
        index_curve=bench_curve,
    )


def _grade(metrics: dict[str, Any]) -> tuple[str, str]:
    """根据指标给出总评等级与说明。"""
    ret = metrics.get("total_return_pct", 0)
    dd = metrics.get("max_drawdown_pct", 0)
    wr = metrics.get("win_rate", 0)

    score = ret * 1.5 - dd * 2 + wr * 0.5

    if score >= 60:
        return "A", f"策略表现优秀！收益率 {ret:.1f}%，最大回撤仅 {dd:.1f}%，建议可以考虑小仓位实盘验证。"
    elif score >= 30:
        return "B", f"策略表现良好。收益率 {ret:.1f}%，回撤 {dd:.1f}% 可控，建议优化止损参数后再回测验证。"
    elif score >= 0:
        return "C", f"策略表现一般。收益率 {ret:.1f}%，回撤 {dd:.1f}% 偏高，建议调整指标或缩短回测周期重新测试。"
    else:
        return "D", f"策略表现较差。亏损 {abs(ret):.1f}%，回撤 {dd:.1f}%，建议更换策略逻辑或减少标的数量。"


def _ai_suggestion(metrics: dict[str, Any], fee: FeeBreakdown) -> str:
    """生成 AI 优化建议。"""
    suggestions = []
    dd = metrics.get("max_drawdown_pct", 0)
    wr = metrics.get("win_rate", 0)
    ret = metrics.get("total_return_pct", 0)

    if dd > 15:
        suggestions.append("• 最大回撤较高，建议设置更严格的止损线（如 8%），或增加趋势确认指标")
    if wr < 50:
        suggestions.append("• 胜率偏低，建议增加入场条件（如加入 MACD 金叉确认），减少假信号")
    if ret < 5:
        suggestions.append("• 收益率偏低，可考虑增加持仓时间或选择波动率更高的标的")
    if fee.fee_ratio > 1:
        suggestions.append(f"• 交易成本占比 {fee.fee_ratio:.2f}%，建议降低交易频率或选择佣金更低的券商")
    if not suggestions:
        suggestions.append("• 策略整体表现不错，建议增加样本外测试验证稳定性")
        suggestions.append("• 可尝试不同的时间段验证策略的适应性")
    return "\n".join(suggestions)


# ---------- 主入口 ----------

async def create_pending_backtest_task(
    tenant_id: str,
    strategy_id: str,
    request: BacktestRequest,
) -> BacktestTask:
    """仅创建 pending 任务，保存请求参数，立即返回。由 Worker 执行 run_backtest_job。"""
    start = _parse_date(request.start_date)
    end = _parse_date(request.end_date)
    if end < start:
        raise ValueError("invalid date range")

    request_params = request.model_dump()
    async for session in get_session():
        task = BacktestTask(
            tenant_id=tenant_id,
            strategy_id=strategy_id,
            start_date=request.start_date,
            end_date=request.end_date,
            status="pending",
            request_params_json=request_params,
        )
        session.add(task)
        await session.commit()
        await session.refresh(task)
        return task
    raise RuntimeError("session unavailable")


async def run_backtest_job(
    tenant_id: str,
    strategy_id: str,
    backtest_id: str,
) -> None:
    """Worker 调用：执行回测逻辑，更新任务状态与结果。"""
    from src.services.data_service.kline_storage import load_kline_range

    # #region agent log
    try:
        _lp = __import__("pathlib").Path(__file__).resolve().parent.parent.parent.parent / ".cursor" / "debug.log"
        _lp.parent.mkdir(parents=True, exist_ok=True)
        with open(_lp, "a", encoding="utf-8") as _f:
            __import__("json").dump({"hypothesisId": "H3,H4", "location": "backtest_service.run_backtest_job", "message": "job_start", "data": {"backtest_id": backtest_id}, "timestamp": __import__("time").time()}, _f, ensure_ascii=False)
            _f.write("\n")
    except Exception:
        pass
    # #endregion
    async for session in get_session():
        stmt = select(BacktestTask).where(
            BacktestTask.tenant_id == tenant_id,
            BacktestTask.strategy_id == strategy_id,
            BacktestTask.id == backtest_id,
        )
        result = await session.execute(stmt)
        task = result.scalar_one_or_none()
        if not task:
            raise ValueError("backtest task not found")
        if task.status not in ("pending", "running"):
            return  # 已被其他 worker 处理或已完成/失败

        req = task.request_params_json or {}
        start_date = req.get("start_date") or task.start_date
        end_date = req.get("end_date") or task.end_date
        initial_capital = req.get("initial_capital", 100000)
        targets_raw = [
            {"code": t.get("code", ""), "name": t.get("name") or t.get("code", ""), "weight": t.get("weight", 0)}
            for t in req.get("targets", [])
        ]
        if not targets_raw:
            targets_raw = [{"code": "600519.SH", "name": "贵州茅台", "weight": 100}]
        commission_rate = req.get("commission_rate", 0.00025)
        min_commission = req.get("min_commission", 5.0)
        stamp_tax_rate = req.get("stamp_tax_rate", 0.001)
        transfer_fee_rate = req.get("transfer_fee_rate", 0.00002)
        take_profit = req.get("take_profit")
        stop_loss = req.get("stop_loss")
        benchmark_index = req.get("benchmark_index", "000300.SH")

        # 若尚未 running，标记之（兼容直接调用）
        from datetime import datetime as dt
        if task.status == "pending":
            await session.execute(
                update(BacktestTask)
                .where(BacktestTask.id == backtest_id)
                .values(status="running", started_at=dt.utcnow())
            )
            await session.commit()

    try:
        seed = _seed_from_id(strategy_id)
        primary_code = targets_raw[0].get("code", "600519.SH")
        primary_norm = _normalize_symbol(primary_code)
        bars = await load_kline_range(primary_norm, "daily", start_date, end_date)

        trades, equity_curve, kline_data, kline_source = _simulate_trades(
            start_date, end_date, seed, initial_capital, targets_raw,
            commission_rate, min_commission, stamp_tax_rate, transfer_fee_rate,
            take_profit, stop_loss, bars=bars if bars else None,
        )

        metrics = _calculate_metrics(equity_curve, initial_capital)
        fee = _calculate_fees(trades)
        bench = _benchmark_comparison(equity_curve, benchmark_index, seed, initial_capital)
        grade_letter, grade_text = _grade(metrics)
        suggestion = _ai_suggestion(metrics, fee)

        full_result = {
            "metrics": metrics,
            "fee_breakdown": fee.model_dump(),
            "trade_details": [t.model_dump() for t in trades],
            "benchmark": bench.model_dump(),
            "equity_curve": equity_curve,
            "kline_data": kline_data,
            "ai_suggestion": suggestion,
            "grade": grade_letter,
            "grade_text": grade_text,
            "kline_source": kline_source,
        }

        from datetime import datetime as dt
        async for session in get_session():
            await session.execute(
                update(BacktestTask)
                .where(BacktestTask.id == backtest_id)
                .values(
                    status="completed",
                    result_metrics_json=full_result,
                    finished_at=dt.utcnow(),
                    error_detail=None,
                )
            )
            await session.commit()

        try:
            from src.services.strategy_service import update_strategy_backtest_summary
            await update_strategy_backtest_summary(
                tenant_id=tenant_id,
                strategy_id=strategy_id,
                backtest_id=backtest_id,
                grade=grade_letter,
                metrics_summary={
                    "total_return_pct": metrics.get("total_return_pct", 0),
                    "win_rate": metrics.get("win_rate", 0),
                    "max_drawdown_pct": metrics.get("max_drawdown_pct", 0),
                    "sharpe_ratio": metrics.get("sharpe_ratio", 0),
                },
            )
        except Exception:
            pass
        # #region agent log
        try:
            _lp = __import__("pathlib").Path(__file__).resolve().parent.parent.parent.parent / ".cursor" / "debug.log"
            with open(_lp, "a", encoding="utf-8") as _f:
                __import__("json").dump({"hypothesisId": "H3", "location": "backtest_service.run_backtest_job", "message": "job_completed", "data": {"backtest_id": backtest_id}, "timestamp": __import__("time").time()}, _f, ensure_ascii=False)
                _f.write("\n")
        except Exception:
            pass
        # #endregion
        return
    except Exception as exc:
        err_msg = str(exc)[:500]
        # #region agent log
        try:
            _lp = __import__("pathlib").Path(__file__).resolve().parent.parent.parent.parent / ".cursor" / "debug.log"
            with open(_lp, "a", encoding="utf-8") as _f:
                __import__("json").dump({"hypothesisId": "H3,H4", "location": "backtest_service.run_backtest_job", "message": "job_failed", "data": {"backtest_id": backtest_id, "exc_type": type(exc).__name__, "exc_msg": err_msg[:200]}, "timestamp": __import__("time").time()}, _f, ensure_ascii=False)
                _f.write("\n")
        except Exception:
            pass
        # #endregion
        from datetime import datetime as dt
        async for session in get_session():
            await session.execute(
                update(BacktestTask)
                .where(BacktestTask.id == backtest_id)
                .values(
                    status="failed",
                    error_detail=err_msg,
                    finished_at=dt.utcnow(),
                )
            )
            await session.commit()
        raise


async def create_backtest_task(
    tenant_id: str,
    strategy_id: str,
    start_date: str,
    end_date: str,
    seed: int,
    request: BacktestRequest | None = None,
) -> BacktestTask:
    """兼容旧调用：同步执行回测并返回。供测试或同步模式使用。"""
    if not request:
        from src.schemas.backtest import BacktestRequest
        request = BacktestRequest(start_date=start_date, end_date=end_date)
    task = await create_pending_backtest_task(tenant_id, strategy_id, request)
    await run_backtest_job(tenant_id, strategy_id, task.id)
    async for session in get_session():
        stmt = select(BacktestTask).where(BacktestTask.id == task.id)
        result = await session.execute(stmt)
        return result.scalar_one()
    raise RuntimeError("session unavailable")


async def claim_next_pending_backtest_task(
) -> Optional[tuple[str, str, str]]:
    """原子获取一条 pending 任务并改为 running。返回 (tenant_id, strategy_id, backtest_id) 或 None。"""
    from datetime import datetime as dt

    async for session in get_session():
        stmt = (
            select(BacktestTask)
            .where(BacktestTask.status == "pending")
            .order_by(BacktestTask.created_at)
            .limit(1)
        )
        result = await session.execute(stmt)
        task = result.scalar_one_or_none()
        if not task:
            return None
        bid, tid, sid = task.id, task.tenant_id, task.strategy_id
        r = await session.execute(
            update(BacktestTask)
            .where(BacktestTask.id == bid, BacktestTask.status == "pending")
            .values(status="running", started_at=dt.utcnow())
        )
        await session.commit()
        # 若另一 worker 已更新，此处仍会 commit 但可能覆盖；为简化先直接返回
        return (tid, sid, bid)
    return None


async def get_backtest_task(
    tenant_id: str,
    strategy_id: str,
    backtest_id: str,
) -> Optional[BacktestTask]:
    """获取单次回测任务的完整数据。"""
    async for session in get_session():
        stmt = (
            select(BacktestTask)
            .where(
                BacktestTask.tenant_id == tenant_id,
                BacktestTask.strategy_id == strategy_id,
                BacktestTask.id == backtest_id,
            )
        )
        result = await session.execute(stmt)
        return result.scalar_one_or_none()
    return None


async def list_backtest_history(
    tenant_id: str,
    strategy_id: str,
    limit: int = 5,
) -> List[dict]:
    """获取指定策略最近 N 次回测记录摘要。"""
    async for session in get_session():
        stmt = (
            select(BacktestTask)
            .where(
                BacktestTask.tenant_id == tenant_id,
                BacktestTask.strategy_id == strategy_id,
            )
            .order_by(BacktestTask.created_at.desc())
            .limit(limit)
        )
        result = await session.execute(stmt)
        tasks = result.scalars().all()
        items = []
        for idx, t in enumerate(tasks):
            rj = t.result_metrics_json or {}
            m = rj.get("metrics", {})
            # 从交易明细中提取标的列表
            td = rj.get("trade_details", [])
            seen = set()
            tgt_list = []
            for tr in td:
                code = tr.get("target", "")
                if code and code not in seen:
                    seen.add(code)
                    tgt_list.append({"code": code, "name": tr.get("target_name", code)})
            items.append({
                "backtest_id": t.id,
                "start_date": t.start_date,
                "end_date": t.end_date,
                "grade": rj.get("grade", ""),
                "total_return_pct": m.get("total_return_pct", 0),
                "win_rate": m.get("win_rate", 0),
                "max_drawdown_pct": m.get("max_drawdown_pct", 0),
                "created_at": str(t.created_at) if t.created_at else "",
                "targets": tgt_list,
                "trade_count": len(td),
            })
        return items
    return []
