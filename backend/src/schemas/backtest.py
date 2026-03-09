from typing import Any, Optional

from pydantic import BaseModel, Field


class TargetItem(BaseModel):
    """回测标的"""
    code: str = Field(..., description="标的代码，如 600519.SH")
    name: Optional[str] = Field(None, description="标的名称")
    weight: float = Field(0, ge=0, le=100, description="持仓权重百分比，0 表示均分")


class BacktestRequest(BaseModel):
    start_date: str = Field(..., min_length=8, max_length=32, description="回测开始日期 YYYY-MM-DD")
    end_date: str = Field(..., min_length=8, max_length=32, description="回测结束日期 YYYY-MM-DD")
    initial_capital: float = Field(100000, ge=10000, le=10000000, description="初始资金(元)")
    targets: list[TargetItem] = Field(default_factory=list, description="回测标的列表")
    target_type: str = Field("stock", description="标的类型: stock/sector/concept/index")
    commission_rate: float = Field(0.00025, ge=0, le=0.01, description="佣金费率(默认万2.5)")
    min_commission: float = Field(5.0, ge=0, description="最低佣金(元)，0表示免5")
    stamp_tax_rate: float = Field(0.001, ge=0, le=0.01, description="印花税率(默认千1)")
    transfer_fee_rate: float = Field(0.00002, ge=0, le=0.001, description="过户费率(默认万0.2)")
    take_profit: Optional[float] = Field(None, ge=0, le=100, description="止盈比例(%)")
    stop_loss: Optional[float] = Field(None, ge=0, le=100, description="止损比例(%)")
    benchmark_index: str = Field("000300.SH", description="对标指数代码(默认沪深300)")


class TradeDetail(BaseModel):
    """单笔交易明细"""
    date: str = Field(..., description="交易日期")
    action: str = Field(..., description="买入/卖出")
    target: str = Field(..., description="标的代码")
    target_name: str = Field("", description="标的名称")
    price: float = Field(..., description="成交价格")
    quantity: int = Field(..., description="成交数量")
    amount: float = Field(..., description="成交金额")
    commission: float = Field(0, description="佣金")
    stamp_tax: float = Field(0, description="印花税")
    transfer_fee: float = Field(0, description="过户费")
    total_cost: float = Field(0, description="总费用")
    pnl: float = Field(0, description="该笔盈亏")
    pnl_pct: float = Field(0, description="该笔盈亏比例(%)")


class FeeBreakdown(BaseModel):
    """费用明细"""
    total_commission: float = Field(0, description="总佣金")
    total_stamp_tax: float = Field(0, description="总印花税")
    total_transfer_fee: float = Field(0, description="总过户费")
    total_fees: float = Field(0, description="总费用合计")
    fee_ratio: float = Field(0, description="费用占初始资金比例(%)")


class BenchmarkComparison(BaseModel):
    """指数对比数据"""
    index_code: str = Field("", description="对标指数代码")
    index_name: str = Field("", description="对标指数名称")
    index_return: float = Field(0, description="指数区间收益率(%)")
    strategy_return: float = Field(0, description="策略收益率(%)")
    excess_return: float = Field(0, description="超额收益(%)")
    index_max_drawdown: float = Field(0, description="指数最大回撤(%)")
    index_curve: list[dict[str, Any]] = Field(default_factory=list, description="指数净值曲线")


class BacktestResult(BaseModel):
    backtest_id: str
    strategy_id: str = Field("", description="策略ID")
    strategy_name: str = Field("", description="策略名称")
    status: str = Field("completed", description="pending | running | completed | failed")
    error_detail: Optional[str] = Field(None, description="失败时错误信息")
    metrics: dict[str, Any] = Field(default_factory=dict, description="核心指标")
    fee_breakdown: FeeBreakdown = Field(default_factory=FeeBreakdown, description="费用明细")
    trade_details: list[TradeDetail] = Field(default_factory=list, description="交易明细列表")
    benchmark: BenchmarkComparison = Field(default_factory=BenchmarkComparison, description="指数对比")
    equity_curve: list[dict[str, Any]] = Field(default_factory=list, description="净值曲线")
    kline_data: list[dict[str, Any]] = Field(default_factory=list, description="K线OHLCV数据")
    ai_suggestion: str = Field("", description="AI优化建议")
    grade: str = Field("", description="总评等级: A/B/C/D")
    grade_text: str = Field("", description="总评说明")
