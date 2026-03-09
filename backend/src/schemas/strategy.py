from typing import Any, Literal, Optional

from pydantic import BaseModel, Field


class FromAnalysisRequest(BaseModel):
    report_id: str = Field(..., min_length=1, description="分析报告 ID，来自 ai_analysis_reports.id")


class StrategyCreateRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=128)
    logic_code: str = Field(default="")
    logic_desc: Optional[str] = Field(None, description="中文策略描述，双面板左侧")
    params_json: Optional[dict[str, Any]] = None


class StrategyUpdateRequest(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=128)
    logic_code: Optional[str] = Field(None, description="策略代码，可为空")
    logic_desc: Optional[str] = Field(None, description="中文策略描述")
    params_json: Optional[dict[str, Any]] = None


class StrategyOut(BaseModel):
    strategy_id: str
    tenant_id: str
    name: str
    status: str
    logic_desc: Optional[str] = None
    logic_code: Optional[str] = None
    params_json: Optional[dict[str, Any]] = None
    created_at: Optional[str] = None
    version: int = 1
    # 回测摘要
    last_backtest_id: Optional[str] = None
    last_backtest_grade: Optional[str] = None
    last_backtest_metrics: Optional[dict[str, Any]] = None
    source_report_id: Optional[str] = None


class StrategyList(BaseModel):
    items: list[StrategyOut]
    page: int
    limit: int
    total: int


class StrategyVersionItem(BaseModel):
    version_no: int
    version_label: str  # e.g. V1.0, V1.1
    created_at: str
    created_by: Optional[str] = None
    is_active: bool = False


class StrategyVersionList(BaseModel):
    items: list[StrategyVersionItem]
    total: int
    page: int
    page_size: int


class StrategyVersionSnapshotOut(BaseModel):
    """单版本快照，用于编辑/复制并编辑填充表单。"""
    logic_desc: Optional[str] = None
    logic_code: Optional[str] = None


class LogicBlock(BaseModel):
    id: str
    type: str
    label: str
    details: Optional[str] = None


class StrategyParseRequest(BaseModel):
    prompt: str = Field(..., min_length=1)


class StrategyParseResult(BaseModel):
    name: str
    logic_code: str
    params_json: dict[str, Any]
    logic_blocks: list[LogicBlock]
    risk_hint: str


class DeployRequest(BaseModel):
    """部署请求：target=sim 模拟盘，target=live 实盘。gateway_id 为网关在 gateways 列表中的索引（可选）。"""

    target: Literal["sim", "live"] = Field(..., description="sim=模拟盘, live=实盘")
    gateway_id: Optional[str] = Field(None, description="网关索引，如 0、1，由部署向导选择后传入")
    gateway_account: Optional[str] = Field(None, description="可选，用户自有账号，用于该策略连接网关")
    gateway_password: Optional[str] = Field(None, description="可选，用户自有账号密码，仅当次连接使用不落库或加密后存")


class DeployOut(BaseModel):
    strategy_id: str
    target: str
    status: str
    run_env: str
