"""AI 个股分析：请求与响应模型，与前端 _sdAiResult 一致；报告版本 CRUD 用 Schema。"""
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class AiAnalysisGenerateRequest(BaseModel):
    """生成分析请求"""
    symbol: str = Field(..., description="股票代码，如 000630 或 000630.SZ")
    user_notes: Optional[str] = Field(default=None, description="补充说明（选填）")


class DimensionOut(BaseModel):
    """单维度结论：技术面/资金面/基本面/舆情"""
    name: str = Field(..., description="维度名称")
    direction: str = Field(..., description="偏多|偏空|中性")
    strength: int = Field(..., ge=0, le=100, description="强度 0-100")
    summary: str = Field(..., description="一段话结论")


class MainOut(BaseModel):
    """主控结论"""
    direction: str = Field(..., description="偏多|偏空|中性")
    score: int = Field(..., ge=0, le=100)
    confidence: float = Field(..., ge=0, le=1)
    summary: str = Field(..., description="综合结论摘要")


class ZoneOut(BaseModel):
    """风险/收益分区"""
    id: str = Field(..., description="如 aggressive/balanced/stable")
    label: str = Field(..., description="如 激进区")
    desc: str = Field(..., description="描述")
    action: str = Field(..., description="建议操作")


class SuggestOut(BaseModel):
    """买卖建议"""
    buy: bool = Field(..., description="是否偏多/可考虑关注")
    text: str = Field(..., description="建议文案")


class AiAnalysisResult(BaseModel):
    """与前端 _sdAiResult 一致的结构化分析结果"""
    dimensions: List[DimensionOut] = Field(..., description="四维结论")
    main: MainOut = Field(..., description="主控结论")
    zones: List[ZoneOut] = Field(..., description="激进/平衡/稳定等分区")
    suggest: SuggestOut = Field(..., description="买卖建议")


class AiAnalysisConfirmRequest(BaseModel):
    """确认分析（采纳）请求 — 供审计/后续步骤使用"""
    symbol: str = Field(..., description="股票代码")
    user_notes: Optional[str] = Field(default=None, description="用户补充说明")
    main_summary: Optional[str] = Field(default=None, description="主控结论摘要")
    suggest_text: Optional[str] = Field(default=None, description="买卖建议文案")
    report_id: Optional[str] = Field(default=None, description="报告 ID，采纳时更新该条为 adopted")


# ----- 分析报告版本（列表/创建/详情） -----


class AiAnalysisReportCreate(BaseModel):
    """创建分析报告（生成后落库 draft）"""
    symbol: str = Field(..., description="标的代码")
    user_notes: Optional[str] = Field(default=None, description="补充说明")
    report_snapshot: Dict[str, Any] = Field(..., description="完整分析结果，与 AiAnalysisResult 结构一致")
    status: str = Field(default="draft", description="draft | adopted")


class AiAnalysisReportItem(BaseModel):
    """报告列表项"""
    id: str = Field(..., description="报告 ID")
    symbol: str = Field(..., description="标的代码")
    main_summary: Optional[str] = Field(default=None, description="主结论摘要，便于表格展示")
    status: str = Field(..., description="draft | adopted")
    created_at: Optional[str] = Field(default=None, description="创建时间 ISO 或格式化串")


class AiAnalysisReportList(BaseModel):
    """报告分页列表"""
    items: List[AiAnalysisReportItem] = Field(default_factory=list)
    total: int = Field(..., ge=0)
    page: int = Field(..., ge=1)
    page_size: int = Field(..., ge=1, le=50)


class AiAnalysisReportOut(BaseModel):
    """单条报告详情（含快照，用于「查看」恢复）"""
    id: str = Field(..., description="报告 ID")
    symbol: str = Field(..., description="标的代码")
    created_at: Optional[str] = Field(default=None)
    report_snapshot: Dict[str, Any] = Field(..., description="完整分析结果")
