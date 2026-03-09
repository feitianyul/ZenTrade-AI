from typing import Any, Optional

from pydantic import BaseModel, Field


class ReplayRequest(BaseModel):
    strategy_id: str
    start_date: str = Field(..., min_length=8, max_length=32)
    end_date: str = Field(..., min_length=8, max_length=32)


class ReplayReportOut(BaseModel):
    report_id: str
    strategy_id: str
    status: str
    report_json: dict[str, Any]


class TradeAnalysisOut(BaseModel):
    analysis_id: str
    trade_id: str
    metrics_json: dict[str, Any]
    summary: str


class ExportRequest(BaseModel):
    export_type: str
    payload: Optional[dict[str, Any]] = None
