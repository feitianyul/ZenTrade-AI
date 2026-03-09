from __future__ import annotations

import io
import json
from datetime import datetime
from typing import Any

from sqlalchemy import select

from src.core.db import get_session, with_tenant
from src.core.errors import NotFoundError, ValidationError
from src.core.file_limits import ensure_export_limits
from src.core.pdf import build_pdf_bytes
from src.models.replay_report import ReplayReport


def _build_report_lines(report_json: dict[str, Any]) -> list[str]:
    lines = []
    summary = report_json.get("summary")
    if summary:
        lines.append(f"摘要: {summary}")
    range_info = report_json.get("range")
    if isinstance(range_info, dict):
        start = range_info.get("start")
        end = range_info.get("end")
        lines.append(f"区间: {start} - {end}")
    signals = report_json.get("signals")
    if isinstance(signals, list):
        lines.append(f"信号数量: {len(signals)}")
    return lines


def _build_xlsx(report_json: dict[str, Any]) -> bytes:
    try:
        from openpyxl import Workbook
    except ImportError as exc:
        raise ValidationError("xlsx not supported") from exc
    workbook = Workbook()
    sheet = workbook.active
    sheet.append(["key", "value"])
    for key, value in report_json.items():
        sheet.append([key, json.dumps(value, ensure_ascii=False)])
    output = io.BytesIO()
    workbook.save(output)
    return output.getvalue()


async def export_replay_report(
    tenant_id: str,
    report_id: str,
    export_type: str,
) -> dict[str, Any]:
    async for session in get_session():
        query = with_tenant(select(ReplayReport), ReplayReport, tenant_id).where(
            ReplayReport.id == report_id
        )
        result = await session.execute(query)
        report = result.scalar_one_or_none()
        if not report:
            raise NotFoundError("replay report not found")
        report_json = report.report_json
        break
    if export_type == "pdf":
        lines = _build_report_lines(report_json)
        payload = build_pdf_bytes("复盘报告", lines)
        content_type = "application/pdf"
        extension = "pdf"
    elif export_type == "xlsx":
        payload = _build_xlsx(report_json)
        content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        extension = "xlsx"
    else:
        raise ValidationError("unsupported export type")

    ensure_export_limits(len(payload))
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    filename = f"replay_report_{report_id}_{timestamp}.{extension}"
    return {"filename": filename, "content_type": content_type, "payload": payload}
