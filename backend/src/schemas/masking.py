from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class MaskingRule(BaseModel):
    field: str
    type: str  # phone, bank_card, email, id_card, name, address, trade_amount
    mask_char: str = "*"

class MaskingRequest(BaseModel):
    data: Dict[str, Any]
    rules: List[MaskingRule]

class DataRightRequest(BaseModel):
    request_type: str = Field(..., description="export or delete")
    reason: Optional[str] = None
    data_categories: List[str] = Field(default_factory=list)

class ConsentRecord(BaseModel):
    consent_id: str
    user_id: str
    scope: str
    status: str  # granted, revoked
    timestamp: float
