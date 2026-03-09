from pydantic import BaseModel


class AuditLogOut(BaseModel):
    id: str
    tenant_id: str
    actor_id: str
    action: str
    resource_type: str
    resource_id: str
    status: str
    ip_address: str
    user_agent: str
    detail: str
    created_at: str  # 北京时间 YYYY-MM-DD HH:MM:SS

    class Config:
        from_attributes = True
