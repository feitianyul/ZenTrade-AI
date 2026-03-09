from typing import Awaitable, Callable

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response


def extract_tenant_id(request: Request) -> str:
    tenant_id = request.headers.get("X-Tenant-Id")
    if tenant_id:
        return tenant_id
    return request.headers.get("X-Tenant", "public")

class TenantContextMiddleware(BaseHTTPMiddleware):
    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        request.state.tenant_id = extract_tenant_id(request)
        return await call_next(request)
