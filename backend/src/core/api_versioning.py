"""T171 - API 版本与兼容策略"""

import re
from typing import Any, Callable, Optional

from fastapi import Header, HTTPException, Request

# 支持的 API 版本
SUPPORTED_VERSIONS = ["v1", "v2"]
DEFAULT_VERSION = "v1"
DEPRECATED_VERSIONS = ["v0"]


def extract_api_version(path: str) -> str:
    """从 URL 路径提取 API 版本"""
    match = re.match(r"^/api/(v\d+)/", path)
    if match:
        return match.group(1)
    return DEFAULT_VERSION


def validate_api_version(version: str) -> bool:
    """校验 API 版本是否受支持"""
    return version in SUPPORTED_VERSIONS


def is_deprecated(version: str) -> bool:
    """是否已废弃"""
    return version in DEPRECATED_VERSIONS


async def api_version_middleware(request: Request, call_next: Callable) -> Any:
    """API 版本中间件"""
    version = extract_api_version(request.url.path)

    if version in DEPRECATED_VERSIONS:
        # 允许但添加警告头
        response = await call_next(request)
        response.headers["X-API-Deprecated"] = "true"
        response.headers["X-API-Sunset"] = "2026-06-01"
        return response

    if version not in SUPPORTED_VERSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported API version: {version}. Supported: {SUPPORTED_VERSIONS}",
        )

    response = await call_next(request)
    response.headers["X-API-Version"] = version
    return response


class VersionedRoute:
    """版本化路由帮助器"""

    def __init__(self, version: str = DEFAULT_VERSION):
        self.version = version
        self.prefix = f"/api/{version}"

    def path(self, route: str) -> str:
        return f"{self.prefix}{route}"


def version_negotiation(
    accept_version: Optional[str] = Header(default=None, alias="X-API-Version"),
) -> str:
    """通过请求头协商 API 版本"""
    version = accept_version or DEFAULT_VERSION
    if not validate_api_version(version):
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported version: {version}",
        )
    return version


# 版本兼容性映射
COMPATIBILITY_MAP: dict[str, dict[str, str]] = {
    "v1": {
        "response_format": "wrapped",
        "pagination": "offset",
        "date_format": "iso8601",
    },
    "v2": {
        "response_format": "wrapped",
        "pagination": "cursor",
        "date_format": "iso8601",
    },
}


def get_version_config(version: str) -> dict[str, str]:
    """获取版本特定配置"""
    return COMPATIBILITY_MAP.get(version, COMPATIBILITY_MAP[DEFAULT_VERSION])
