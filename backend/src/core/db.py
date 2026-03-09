import os
from typing import Any, AsyncGenerator, Type, cast

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.sql import ColumnElement, Select


def _normalize_dsn(dsn: str) -> str:
    if dsn.startswith("mysql+pymysql://"):
        return "mysql+asyncmy://" + dsn.split("mysql+pymysql://", 1)[1]
    return dsn

_engine: AsyncEngine | None = None
_SessionLocal: async_sessionmaker[AsyncSession] | None = None


def _ensure_engine() -> None:
    global _engine, _SessionLocal
    if _engine is None:
        dsn = _normalize_dsn(
            os.getenv(
                "MYSQL_DSN",
                "mysql+asyncmy://user:pass@localhost:3306/retail_lowfreq",
            )
        )
        engine_kwargs: dict[str, Any] = {
            "pool_pre_ping": True,
            "future": True,
        }
        if not dsn.startswith("sqlite+"):
            engine_kwargs.update(
                {
                    "pool_size": 5,
                    "max_overflow": 10,
                    "pool_recycle": 300,
                }
            )
        _engine = create_async_engine(dsn, **engine_kwargs)
        _SessionLocal = async_sessionmaker(_engine, expire_on_commit=False, class_=AsyncSession)

async def get_session() -> AsyncGenerator[AsyncSession, None]:
    _ensure_engine()
    assert _SessionLocal is not None
    async with _SessionLocal() as session:
        yield session


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async for session in get_session():
        yield session


def get_engine() -> AsyncEngine:
    _ensure_engine()
    assert _engine is not None
    return _engine

def tenant_filter(model: Type[Any], tenant_id: str) -> ColumnElement[bool]:
    return cast(ColumnElement[bool], model.tenant_id == tenant_id)

def with_tenant(query: Select[Any], model: Type[Any], tenant_id: str) -> Select[Any]:
    return query.where(tenant_filter(model, tenant_id))
