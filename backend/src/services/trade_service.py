from datetime import datetime, timedelta
from typing import cast

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.db import get_session, with_tenant
from src.core.errors import ValidationError
from src.models.order import Order
from src.models.position import Position
from src.schemas.trade import OrderRequest
from src.services.trading_gateway.vnpy_gateway import get_vnpy_gateway


def _day_bounds(value: datetime) -> tuple[datetime, datetime]:
    start = datetime(value.year, value.month, value.day)
    return start, start + timedelta(days=1)


async def _count_orders_today(tenant_id: str, user_id: str) -> int:
    now = datetime.utcnow()
    start, end = _day_bounds(now)
    async for session in get_session():
        query = with_tenant(select(func.count(Order.id)), Order, tenant_id).where(
            Order.user_id == user_id,
            Order.created_at >= start,
            Order.created_at < end,
            Order.is_deleted.is_(False),
        )
        result = await session.execute(query)
        return int(result.scalar() or 0)
    return 0


async def create_order(tenant_id: str, user_id: str, payload: OrderRequest) -> Order:
    trade_count = await _count_orders_today(tenant_id, user_id)
    if trade_count >= 10:
        raise ValidationError("您今日交易次数已达上限（10次）")
    gateway = get_vnpy_gateway()
    gateway_ref = await gateway.send_order(
        {
            "symbol": payload.symbol,
            "direction": payload.direction,
            "price": payload.price,
            "volume": payload.volume,
            "env": payload.env,
        }
    )
    async for session in get_session():
        order = Order(
            tenant_id=tenant_id,
            user_id=user_id,
            env=payload.env,
            symbol=payload.symbol,
            direction=payload.direction,
            price=payload.price,
            volume=payload.volume,
            status="submitted",
            gateway_ref=gateway_ref,
            is_deleted=False,
        )
        session.add(order)
        position = await _get_position(session, tenant_id, user_id, payload.env, payload.symbol)
        if position is None:
            position = Position(
                tenant_id=tenant_id,
                user_id=user_id,
                env=payload.env,
                symbol=payload.symbol,
                volume=0,
                avg_price=0.0,
                pnl=0.0,
                frozen_volume=0,
                is_deleted=False,
            )
            session.add(position)
        if payload.direction == "BUY":
            total_cost = position.avg_price * position.volume + payload.price * payload.volume
            position.volume += payload.volume
            if position.volume > 0:
                position.avg_price = total_cost / position.volume
        else:
            sell_volume = min(position.volume, payload.volume)
            position.volume -= sell_volume
            position.pnl += (payload.price - position.avg_price) * sell_volume
        await session.commit()
        await session.refresh(order)
        return order
    raise RuntimeError("session unavailable")


async def _get_position(
    session: AsyncSession, tenant_id: str, user_id: str, env: str, symbol: str
) -> Position | None:
    query = with_tenant(select(Position), Position, tenant_id).where(
        Position.user_id == user_id,
        Position.env == env,
        Position.symbol == symbol,
        Position.is_deleted.is_(False),
    )
    result = await session.execute(query)
    return cast(Position | None, result.scalar_one_or_none())


async def list_positions(tenant_id: str, user_id: str, env: str | None = None) -> list[Position]:
    async for session in get_session():
        query = with_tenant(select(Position), Position, tenant_id).where(
            Position.user_id == user_id,
            Position.is_deleted.is_(False),
        )
        if env:
            query = query.where(Position.env == env)
        result = await session.execute(query.order_by(Position.symbol))
        return list(result.scalars().all())
    return []
