from fastapi import APIRouter, Header, HTTPException, Query
from sqlalchemy import select

from src.core.db import get_session
from src.core.errors import ValidationError
from src.models.order import Order
from src.schemas.response import BaseResponse, ok
from src.schemas.trade import OrderOut, OrderRequest
from src.schemas.user import UserOut
from src.services.auth_service import get_user_from_token
from src.services.deploy_service import get_deploy_eligibility, get_deploy_gateways
from src.services.trade_mode_service import list_trade_modes
from src.services.trade_service import create_order
from src.services.validation_service import validate_order_payload

router = APIRouter()


async def _require_user(authorization: str | None) -> UserOut:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="missing token")
    token = authorization.split(" ", 1)[1]
    user = await get_user_from_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="invalid token")
    return user


@router.post("/trade/order", response_model=BaseResponse[OrderOut])
async def submit_order(
    payload: OrderRequest,
    authorization: str | None = Header(default=None),
) -> BaseResponse[OrderOut]:
    user = await _require_user(authorization)
    errors = validate_order_payload(payload.symbol, payload.price, payload.volume)
    if errors:
        raise ValidationError("; ".join(errors)).as_http_exception()
    try:
        order = await create_order(user.tenant_id, user.user_id, payload)
    except ValidationError as exc:
        raise exc.as_http_exception() from exc
    return ok(
        OrderOut(
            order_id=order.id,
            symbol=order.symbol,
            status=order.status,
            direction=order.direction,
            price=order.price,
            volume=order.volume,
            env=order.env,
        )
    )


@router.post("/trade/order/async", response_model=BaseResponse[OrderOut])
async def submit_order_async(
    payload: OrderRequest,
    authorization: str | None = Header(default=None),
) -> BaseResponse[OrderOut]:
    user = await _require_user(authorization)
    errors = validate_order_payload(payload.symbol, payload.price, payload.volume)
    if errors:
        raise ValidationError("; ".join(errors)).as_http_exception()
    try:
        order = await create_order(user.tenant_id, user.user_id, payload)
    except ValidationError as exc:
        raise exc.as_http_exception() from exc
    return ok(
        OrderOut(
            order_id=order.id,
            symbol=order.symbol,
            status=order.status,
            direction=order.direction,
            price=order.price,
            volume=order.volume,
            env=order.env,
        )
    )


@router.get("/trade/orders", response_model=BaseResponse[list[dict]])
async def list_orders(
    status: str | None = Query(default=None, description="按状态过滤: submitted, filled, cancelled 等"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    authorization: str | None = Header(default=None),
) -> BaseResponse[list[dict]]:
    """查询用户委托记录，支持分页和状态过滤"""
    user = await _require_user(authorization)
    async for session in get_session():
        query = (
            select(Order)
            .where(Order.user_id == user.user_id)
            .where(Order.is_deleted.is_(False))
        )
        if status:
            query = query.where(Order.status == status)
        query = query.order_by(Order.created_at.desc()).offset(offset).limit(limit)
        result = await session.execute(query)
        orders = result.scalars().all()
        return ok([
            {
                "order_id": str(o.id),
                "symbol": o.symbol,
                "direction": o.direction,
                "price": o.price,
                "volume": o.volume,
                "status": o.status,
                "env": o.env,
                "created_at": o.created_at.isoformat() if o.created_at else "",
            }
            for o in orders
        ])
    return ok([])


@router.get("/trade/modes", response_model=BaseResponse[list[str]])
async def list_modes(
    authorization: str | None = Header(default=None),
) -> BaseResponse[list[str]]:
    user = await _require_user(authorization)
    modes = await list_trade_modes(user_level=user.level)
    return ok(list(modes))


@router.get(
    "/trade/deploy-eligibility",
    response_model=BaseResponse[dict],
    summary="部署资格查询",
    description="返回 sim/live 是否可部署，用于前端禁用实盘按钮或展示提示。",
)
async def deploy_eligibility(
    authorization: str | None = Header(default=None),
) -> BaseResponse[dict]:
    user = await _require_user(authorization)
    tenant_id = getattr(user, "tenant_id", "public")
    result = await get_deploy_eligibility(tenant_id)
    return ok(result)


@router.get(
    "/trade/deploy-gateways",
    response_model=BaseResponse[list],
    summary="部署可用网关列表",
    description="按 target=sim|live 返回可选网关列表（脱敏），用于部署向导选择网关。",
)
async def deploy_gateways(
    target: str = Query(..., description="sim=模拟盘, live=实盘"),
    authorization: str | None = Header(default=None),
) -> BaseResponse[list]:
    if target not in ("sim", "live"):
        raise HTTPException(status_code=400, detail="target 必须为 sim 或 live")
    user = await _require_user(authorization)
    tenant_id = getattr(user, "tenant_id", "public")
    result = await get_deploy_gateways(tenant_id, target)
    return ok(result)
