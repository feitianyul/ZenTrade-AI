from fastapi import APIRouter, Header, HTTPException

from src.schemas.response import BaseResponse, ok
from src.schemas.trade import PositionOut
from src.schemas.user import UserOut
from src.services.auth_service import get_user_from_token
from src.services.trade_service import list_positions

router = APIRouter()


async def _require_user(authorization: str | None) -> UserOut:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="missing token")
    token = authorization.split(" ", 1)[1]
    user = await get_user_from_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="invalid token")
    return user


@router.get("/account/positions", response_model=BaseResponse[list[PositionOut]])
async def get_positions(
    env: str | None = None,
    authorization: str | None = Header(default=None),
) -> BaseResponse[list[PositionOut]]:
    user = await _require_user(authorization)
    positions = await list_positions(user.tenant_id, user.user_id, env)
    return ok(
        [
            PositionOut(
                symbol=position.symbol,
                volume=position.volume,
                pnl=position.pnl,
                avg_price=position.avg_price,
                frozen_volume=position.frozen_volume,
                env=position.env,
            )
            for position in positions
        ]
    )
