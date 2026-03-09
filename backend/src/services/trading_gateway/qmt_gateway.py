"""QMT/MiniQMT 交易网关 (迅投 xtquant)

TODO: 安装 xtquant SDK 后接入真实下单/查仓/行情订阅。
      当前为框架占位，所有交易操作返回模拟结果。
      真实接入步骤:
        1. pip install xtquant
        2. 启动 MiniQMT 客户端
        3. 替换 connect/send_order/query_positions 中的 Mock 逻辑

配置字段：
    - account_type: MiniQMT / QMT 完整版
    - client_path: MiniQMT 安装路径
    - account: 资金账号
    - password: 交易密码
    - market: 全部 / 沪市 / 深市
"""

import logging
import uuid
from typing import Any, Dict, Iterable

from src.services.trading_gateway.base import TradingGateway

logger = logging.getLogger(__name__)


class QmtGateway(TradingGateway):
    """QMT/MiniQMT Gateway — 迅投量化交易接口封装。"""

    def __init__(self, config: Dict[str, Any] | None = None):
        super().__init__("qmt", config)
        self.account_type: str = (config or {}).get("account_type", "MiniQMT")
        self.client_path: str = (config or {}).get("client_path", "")
        self.account: str = (config or {}).get("account", "")
        self.market: str = (config or {}).get("market", "全部")

    # ------------------------------------------------------------------
    # 连接管理
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """
        连接 QMT 客户端。
        真实实现需要：
            from xtquant import xttrader
            self._session = xttrader.XtQuantTrader(self.client_path, session_id)
            self._session.start()
            self._session.connect()
        """
        logger.info("QMT connect: account_type=%s, account=%s", self.account_type, self.account)
        # Mock: 标记为已连接
        self.connected = True
        for p in self.plugins:
            p.on_connect()

    async def close(self) -> None:
        logger.info("QMT close")
        self.connected = False

    async def test_connection(self) -> bool:
        """测试 QMT 是否可连接。返回 True/False。"""
        try:
            await self.connect()
            result = self.connected
            await self.close()
            return result
        except Exception as exc:
            logger.warning("QMT test_connection failed: %s", exc)
            return False

    # ------------------------------------------------------------------
    # 交易操作
    # ------------------------------------------------------------------

    async def send_order(self, order: dict[str, Any]) -> str:
        """
        发送委托。
        真实实现：
            xttrader.order_stock(account, stock_code, order_type, volume, strategy, price)
        """
        if not self.connected:
            await self.connect()
        for p in self.plugins:
            p.on_order(order)
        order_id = f"QMT{uuid.uuid4().hex[:12].upper()}"
        logger.info("QMT send_order: %s -> %s", order, order_id)
        return order_id

    async def cancel_order(self, order_id: str) -> None:
        logger.info("QMT cancel_order: %s", order_id)

    async def query_positions(self) -> Iterable[dict[str, Any]]:
        """
        查询持仓。
        真实实现：xttrader.query_stock_positions(account)
        """
        return []

    async def query_account(self) -> dict[str, Any]:
        """
        查询账户资金。
        真实实现：xttrader.query_stock_asset(account)
        """
        return {"balance": 0.0, "available": 0.0, "frozen": 0.0}

    async def subscribe(self, symbols: list[str]) -> None:
        """
        订阅行情。
        真实实现：xtdata.subscribe_quote(symbols, period='tick')
        """
        logger.info("QMT subscribe: %s", symbols)
