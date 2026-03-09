"""OpenCTP (TTS) 交易网关 — 基于 CTP 协议的多券商交易接口

服务器端部署，无需本地 GUI 客户端。通过 TCP 连接 OpenCTP 前置服务器。
支持模拟交易（7x24）和实盘交易（切换前置地址即可）。

模拟环境:
    - BrokerID: 9999
    - 交易前置: tcp://trading.openctp.cn:30001  (7x24)
    - 行情前置: tcp://trading.openctp.cn:30011  (7x24)
    - AppID: client_panda_v1.0
    - AuthCode: 0000000000000000

配置字段:
    - front_td:   交易前置地址
    - front_md:   行情前置地址 (可选)
    - broker_id:  BrokerID
    - user_id:    用户名 / 资金账号
    - password:   密码
    - app_id:     AppID (认证用)
    - auth_code:  AuthCode (认证用)
    - env:        sim / live (模拟 / 实盘)
"""

import asyncio
import logging
import os
import threading
import time
import uuid
from typing import Any, Dict, Iterable, List, Optional

from src.services.trading_gateway.base import TradingGateway

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# OpenCTP 模拟环境默认值
# ---------------------------------------------------------------------------
_DEFAULT_SIM = {
    "front_td": "tcp://trading.openctp.cn:30001",
    "front_md": "tcp://trading.openctp.cn:30011",
    "broker_id": "9999",
    "app_id": "client_panda_v1.0",
    "auth_code": "0000000000000000",
}


def _try_import_ctp():
    """延迟导入 openctp_ctp，若未安装则返回 None。"""
    try:
        from openctp_ctp import thosttraderapi as tdapi
        return tdapi
    except ImportError:
        logger.warning("openctp_ctp 未安装，请执行: pip install openctp-ctp")
        return None


# ---------------------------------------------------------------------------
# 动态创建 SPI 子类（仅继承 SWIG 基类，所有回调直接定义）
# ---------------------------------------------------------------------------
def _make_spi_class(tdapi_mod):
    """根据 openctp_ctp 动态创建 SPI 继承类。

    不使用多重继承以避免 SWIG 回调分派问题。
    所有回调方法直接定义在类体中。
    """

    class TdSpi(tdapi_mod.CThostFtdcTraderSpi):
        def __init__(self):
            super().__init__()
            self._tdapi = tdapi_mod
            self.api = None
            self.broker_id = ""
            self.user_id = ""
            self.password = ""
            self.app_id = ""
            self.auth_code = ""

            # 状态
            self.connected = False
            self.logged_in = False
            self.login_error: Optional[str] = None
            self.front_id = 0
            self.session_id = 0
            self.order_ref = 1
            self.trading_day = ""

            # 同步事件
            self._evt_connected = threading.Event()
            self._evt_login = threading.Event()
            self._evt_query = threading.Event()

            # 查询结果暂存
            self._positions: List[Dict] = []
            self._account: Dict = {}

        # -- 连接回调 --
        def OnFrontConnected(self):
            logger.info("OpenCTP: OnFrontConnected")
            self.connected = True
            self._evt_connected.set()
            req = self._tdapi.CThostFtdcReqAuthenticateField()
            req.BrokerID = self.broker_id
            req.UserID = self.user_id
            req.AppID = self.app_id
            req.AuthCode = self.auth_code
            self.api.ReqAuthenticate(req, 0)

        def OnFrontDisconnected(self, nReason):
            logger.warning("OpenCTP: OnFrontDisconnected reason=%s", nReason)
            self.connected = False
            self.logged_in = False
            self.login_error = self.login_error or f"前置断开 (reason={nReason})"
            self._evt_login.set()

        # -- 认证回调 --
        def OnRspAuthenticate(self, pRspAuthenticateField, pRspInfo, nRequestID, bIsLast):
            if pRspInfo and pRspInfo.ErrorID != 0:
                self.login_error = f"认证失败: {pRspInfo.ErrorMsg}"
                logger.error("OpenCTP: %s", self.login_error)
                self._evt_login.set()
                return
            logger.info("OpenCTP: Authenticate OK, requesting login...")
            req = self._tdapi.CThostFtdcReqUserLoginField()
            req.BrokerID = self.broker_id
            req.UserID = self.user_id
            req.Password = self.password
            req.UserProductInfo = "panda"  # char[11] 限制
            self.api.ReqUserLogin(req, 0)

        # -- 登录回调 --
        def OnRspUserLogin(self, pRspUserLogin, pRspInfo, nRequestID, bIsLast):
            if pRspInfo and pRspInfo.ErrorID != 0:
                self.login_error = f"登录失败: {pRspInfo.ErrorMsg}"
                logger.error("OpenCTP: %s", self.login_error)
                self._evt_login.set()
                return
            self.logged_in = True
            self.login_error = None
            self.trading_day = pRspUserLogin.TradingDay
            self.front_id = pRspUserLogin.FrontID
            self.session_id = pRspUserLogin.SessionID
            logger.info(
                "OpenCTP: Login OK. TradingDay=%s FrontID=%s SessionID=%s",
                self.trading_day, self.front_id, self.session_id,
            )
            self._evt_login.set()

        # -- 委托回调 --
        def OnRspOrderInsert(self, pInputOrder, pRspInfo, nRequestID, bIsLast):
            if pRspInfo and pRspInfo.ErrorID != 0:
                logger.error("OpenCTP: OrderInsert failed: %s", pRspInfo.ErrorMsg)

        def OnRtnOrder(self, pOrder):
            if pOrder:
                logger.info(
                    "OpenCTP: OnRtnOrder %s %s status=%s msg=%s",
                    pOrder.InstrumentID, pOrder.OrderSysID,
                    pOrder.OrderStatus, pOrder.StatusMsg,
                )

        def OnRtnTrade(self, pTrade):
            if pTrade:
                logger.info(
                    "OpenCTP: OnRtnTrade %s price=%s vol=%s",
                    pTrade.InstrumentID, pTrade.Price, pTrade.Volume,
                )

        # -- 查询持仓回调 --
        def OnRspQryInvestorPosition(self, pPos, pRspInfo, nRequestID, bIsLast):
            if pRspInfo and pRspInfo.ErrorID != 0:
                logger.error("OpenCTP: QryPosition failed: %s", pRspInfo.ErrorMsg)
            if pPos and pPos.InstrumentID:
                self._positions.append({
                    "symbol": pPos.InstrumentID,
                    "exchange": pPos.ExchangeID,
                    "direction": "long" if pPos.PosiDirection == ord("2") else "short",
                    "volume": pPos.Position,
                    "yesterday": pPos.YdPosition,
                    "today": pPos.TodayPosition,
                    "cost": pPos.PositionCost,
                    "margin": pPos.UseMargin,
                })
            if bIsLast:
                self._evt_query.set()

        # -- 查询资金回调 --
        def OnRspQryTradingAccount(self, pAccount, pRspInfo, nRequestID, bIsLast):
            if pRspInfo and pRspInfo.ErrorID != 0:
                logger.error("OpenCTP: QryAccount failed: %s", pRspInfo.ErrorMsg)
            if pAccount:
                self._account = {
                    "balance": pAccount.Balance,
                    "available": pAccount.Available,
                    "frozen_margin": pAccount.FrozenMargin,
                    "commission": pAccount.Commission,
                    "close_profit": pAccount.CloseProfit,
                    "currency": pAccount.CurrencyID,
                }
            if bIsLast:
                self._evt_query.set()

    return TdSpi


# ---------------------------------------------------------------------------
# OpenCTP Gateway 实现
# ---------------------------------------------------------------------------
class OpenctpGateway(TradingGateway):
    """OpenCTP (TTS) 网关 — 服务器端 CTP 协议交易接口。"""

    def __init__(self, config: Dict[str, Any] | None = None):
        super().__init__("openctp", config)
        cfg = config or {}
        self.env: str = cfg.get("env", "sim")  # sim / live
        defaults = _DEFAULT_SIM if self.env == "sim" else {}
        self.front_td: str = cfg.get("front_td") or defaults.get("front_td", "")
        self.front_md: str = cfg.get("front_md") or defaults.get("front_md", "")
        self.broker_id: str = cfg.get("broker_id") or defaults.get("broker_id", "")
        self.user_id: str = cfg.get("user_id", "")
        self.password: str = cfg.get("password", "")
        self.app_id: str = cfg.get("app_id") or defaults.get("app_id", "")
        self.auth_code: str = cfg.get("auth_code") or defaults.get("auth_code", "")

        self._tdapi = None
        self._spi: Optional[Any] = None  # TdSpi instance (动态创建的类)
        self._api = None

    # ------------------------------------------------------------------
    # 连接管理
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """连接 OpenCTP 交易前置并登录。"""
        tdapi = _try_import_ctp()
        if tdapi is None:
            raise RuntimeError("openctp-ctp 未安装")
        self._tdapi = tdapi

        SpiClass = _make_spi_class(tdapi)
        spi = SpiClass()
        spi.broker_id = self.broker_id
        spi.user_id = self.user_id
        spi.password = self.password
        spi.app_id = self.app_id
        spi.auth_code = self.auth_code

        # 创建 API（使用临时目录存放 .con 文件）
        flow_path = os.path.join(
            os.environ.get("TEMP", "/tmp"),
            f"openctp_td_{self.user_id}_{os.getpid()}",
            "",  # trailing separator
        )
        os.makedirs(flow_path, exist_ok=True)

        api = tdapi.CThostFtdcTraderApi.CreateFtdcTraderApi(flow_path)
        api.RegisterSpi(spi)
        api.RegisterFront(self.front_td)
        api.SubscribePrivateTopic(tdapi.THOST_TERT_QUICK)
        api.SubscribePublicTopic(tdapi.THOST_TERT_QUICK)

        spi.api = api
        self._spi = spi
        self._api = api

        # Init() 在内部线程中运行，我们等待回调
        api.Init()

        # 等待登录完成（最多 10 秒）
        login_ok = await asyncio.to_thread(spi._evt_login.wait, 10.0)
        if not login_ok or not spi.logged_in:
            err = spi.login_error or "连接超时"
            raise RuntimeError(f"OpenCTP 登录失败: {err}")

        self.connected = True
        for p in self.plugins:
            p.on_connect()
        logger.info("OpenCTP: 连接成功 TradingDay=%s", spi.trading_day)

    async def close(self) -> None:
        """断开连接并释放资源。"""
        if self._api:
            try:
                self._api.Release()
            except Exception:
                pass
            self._api = None
        self._spi = None
        self.connected = False
        logger.info("OpenCTP: 已断开")

    async def test_connection(self) -> Dict[str, Any]:
        """测试连接，返回详细结果。"""
        t0 = time.time()
        try:
            await self.connect()
            latency = int((time.time() - t0) * 1000)
            trading_day = self._spi.trading_day if self._spi else ""
            await self.close()
            return {
                "success": True,
                "latency_ms": latency,
                "trading_day": trading_day,
                "message": f"连接成功, 交易日={trading_day}",
            }
        except Exception as exc:
            latency = int((time.time() - t0) * 1000)
            await self.close()
            return {
                "success": False,
                "latency_ms": latency,
                "trading_day": "",
                "message": str(exc)[:300],
            }

    # ------------------------------------------------------------------
    # 交易操作
    # ------------------------------------------------------------------

    async def send_order(self, order: dict[str, Any]) -> str:
        """发送委托。

        order 字段:
            - symbol: 合约代码 (如 '600000')
            - exchange: 交易所 (SSE / SZSE / SHFE / DCE / CZCE / CFFEX)
            - direction: buy / sell
            - offset: open / close / close_today
            - price: 委托价格
            - volume: 委托数量
        """
        if not self.connected or not self._spi or not self._api:
            await self.connect()

        tdapi = self._tdapi
        spi = self._spi

        req = tdapi.CThostFtdcInputOrderField()
        req.BrokerID = self.broker_id
        req.UserID = self.user_id
        req.InvestorID = self.user_id
        req.ExchangeID = order.get("exchange", "SSE")
        req.InstrumentID = order.get("symbol", "")

        # 方向
        if order.get("direction") == "buy":
            req.Direction = tdapi.THOST_FTDC_D_Buy
        else:
            req.Direction = tdapi.THOST_FTDC_D_Sell

        # 开平
        offset = order.get("offset", "open")
        if offset == "close":
            req.CombOffsetFlag = tdapi.THOST_FTDC_OF_Close
        elif offset == "close_today":
            req.CombOffsetFlag = tdapi.THOST_FTDC_OF_CloseToday
        else:
            req.CombOffsetFlag = tdapi.THOST_FTDC_OF_Open

        req.CombHedgeFlag = tdapi.THOST_FTDC_HF_Speculation
        req.OrderPriceType = tdapi.THOST_FTDC_OPT_LimitPrice
        req.LimitPrice = float(order.get("price", 0))
        req.VolumeTotalOriginal = int(order.get("volume", 0))
        req.TimeCondition = tdapi.THOST_FTDC_TC_GFD
        req.VolumeCondition = tdapi.THOST_FTDC_VC_AV
        req.MinVolume = 1
        req.ForceCloseReason = tdapi.THOST_FTDC_FCC_NotForceClose
        req.ContingentCondition = tdapi.THOST_FTDC_CC_Immediately

        order_ref = str(spi.order_ref)
        spi.order_ref += 1
        req.OrderRef = order_ref

        self._api.ReqOrderInsert(req, 0)

        for p in self.plugins:
            p.on_order(order)

        order_id = f"OCTP_{spi.front_id}_{spi.session_id}_{order_ref}"
        logger.info("OpenCTP: send_order %s -> %s", order, order_id)
        return order_id

    async def cancel_order(self, order_id: str) -> None:
        """撤销委托。order_id 格式: OCTP_{FrontID}_{SessionID}_{OrderRef}"""
        if not self.connected or not self._api:
            return

        tdapi = self._tdapi
        parts = order_id.split("_")
        front_id = int(parts[1]) if len(parts) > 1 else 0
        session_id = int(parts[2]) if len(parts) > 2 else 0
        order_ref = parts[3] if len(parts) > 3 else ""

        req = tdapi.CThostFtdcInputOrderActionField()
        req.BrokerID = self.broker_id
        req.UserID = self.user_id
        req.InvestorID = self.user_id
        req.FrontID = front_id
        req.SessionID = session_id
        req.OrderRef = order_ref
        req.ActionFlag = tdapi.THOST_FTDC_AF_Delete

        self._api.ReqOrderAction(req, 0)
        logger.info("OpenCTP: cancel_order %s", order_id)

    async def query_positions(self) -> Iterable[dict[str, Any]]:
        """查询持仓。"""
        if not self.connected or not self._spi or not self._api:
            return []

        tdapi = self._tdapi
        spi = self._spi
        spi._positions = []
        spi._evt_query.clear()

        req = tdapi.CThostFtdcQryInvestorPositionField()
        req.BrokerID = self.broker_id
        req.InvestorID = self.user_id
        self._api.ReqQryInvestorPosition(req, 0)

        await asyncio.to_thread(spi._evt_query.wait, 5.0)
        return spi._positions

    async def query_account(self) -> dict[str, Any]:
        """查询账户资金。"""
        if not self.connected or not self._spi or not self._api:
            return {"balance": 0}

        tdapi = self._tdapi
        spi = self._spi
        spi._account = {}
        spi._evt_query.clear()

        req = tdapi.CThostFtdcQryTradingAccountField()
        req.BrokerID = self.broker_id
        req.InvestorID = self.user_id
        self._api.ReqQryTradingAccount(req, 0)

        await asyncio.to_thread(spi._evt_query.wait, 5.0)
        return spi._account or {"balance": 0}

    async def subscribe(self, symbols: list[str]) -> None:
        """订阅行情（需要行情 API，此处为占位）。"""
        logger.info("OpenCTP: subscribe %s (行情订阅需 MdApi，暂未实现)", symbols)
