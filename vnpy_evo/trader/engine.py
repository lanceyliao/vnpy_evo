from datetime import datetime
from threading import Thread
from queue import Queue, Empty

import requests
from loguru import logger

from vnpy.trader.engine import (
    BaseEngine,
    EventEngine,
    MainEngine as OriginalMainEngine,
    OmsEngine,
    EmailEngine,
    Event,
    EVENT_LOG,
    Path,
    get_folder_path
)

from .object import LogData
from .setting import SETTINGS


class MainEngine(OriginalMainEngine):
    """New main engine of VeighNa Evo"""

    def init_engines(self) -> None:
        """
        Init all engines.
        """
        self.add_engine(LogEngine)
        oms_engine: OmsEngine = self.add_engine(OmsEngine)
        self.get_tick: Callable[[str], TickData | None] = oms_engine.get_tick
        self.get_order: Callable[[str], OrderData | None] = oms_engine.get_order
        self.get_trade: Callable[[str], TradeData | None] = oms_engine.get_trade
        self.get_position: Callable[[str], PositionData | None] = oms_engine.get_position
        self.get_account: Callable[[str], AccountData | None] = oms_engine.get_account
        self.get_contract: Callable[[str], ContractData | None] = oms_engine.get_contract
        self.get_quote: Callable[[str], QuoteData | None] = oms_engine.get_quote
        self.get_all_ticks: Callable[[], list[TickData]] = oms_engine.get_all_ticks
        self.get_all_orders: Callable[[], list[OrderData]] = oms_engine.get_all_orders
        self.get_all_trades: Callable[[], list[TradeData]] = oms_engine.get_all_trades
        self.get_all_positions: Callable[[], list[PositionData]] = oms_engine.get_all_positions
        self.get_all_accounts: Callable[[], list[AccountData]] = oms_engine.get_all_accounts
        self.get_all_contracts: Callable[[], list[ContractData]] = oms_engine.get_all_contracts
        self.get_all_quotes: Callable[[], list[QuoteData]] = oms_engine.get_all_quotes
        self.get_all_active_orders: Callable[[], list[OrderData]] = oms_engine.get_all_active_orders
        self.get_all_active_quotes: Callable[[], list[QuoteData]] = oms_engine.get_all_active_quotes
        self.update_order_request: Callable[[OrderRequest, str, str], None] = oms_engine.update_order_request
        self.convert_order_request: Callable[[OrderRequest, str, bool, bool], list[OrderRequest]] = oms_engine.convert_order_request
        self.get_converter: Callable[[str], OffsetConverter | None] = oms_engine.get_converter
        self.add_engine(EmailEngine)
        self.add_engine(TelegramEngine)


class LogEngine(BaseEngine):
    """Use loguru instead of logging"""

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine) -> None:
        """"""
        super().__init__(main_engine, event_engine, "log")

        self.active = SETTINGS["log.active"]
        self.level: int = SETTINGS["log.level"]
        self.format: str = "{time}  {level}: {message}"

        if not SETTINGS["log.console"]:
            logger.remove()     # Remove default stderr output

        if SETTINGS["log.file"]:
            today_date: str = datetime.now().strftime("%Y%m%d")
            filename: str = f"vt_{today_date}.log"
            log_path: Path = get_folder_path("log")
            file_path: Path = log_path.joinpath(filename)

            logger.add(
                sink=file_path,
                level=self.level,
                retention="4 weeks"
            )

        self.register_event()

    def register_event(self) -> None:
        """Register log event handler"""
        self.event_engine.register(EVENT_LOG, self.process_log_event)

    def process_log_event(self, event: Event) -> None:
        """Process log event"""
        if not self.active:
            return

        log: LogData = event.data
        logger.log(log.level, log.msg)


class TelegramEngine(BaseEngine):
    """Telegram message sending engine"""

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine) -> None:
        super().__init__(main_engine, event_engine, "telegram")

        self.active: bool = SETTINGS.get("telegram.active", False)
        self.token: str = SETTINGS.get("telegram.token", "")
        self.chat: str = SETTINGS.get("telegram.chat", "")
        self.url: str = f"https://api.telegram.org/bot{self.token}/sendMessage"

        self.proxies: dict[str, str] = {}
        proxy: str = SETTINGS.get("telegram.proxy", "")
        if proxy:
            self.proxies["http"] = proxy
            self.proxies["https"] = proxy

        self.thread: Thread = Thread(target=self.run, daemon=True)
        self.queue: Queue = Queue()

        if self.active:
            self.register_event()
            self.thread.start()

    def register_event(self) -> None:
        """Register event handler"""
        self.event_engine.register(EVENT_LOG, self.process_log_event)

    def process_log_event(self, event: Event) -> None:
        """Process log event"""
        log: LogData = event.data

        msg = f"{log.time}\t[{log.gateway_name}] {log.msg}"
        self.queue.put(msg)

    def close(self) -> None:
        """Stop task thread"""
        if not self.active:
            return

        self.active = False
        self.thread.join()

    def run(self) -> None:
        """Task thread loop"""
        while self.active:
            try:
                msg: str = self.queue.get(block=True, timeout=1)
                self.send_msg(msg)
            except Empty:
                pass

    def send_msg(self, msg: str) -> dict:
        """Sending message"""
        data: dict = {
            "chat_id": self.chat,
            "text": msg
        }

        # 发送请求
        try:
            r: requests.Response = requests.post(self.url, data=data)
            return r.json()
        except Exception:
            return None
