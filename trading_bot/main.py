"""
Bitget Trading Bot — Main Orchestrator v8.1
════════════════════════════════════════════

Fix:
✓ dashboard log restored
✓ exchange close-position bug fix
✓ logger pipeline restored
✓ safer monitor loop
"""

import os
import time
import threading
import schedule
import uvicorn

from datetime import datetime, timezone
from loguru import logger

from trading_bot.config import settings
from trading_bot.utils.exchange import BitgetExchange
from trading_bot.utils.risk_manager import RiskManager
from trading_bot.utils.notifier import TelegramNotifier
from trading_bot.utils.indicators import ohlcv_to_df
from trading_bot.models.database import DB

from trading_bot.strategies.rsi_macd import RSIMACDStrategy
from trading_bot.strategies.bollinger import BollingerStrategy
from trading_bot.strategies.breakout import BreakoutStrategy
from trading_bot.strategies.scalping import ScalpingStrategy

from trading_bot.utils.sentiment_analyzer import SentimentAnalyzer
from trading_bot.utils.regime_detector import RegimeDetector
from trading_bot.utils.emerging_scanner import EmergingScanner


ENTRY_DELAY = 2
TRADE_COOLDOWN = 1800


_bot_ref = None


# ==========================================================
# LOGGER SETUP
# ==========================================================

def _setup_logger():

    os.makedirs("logs", exist_ok=True)

    logger.remove()

    logger.add(
        "logs/bot.log",
        rotation="50 MB",
        retention="14 days",
        level=settings.LOG_LEVEL,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}"
    )

    logger.add(
        lambda msg: print(msg, end=""),
        level=settings.LOG_LEVEL,
        colorize=True,
    )

    def _dashboard_log(msg):

        if _bot_ref is None:
            return

        _bot_ref._recent_logs.append({
            "ts": msg.record["time"].isoformat(),
            "level": msg.record["level"].name,
            "msg": msg.record["message"]
        })

        _bot_ref._recent_logs = _bot_ref._recent_logs[-100:]

    logger.add(_dashboard_log, level="DEBUG")


try:
    from trading_bot.dashboard.state_writer import write_state
    DASHBOARD_ENABLED = True
except:
    DASHBOARD_ENABLED = False


# ==========================================================
# BOT
# ==========================================================

class TradingBot:

    def __init__(self):

        self.exchange = BitgetExchange()
        self.risk = RiskManager()
        self.db = DB()
        self.notifier = TelegramNotifier()

        self.sentiment = SentimentAnalyzer()
        self.regime = RegimeDetector()
        self.emerging = EmergingScanner()

        self._last_trade = {}
        self._recent_logs = []

        self.strategies = []

        if settings.ENABLE_RSI_MACD:
            self.strategies.append(RSIMACDStrategy())

        if settings.ENABLE_BOLLINGER:
            self.strategies.append(BollingerStrategy())

        if settings.ENABLE_BREAKOUT:
            self.strategies.append(BreakoutStrategy())

        if settings.ENABLE_SCALPING:
            self.strategies.append(ScalpingStrategy())

        self.running = True


# ==========================================================
# DASHBOARD
# ==========================================================

    def _start_dashboard(self):

        port = int(os.environ.get("PORT", settings.DASHBOARD_PORT))

        config = uvicorn.Config(
            "trading_bot.dashboard.server:app",
            host="0.0.0.0",
            port=port,
            log_level="warning"
        )

        server = uvicorn.Server(config)

        thread = threading.Thread(
            target=server.run,
            daemon=True
        )

        thread.start()

        logger.info(f"[DASHBOARD] running on port {port}")


# ==========================================================
# START
# ==========================================================

    def start(self):

        global _bot_ref

        _bot_ref = self

        _setup_logger()

        logger.info("Starting Trading Bot v8.1")

        if DASHBOARD_ENABLED:
            self._start_dashboard()

        self.exchange.initialize()
        self.db.connect()
        self.risk.recover_from_db()

        self._sync_balance()

        self._send_startup()

        schedule.every(30).seconds.do(self._scan_market)

        schedule.every(8).seconds.do(self._monitor_positions)

        schedule.every(10).minutes.do(self._scan_emerging)

        schedule.every(10).minutes.do(self._auto_rebalance)

        if DASHBOARD_ENABLED:
            schedule.every(20).seconds.do(lambda: write_state(self))

        while self.running:

            try:

                schedule.run_pending()

                time.sleep(1)

            except Exception as e:

                logger.error(f"main loop error {e}")

                time.sleep(5)


# ==========================================================
# MARKET SCAN
# ==========================================================

    def _scan_market(self):

        pairs = self._market_symbol_pairs()

        for market, symbols in pairs:

            for sym in symbols:

                try:

                    df = ohlcv_to_df(
                        self.exchange.fetch_ohlcv(sym, settings.TF_SWING, 200, market)
                    )

                    if df is None:
                        continue

                    for strategy in self.strategies:

                        sig = strategy.analyze(df, sym, market)

                        if sig:
                            self._process_signal(sig)

                except Exception as e:

                    logger.debug(f"scan error {sym} {e}")


# ==========================================================
# SIGNAL
# ==========================================================

    def _process_signal(self, signal):

        key = f"{signal.market}:{signal.symbol}"

        now = time.time()

        if now - self._last_trade.get(key, 0) < TRADE_COOLDOWN:
            return

        ok, reason = self.risk.can_trade_symbol(signal.symbol, signal.market)

        if not ok:
            return

        try:

            self._execute_signal(signal)

            self._last_trade[key] = time.time()

        except Exception as e:

            logger.error(f"signal error {e}")


# ==========================================================
# EXECUTION
# ==========================================================

    def _execute_signal(self, signal):

        time.sleep(ENTRY_DELAY)

        if signal.confidence < settings.MIN_CONFIDENCE:
            return

        balance = self.exchange.get_usdt_balance(signal.market)

        if balance < 10:
            return

        size = self.risk.position_size(
            balance=balance,
            entry=signal.entry,
            stop_loss=signal.stop_loss,
            atr=signal.atr,
            market=signal.market,
            risk_multiplier=1.0,
            symbol=signal.symbol
        )

        if size <= 0:
            return

        params = {}

        if signal.market == "futures":
            params = {"marginMode": settings.MARGIN_MODE}

        order = self.exchange.create_market_order(
            symbol=signal.symbol,
            side=signal.side,
            amount=size,
            market=signal.market,
            params=params
        )

        if not order:
            return

        logger.info(f"TRADE OPEN {signal.symbol}")


# ==========================================================
# MONITOR
# ==========================================================

    def _monitor_positions(self):

        for trade in self.risk.all_open_trades():

            sym = trade["symbol"]
            market = trade["market"]

            try:

                ticker = self.exchange.fetch_ticker(sym, market)

                if ticker is None:
                    continue

                price = float(ticker["last"])

                ok, reason = self.risk.should_close(trade, price)

                if ok:
                    self._close_position(sym, market, trade, price, reason)

            except Exception as e:

                logger.error(f"monitor error {sym} {e}")


# ==========================================================
# CLOSE POSITION FIX
# ==========================================================

    def _close_position(self, sym, market, trade, price, reason):

        try:

            # verifica posizione reale exchange
            pos = self.exchange.get_position(sym)

            if not pos or pos["size"] == 0:

                logger.warning(f"[CLOSE SKIP] no position on exchange {sym}")

                self.risk.force_close(sym, market)

                return

            side = "sell" if trade["side"] == "buy" else "buy"

            order = self.exchange.create_market_order(
                symbol=sym,
                side=side,
                amount=trade["size"],
                market=market,
                params={"reduceOnly": True} if market == "futures" else {}
            )

            if not order:
                return

            logger.info(f"TRADE CLOSED {sym}")

        except Exception as e:

            logger.error(f"close error {sym} {e}")


# ==========================================================
# UTILS
# ==========================================================

    def _send_startup(self):

        try:

            s = self.exchange.get_usdt_balance("spot")
            f = self.exchange.get_usdt_balance("futures")

            self.notifier.startup(
                settings.TRADING_MODE,
                settings.SPOT_SYMBOLS,
                settings.FUTURES_SYMBOLS,
                s,
                f
            )

        except:
            pass


    def _sync_balance(self):

        try:

            s = self.exchange.get_usdt_balance("spot")
            f = self.exchange.get_usdt_balance("futures")

            self.risk.session_start_balance = s + f
            self.risk.peak_balance = s + f

        except:
            pass


    def _scan_emerging(self):

        try:
            self.emerging.scan()
        except:
            pass


    def _auto_rebalance(self):

        try:
            self.exchange.auto_rebalance(keep_spot_usdt=5)
        except:
            pass


    def _market_symbol_pairs(self):

        pairs = []

        if "spot" in settings.MARKET_TYPES:
            pairs.append(("spot", settings.SPOT_SYMBOLS))

        if "futures" in settings.MARKET_TYPES:
            pairs.append(("futures", settings.FUTURES_SYMBOLS))

        return pairs


if __name__ == "__main__":

    TradingBot().start()
