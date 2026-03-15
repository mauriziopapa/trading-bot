"""
Bitget Trading Bot — Main Orchestrator v10
Production-grade stability build
"""

import os
import threading
import time
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

from trading_bot.dashboard.server import app

try:
    from trading_bot.dashboard.state_writer import write_state
    DASHBOARD_ENABLED = True
except:
    DASHBOARD_ENABLED = False


ENTRY_DELAY_SECONDS = 2
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
        colorize=True
    )

    def _dashboard_log(msg):

        if _bot_ref is None:
            return

        _bot_ref._recent_logs.append({
            "ts": msg.record["time"].isoformat(),
            "level": msg.record["level"].name,
            "msg": msg.record["message"]
        })

        _bot_ref._recent_logs = _bot_ref._recent_logs[-200:]

    logger.add(_dashboard_log, level="DEBUG")


# ==========================================================
# BOT CLASS
# ==========================================================

class TradingBot:

    def __init__(self):

        self.exchange = BitgetExchange()
        self.risk = RiskManager()
        self.notifier = TelegramNotifier()
        self.db = DB()

        self._last_trade = {}
        self._recent_logs = []

        self._running = True

        self.strategies = []

        if settings.ENABLE_RSI_MACD:
            self.strategies.append(RSIMACDStrategy())

        if settings.ENABLE_BOLLINGER:
            self.strategies.append(BollingerStrategy())

        if settings.ENABLE_BREAKOUT:
            self.strategies.append(BreakoutStrategy())

        if settings.ENABLE_SCALPING:
            self.strategies.append(ScalpingStrategy())


# ==========================================================
# START
# ==========================================================

    def start(self):

        global _bot_ref
        _bot_ref = self

        _setup_logger()

        logger.info("Starting Trading Bot v10")

        self.exchange.initialize()

        self.db.connect()

        self.risk.recover_from_db()

        logger.info(f"[BOOT] recovered trades: {len(self.risk.all_open_trades())}")

        self._sync_balance()

        self._start_dashboard()

        try:
            self.notifier.send("🤖 Trading Bot avviato")
        except Exception:
            logger.warning("Telegram notifier unavailable")

        self._setup_scheduler()

        logger.info("Bot operativo")

        while self._running:

            schedule.run_pending()

            if DASHBOARD_ENABLED:

                write_state({
                    "open_trades": self.risk.all_open_trades(),
                    "logs": self._recent_logs,
                    "stats": self.risk.stats()
                })

            time.sleep(2)


# ==========================================================
# DASHBOARD
# ==========================================================

    def _start_dashboard(self):

        try:

            port = int(os.environ.get("PORT", settings.DASHBOARD_PORT))

            config = uvicorn.Config(
                app,
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

        except Exception as e:

            logger.warning(f"Dashboard start error: {e}")


# ==========================================================
# SCHEDULER
# ==========================================================

    def _setup_scheduler(self):

        schedule.every().minute.do(self._scan_swing_if_candle_closed)

        schedule.every().minute.do(self._scan_breakout_if_candle_closed)

        if settings.ENABLE_SCALPING:
            schedule.every(45).seconds.do(self._scan_scalping)

        schedule.every(10).seconds.do(self._monitor_positions)

        schedule.every(60).seconds.do(self._sync_positions)

        schedule.every(5).minutes.do(self._sync_balance)

        schedule.every(10).minutes.do(self._auto_rebalance)

        schedule.every(1).hours.do(self._health_check)


# ==========================================================
# MARKET SCANS
# ==========================================================

    def _scan_swing_if_candle_closed(self):

        now = datetime.now(timezone.utc)

        if now.minute % 15 == 0 and now.second < 4:

            time.sleep(ENTRY_DELAY_SECONDS)

            self._scan_swing()


    def _scan_breakout_if_candle_closed(self):

        now = datetime.now(timezone.utc)

        if now.minute % 5 == 0 and now.second < 4:

            time.sleep(ENTRY_DELAY_SECONDS)

            self._scan_breakout()


# ==========================================================
# STRATEGY EXECUTION
# ==========================================================

    def _scan_swing(self):

        for market, symbols in self._market_symbol_pairs():

            for symbol in symbols:

                try:

                    df = ohlcv_to_df(
                        self.exchange.fetch_ohlcv(
                            symbol,
                            settings.TF_SWING,
                            300,
                            market
                        )
                    )

                    for strategy in self.strategies:

                        signal = strategy.analyze(df, symbol, market)

                        if signal:
                            self._process_signal(signal)

                except Exception as e:

                    logger.error(f"[SWING] {symbol} {e}")


    def _scan_breakout(self):

        strategy = next((s for s in self.strategies if s.NAME == "BREAKOUT"), None)

        if not strategy:
            return

        for market, symbols in self._market_symbol_pairs():

            for symbol in symbols:

                try:

                    df = ohlcv_to_df(
                        self.exchange.fetch_ohlcv(
                            symbol,
                            settings.TF_BREAKOUT,
                            150,
                            market
                        )
                    )

                    signal = strategy.analyze(df, symbol, market)

                    if signal:
                        self._process_signal(signal)

                except Exception as e:

                    logger.error(f"[BREAKOUT] {symbol} {e}")


# ==========================================================
# SIGNAL PROCESSING
# ==========================================================

    def _process_signal(self, signal):

        now = time.time()

        last = self._last_trade.get(signal.symbol, 0)

        if now - last < TRADE_COOLDOWN:
            return

        ok, reason = self.risk.can_trade_symbol(signal.symbol, signal.market)

        if not ok:
            return

        try:

            self._execute_signal(signal)

            self._last_trade[signal.symbol] = time.time()

        except Exception as e:

            logger.error(f"[SIGNAL] {signal.symbol} {e}")


# ==========================================================
# EXECUTE ORDER
# ==========================================================

    def _execute_signal(self, signal):

        time.sleep(ENTRY_DELAY_SECONDS)

        if signal.confidence < settings.MIN_CONFIDENCE:
            return

        balance = self.exchange.get_usdt_balance(signal.market)

        size = self.risk.position_size(
            balance,
            signal.entry,
            signal.stop_loss,
            signal.atr,
            signal.market,
            1.0,
            signal.symbol
        )

        if size <= 0:
            return

        params = {}

        if signal.market == "futures":
            params = {"reduceOnly": False}

        order = self.exchange.create_market_order(
            symbol=signal.symbol,
            side=signal.side,
            amount=size,
            market=signal.market,
            params=params
        )

        if not order:
            return

        trade_data = {
            "entry": signal.entry,
            "size": size,
            "side": signal.side,
            "stop_loss": signal.stop_loss,
            "take_profit": signal.take_profit,
            "atr": signal.atr,
            "pyramid_level": 0
        }

        self.risk.register_open(signal.symbol, trade_data, signal.market)

        logger.info(f"TRADE OPEN {signal.symbol} {signal.side} size={size}")

        try:
            self.notifier.send(
                f"🟢 TRADE OPEN {signal.symbol}\nSide: {signal.side}\nSize: {size}"
            )
        except:
            pass


# ==========================================================
# POSITION MONITOR
# ==========================================================

    def _monitor_positions(self):

        for trade in self.risk.all_open_trades():

            symbol = trade["symbol"]
            market = trade["market"]

            try:

                ticker = self.exchange.fetch_ticker(symbol, market)

                if not ticker:
                    continue

                price = float(ticker.get("last") or ticker.get("close"))

                close, reason = self.risk.should_close(trade, price)

                if close:
                    self._close_position(symbol, market, trade, price, reason)

            except Exception as e:

                logger.error(f"[MONITOR] {symbol} {e}")


# ==========================================================
# CLOSE POSITION
# ==========================================================

    def _close_position(self, symbol, market, trade, price, reason):

        try:

            side = "sell" if trade["side"] == "buy" else "buy"

            order = self.exchange.create_market_order(
                symbol=symbol,
                side=side,
                amount=trade["size"],
                market=market,
                params={"reduceOnly": True} if market == "futures" else {}
            )

            if not order:
                return

            entry = trade.get("entry", price)

            if trade["side"] == "buy":
                pnl_pct = (price - entry) / entry * 100
            else:
                pnl_pct = (entry - price) / entry * 100

            self.risk.register_close(symbol, pnl_pct, market, reason)

            logger.info(f"TRADE CLOSED {symbol} pnl={pnl_pct:.2f}%")

        except Exception as e:

            logger.error(f"close error {symbol} {e}")

            if "No position to close" in str(e):

                self.risk.force_close(symbol, market)


# ==========================================================
# SYNC POSITIONS
# ==========================================================

    def _sync_positions(self):

        try:

            exchange_positions = self.exchange.fetch_positions()

            exchange_symbols = {p["symbol"] for p in exchange_positions}

            for trade in self.risk.all_open_trades():

                sym = trade["symbol"]
                market = trade["market"]

                if market != "futures":
                    continue

                if sym not in exchange_symbols:

                    logger.warning(f"[SYNC] removing phantom position {sym}")

                    self.risk.force_close(sym, market)

        except Exception as e:

            logger.warning(f"[SYNC] error {e}")


# ==========================================================
# BALANCE
# ==========================================================

    def _sync_balance(self):

        try:

            s = self.exchange.get_usdt_balance("spot")
            f = self.exchange.get_usdt_balance("futures")

            self.risk.session_start_balance = s + f
            self.risk.peak_balance = s + f

        except Exception as e:

            logger.warning(e)


# ==========================================================
# REBALANCE
# ==========================================================

    def _auto_rebalance(self):

        try:

            self.exchange.auto_rebalance(keep_spot_usdt=5)

        except Exception as e:

            logger.warning(f"[REBALANCE] {e}")


# ==========================================================
# HEALTH CHECK
# ==========================================================

    def _health_check(self):

        logger.info("health check")


# ==========================================================
# SYMBOLS
# ==========================================================

    def _market_symbol_pairs(self):

        pairs = []

        if "spot" in settings.MARKET_TYPES:
            pairs.append(("spot", settings.SPOT_SYMBOLS))

        if "futures" in settings.MARKET_TYPES:
            pairs.append(("futures", settings.FUTURES_SYMBOLS))

        return pairs


if __name__ == "__main__":

    TradingBot().start()