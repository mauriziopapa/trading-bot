"""
Bitget Trading Bot — Main Orchestrator v10 SNIPER MODE
Production Ready + Safe Execution + Anti Overtrading + Asset Filtering
"""

import os
import time
import schedule
import threading

from datetime import datetime
from loguru import logger

from trading_bot.config import settings
from trading_bot.utils.exchange import BitgetExchange
from trading_bot.utils.risk_manager import RiskManager
from trading_bot.utils.notifier import TelegramNotifier
from trading_bot.utils.indicators import ohlcv_to_df
from trading_bot.utils.profit_engine import ProfitEngine

from trading_bot.models.database import DB

from trading_bot.strategies.rsi_macd import RSIMACDStrategy
from trading_bot.strategies.bollinger import BollingerStrategy
from trading_bot.strategies.breakout import BreakoutStrategy
from trading_bot.strategies.scalping import ScalpingStrategy

from trading_bot.utils.emerging_scanner import EmergingScanner
from trading_bot.utils.regime_detector import RegimeDetector
from trading_bot.utils.sentiment_analyzer import SentimentAnalyzer


# ==========================================================
# SAFE FLOAT
# ==========================================================

def safe_float(x, default=0.0):
    try:
        return float(x) if x is not None else default
    except:
        return default


# ==========================================================
# DASHBOARD
# ==========================================================

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
        self.notifier = TelegramNotifier()
        self.db = DB()
        self.profit = ProfitEngine()

        self._emerging = EmergingScanner()
        self._regime = RegimeDetector()
        self._sentiment = SentimentAnalyzer()

        self._recent_logs = []
        self._recent_signals = []

        self.sentiment = {}
        self.active_strategy = "SNIPER"
        self._running = True

        # 🔥 CONTROLLO TRADING
        self.max_concurrent_trades = 2
        self.last_trade_time = {}
        self.cooldown_seconds = 120

        # 🔥 SOLO ASSET REALI
        self.allowed_symbols = [
            "BTC/USDT:USDT",
            "ETH/USDT:USDT",
            "SOL/USDT:USDT",
            "AVAX/USDT:USDT",
            "LINK/USDT:USDT",
            "MATIC/USDT:USDT",
            "XRP/USDT:USDT",
            "DOGE/USDT:USDT"
        ]

        self.strategies = [
            ScalpingStrategy(),
            BreakoutStrategy(),
            RSIMACDStrategy(),
            BollingerStrategy()
        ]

        self.runtime = {
            "signals": [],
            "positions": [],
            "sentiment": {},
            "logs": []
        }


# ==========================================================
# START
# ==========================================================

    def start(self):

        self._setup_logger()

        logger.info("════════════════════════════════════")
        logger.info("BITGET TRADING BOT v10 SNIPER MODE")
        logger.info("════════════════════════════════════")

        if DASHBOARD_ENABLED:
            self._start_dashboard()

        self.exchange.initialize()
        self._recover_positions_from_exchange()
        self.db.connect()

        self.risk.db = self.db
        self.risk.recover_from_db()

        self._notify_startup()
        self._setup_scheduler()

        while self._running:
            try:
                schedule.run_pending()
            except Exception as e:
                logger.error(f"[SCHEDULER] {e}")

            time.sleep(1)


# ==========================================================
# LOGGER
# ==========================================================

    def _setup_logger(self):

        os.makedirs("logs", exist_ok=True)

        logger.remove()

        logger.add("logs/bot.log", rotation="10 MB", level="DEBUG")
        logger.add(lambda msg: print(msg, end=""))

        def _dash_log(msg):
            self.runtime["logs"].append(msg.record["message"])
            self.runtime["logs"] = self.runtime["logs"][-100:]

        logger.add(_dash_log, level="INFO")


# ==========================================================
# DASHBOARD
# ==========================================================

    def _start_dashboard(self):

        import uvicorn

        thread = threading.Thread(
            target=lambda: uvicorn.run(
                "trading_bot.dashboard.server:app",
                host="0.0.0.0",
                port=int(os.environ.get("PORT", 8080)),
                log_level="warning"
            ),
            daemon=True
        )

        thread.start()

        logger.info("[DASHBOARD] running")


    def _update_dashboard(self):

        if not DASHBOARD_ENABLED:
            return

        try:
            self.runtime["positions"] = self.risk.all_open_trades()
            self.runtime["sentiment"] = self.sentiment
            write_state(self)
        except:
            pass


# ==========================================================
# STARTUP TELEGRAM
# ==========================================================

    def _notify_startup(self):

        try:
            self.notifier.startup("live", [], [], 0, 0)
        except:
            pass


# ==========================================================
# RECOVERY
# ==========================================================

    def _recover_positions_from_exchange(self):

        try:
            positions = self.exchange.fetch_positions()

            for p in positions:

                size = safe_float(p.get("contracts"))
                entry = safe_float(p.get("entryPrice"))

                if size <= 0:
                    continue

                symbol = p["symbol"]
                side = "buy" if p.get("side") == "long" else "sell"

                trade = {
                    "symbol": symbol,
                    "side": side,
                    "entry": entry,
                    "size": size,
                    "stop_loss": entry * 0.97,
                    "take_profit": entry * 1.04,
                    "created_at": time.time() - 60,
                    "market": "futures"
                }

                self.risk.register_open(symbol, trade, "futures")

                logger.info(f"[RECOVERY] {symbol} restored")

        except Exception as e:
            logger.error(f"[RECOVERY] {e}")


# ==========================================================
# SCHEDULER
# ==========================================================

    def _setup_scheduler(self):

        schedule.every(30).seconds.do(self._scan_scalping)
        schedule.every(2).minutes.do(self._scan_emerging)
        schedule.every(20).seconds.do(self._monitor_positions)
        schedule.every(2).minutes.do(self._update_sentiment)

        if DASHBOARD_ENABLED:
            schedule.every(10).seconds.do(self._update_dashboard)


# ==========================================================
# VALIDATION
# ==========================================================

    def _is_valid_trade(self, symbol):

        if symbol not in self.allowed_symbols:
            return False

        now = time.time()
        last = self.last_trade_time.get(symbol, 0)

        if now - last < self.cooldown_seconds:
            return False

        if len(self.risk.all_open_trades()) >= self.max_concurrent_trades:
            return False

        return True


# ==========================================================
# SCALPING
# ==========================================================

    def _scan_scalping(self):

        try:

            coins = self._emerging.scan() or []

            for coin in coins[:3]:

                symbol = f"{coin['symbol']}/USDT:USDT"

                if symbol not in self.allowed_symbols:
                    continue

                ohlcv = self.exchange.fetch_ohlcv(symbol, "1m", 100, "futures")
                if not ohlcv:
                    continue

                df = ohlcv_to_df(ohlcv)

                for strat in self.strategies:

                    signal = strat.analyze(df, symbol, "futures")

                    if signal:
                        logger.info(f"[SIGNAL] {symbol}")
                        self._execute_signal(signal)
                        break

        except Exception as e:
            logger.error(f"[SCALP] {e}")


# ==========================================================
# EMERGING
# ==========================================================

    def _scan_emerging(self):

        try:

            coins = self._emerging.scan() or []

            for coin in coins[:3]:

                if coin.get("volume", 0) < 10_000_000:
                    continue

                symbol = f"{coin['symbol']}/USDT:USDT"

                if symbol not in self.allowed_symbols:
                    continue

                ohlcv = self.exchange.fetch_ohlcv(symbol, "5m", 120, "futures")
                if not ohlcv:
                    continue

                df = ohlcv_to_df(ohlcv)

                for strat in self.strategies:

                    signal = strat.analyze(df, symbol, "futures")

                    if signal:
                        logger.info(f"[EMERGING SIGNAL] {symbol}")
                        self._execute_signal(signal)
                        break

        except Exception as e:
            logger.error(f"[EMERGING] {e}")


# ==========================================================
# EXECUTE
# ==========================================================

    def _execute_signal(self, signal):

        try:

            symbol = signal.symbol
            side = signal.side

            if not self._is_valid_trade(symbol):
                return

            ticker = self.exchange.fetch_ticker(symbol, "futures")
            if not ticker:
                return

            price = safe_float(ticker.get("last"))
            if price <= 0:
                return

            balance = safe_float(self.exchange.get_usdt_balance("futures"))

            risk_capital = balance * 0.02
            size = safe_float(risk_capital / price)

            if size * price < 20:
                return

            order = self.exchange.create_market_order(symbol, side, size, "futures")
            if not order:
                return

            trade = {
                "symbol": symbol,
                "side": side,
                "entry": price,
                "size": size,
                "stop_loss": price * 0.97,
                "take_profit": price * 1.04,
                "created_at": time.time(),
                "market": "futures"
            }

            self.risk.register_open(symbol, trade, "futures")
            self.last_trade_time[symbol] = time.time()

            self.notifier.trade_opened(
                symbol=symbol,
                side=side,
                entry=price,
                size=size,
                stop_loss=trade["stop_loss"],
                take_profit=trade["take_profit"],
                market="futures",
                strategy=getattr(signal, "strategy", "sniper"),
                confidence=getattr(signal, "confidence", 0.7)
            )

            logger.info(f"[TRADE OPEN] {symbol}")

        except Exception as e:
            logger.error(f"[TRADE] {e}")


# ==========================================================
# MONITOR
# ==========================================================

    def _monitor_positions(self):

        for trade in self.risk.all_open_trades():

            ticker = self.exchange.fetch_ticker(trade["symbol"], "futures")
            if not ticker:
                continue

            price = safe_float(ticker.get("last"))

            pnl_pct = (price - trade["entry"]) / trade["entry"] * 100

            if pnl_pct > 3:
                self._close_position(trade, price, "take_profit")

            elif pnl_pct < -2:
                self._close_position(trade, price, "stop_loss")


# ==========================================================
# CLOSE
# ==========================================================

    def _close_position(self, trade, price, reason):

        symbol = trade["symbol"]

        side = "sell" if trade["side"] == "buy" else "buy"

        self.exchange.create_market_order(symbol, side, trade["size"], "futures")

        pnl_pct = (price - trade["entry"]) / trade["entry"] * 100
        pnl_usdt = pnl_pct * trade["size"]

        self.notifier.trade_closed(
            symbol=symbol,
            side=trade["side"],
            entry=trade["entry"],
            exit_price=price,
            pnl_pct=pnl_pct,
            pnl_usdt=pnl_usdt,
            reason=reason,
            market="futures"
        )

        logger.info(f"[CLOSE] {symbol} PnL={pnl_pct:.2f}%")


# ==========================================================

    def _update_sentiment(self):
        try:
            self.sentiment = self._sentiment.get_market_sentiment()
        except:
            self.sentiment = {}


# ==========================================================

if __name__ == "__main__":
    TradingBot().start()