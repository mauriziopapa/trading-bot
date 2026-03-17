"""
Bitget Trading Bot — Main Orchestrator v8.0
Production Ready + Profit Engine
"""

import os
import time
import schedule
import threading

from datetime import datetime, timezone
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

from trading_bot.utils.sentiment_analyzer import SentimentAnalyzer
from trading_bot.utils.emerging_scanner import EmergingScanner
from trading_bot.utils.regime_detector import RegimeDetector


# ==========================================================
# SAFE FLOAT
# ==========================================================

def safe_float(x, default=0.0):
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


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

        self.strategies = [
            RSIMACDStrategy(),
            BollingerStrategy(),
            BreakoutStrategy(),
            ScalpingStrategy()
        ]

        self._running = True


# ==========================================================
# START
# ==========================================================

    def start(self):

        logger.info("════════════════════════════════════")
        logger.info("BITGET TRADING BOT v8.0")
        logger.info("════════════════════════════════════")

        self.exchange.initialize()
        self.db.connect()

        self.risk.db = self.db
        self.risk.recover_from_db()

        self._setup_scheduler()

        while self._running:
            schedule.run_pending()
            time.sleep(1)


# ==========================================================
# SCHEDULER
# ==========================================================

    def _setup_scheduler(self):

        schedule.every(45).seconds.do(lambda: self._scan_scalping())
        schedule.every(3).minutes.do(lambda: self._scan_emerging())
        schedule.every(30).seconds.do(lambda: self._monitor_positions())
        schedule.every(5).minutes.do(lambda: self._check_regime())


# ==========================================================
# SCAN EMERGING
# ==========================================================

    def _scan_emerging(self):

        coins = self._emerging.scan() or []

        for coin in coins[:10]:

            symbol = f"{coin['symbol']}/USDT:USDT"

            ohlcv = self.exchange.fetch_ohlcv(symbol, "5m", 150, "futures")

            if not ohlcv:
                continue

            df = ohlcv_to_df(ohlcv)

            for strat in self.strategies:

                signal = strat.analyze(df, symbol, "futures")

                if signal:
                    self._execute_signal(signal)


# ==========================================================
# EXECUTE TRADE
# ==========================================================

    def _execute_signal(self, signal):

        try:

            symbol = signal.symbol
            side = signal.side
            market = "futures"

            ticker = self.exchange.fetch_ticker(symbol, market)

            if not ticker:
                return

            price = safe_float(ticker.get("last"))

            if price <= 0:
                return

            balance = safe_float(self.exchange.get_usdt_balance(market))

            if balance < 5:
                return

            size = self.risk.position_size(
                balance,
                price,
                price * 0.98,
                price * 0.01,
                market
            )

            size = safe_float(size)

            if size <= 0:
                return

            order = self.exchange.create_market_order(
                symbol,
                side,
                size,
                market
            )

            if not order:
                return

            trade = {
                "symbol": symbol,
                "side": side,
                "entry": price,
                "size": size,
                "stop_loss": price * 0.98,
                "take_profit": price * 1.03
            }

            self.risk.register_open(symbol, trade, market)

        except Exception as e:
            logger.error(f"[TRADE] {e}")


# ==========================================================
# MONITOR POSITIONS
# ==========================================================

    def _monitor_positions(self):

        trades = self.risk.all_open_trades()

        for trade in trades:

            symbol = trade["symbol"]
            market = trade["market"]

            ticker = self.exchange.fetch_ticker(symbol, market)

            if not ticker:
                continue

            price = safe_float(ticker.get("last"))

            if price <= 0:
                continue

            # PROFIT ENGINE
            action = self.profit.update_trade(trade, price)

            if action == "partial_close":
                self._close_partial(symbol, market, trade)
                continue

            close, reason = self.risk.should_close(trade, price)

            if close:
                self._close_position(symbol, market, trade, price)


# ==========================================================
# CLOSE
# ==========================================================

    def _close_position(self, symbol, market, trade, price):

        side = "sell" if trade["side"] == "buy" else "buy"

        self.exchange.create_market_order(symbol, side, trade["size"], market)

        self.risk.register_close(symbol, 0, market, "exit")

        logger.info(f"[CLOSE] {symbol}")


    def _close_partial(self, symbol, market, trade):

        size = trade["size"] * 0.5

        self.exchange.create_market_order(symbol, "sell", size, market)

        trade["size"] *= 0.5

        logger.info(f"[PARTIAL CLOSE] {symbol}")


# ==========================================================
# REGIME
# ==========================================================

    def _check_regime(self):
        self._regime.evaluate(self)


# ==========================================================

if __name__ == "__main__":
    TradingBot().start()