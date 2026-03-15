"""
Bitget Trading Bot — Main Orchestrator v7.1
Stable production engine
"""

import os
import time
import schedule
import threading

from loguru import logger

from trading_bot.config import settings
from trading_bot.utils.exchange import BitgetExchange
from trading_bot.utils.risk_manager import RiskManager
from trading_bot.utils.notifier import TelegramNotifier
from trading_bot.models.database import DB

from trading_bot.strategies.rsi_macd import RSIMACDStrategy
from trading_bot.strategies.bollinger import BollingerStrategy
from trading_bot.strategies.breakout import BreakoutStrategy
from trading_bot.strategies.scalping import ScalpingStrategy

from trading_bot.utils.regime_detector import RegimeDetector
from trading_bot.utils.emerging_scanner import EmergingScanner


# ==========================================================
# LOGGER
# ==========================================================

def setup_logger():

    os.makedirs("logs", exist_ok=True)

    logger.remove()

    logger.add(
        "logs/bot.log",
        rotation="50 MB",
        retention="14 days",
        level="INFO",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    )

    logger.add(lambda msg: print(msg, end=""))


# ==========================================================
# BOT
# ==========================================================

class TradingBot:

    def __init__(self):

        self.exchange = BitgetExchange()
        self.risk = RiskManager()
        self.notifier = TelegramNotifier()
        self.db = DB()

        self.regime = RegimeDetector()
        self.emerging = EmergingScanner()

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

        setup_logger()

        logger.info("════════════════════════════════════")
        logger.info("BITGET TRADING BOT v7.1")
        logger.info("════════════════════════════════════")

        try:

            self.exchange.initialize()
            self.db.connect()

        except Exception as e:

            logger.error(f"startup error {e}")

        self._start_dashboard()

        self._sync_balance()

        self._telegram_startup()

        self._setup_scheduler()

        logger.info("Bot operativo")

        while self._running:

            try:

                schedule.run_pending()

                self._risk_guard()

            except Exception as e:

                logger.error(f"[MAIN LOOP] {e}")

            time.sleep(2)


# ==========================================================
# DASHBOARD
# ==========================================================

    def _start_dashboard(self):

        if not settings.ENABLE_DASHBOARD:
            return

        try:

            import uvicorn

            port = int(os.environ.get("PORT", 8080))

            config = uvicorn.Config(
                "trading_bot.dashboard.server:app",
                host="0.0.0.0",
                port=port,
                log_level="warning",
                access_log=False
            )

            server = uvicorn.Server(config)

            threading.Thread(
                target=server.run,
                daemon=True
            ).start()

            logger.info(f"[DASHBOARD] running on :{port}")

        except Exception as e:

            logger.warning(f"dashboard error {e}")


# ==========================================================
# TELEGRAM
# ==========================================================

    def _telegram_startup(self):

        try:

            spot = self.exchange.get_usdt_balance("spot")
            futures = self.exchange.get_usdt_balance("futures")

            self.notifier.startup(
                settings.TRADING_MODE,
                settings.SPOT_SYMBOLS,
                settings.FUTURES_SYMBOLS,
                spot,
                futures
            )

        except Exception as e:

            logger.warning(f"telegram startup {e}")


# ==========================================================
# RISK GUARD
# ==========================================================

    def _risk_guard(self):

        try:

            spot = self.exchange.get_usdt_balance("spot")
            futures = self.exchange.get_usdt_balance("futures")

            balance = spot + futures

            if self.risk.drawdown_exceeded(balance):

                logger.error("MAX DRAWDOWN HIT")

                try:
                    self.notifier.error("BOT STOPPED MAX DD")
                except:
                    pass

                self._running = False

        except Exception as e:

            logger.warning(f"[RISK GUARD] {e}")


# ==========================================================
# SCHEDULER
# ==========================================================

    def _setup_scheduler(self):

        schedule.every(15).minutes.do(self._scan_market)

        schedule.every(10).seconds.do(self._monitor_positions)

        schedule.every(3).minutes.do(self._scan_emerging)

        schedule.every(5).minutes.do(self._check_regime)

        schedule.every(1).hours.do(self._health_check)


# ==========================================================
# MARKET SCAN
# ==========================================================

    def _scan_market(self):

        regime = self.regime.current_regime

        allowed = self._strategies_for_regime(regime)

        for strat in self.strategies:

            if strat.NAME not in allowed:
                continue

            try:

                signals = strat.scan(self.exchange)

                for signal in signals:

                    self._process_signal(signal)

            except Exception as e:

                logger.warning(f"[SCAN] {strat.NAME} {e}")


# ==========================================================
# REGIME STRATEGY
# ==========================================================

    def _strategies_for_regime(self, regime):

        if regime == "trend":
            return ["BREAKOUT"]

        if regime == "range":
            return ["BOLLINGER"]

        if regime == "volatile":
            return ["SCALPING"]

        return ["RSI_MACD"]


# ==========================================================
# SIGNAL PROCESS
# ==========================================================

    def _process_signal(self, signal):

        if not self.risk.reserve_symbol(signal.symbol):
            return

        try:

            balance = self.exchange.get_usdt_balance(signal.market)

            size = self.risk.position_size(
                balance,
                signal.entry,
                signal.stop_loss,
                signal.atr,
                signal.market,
                symbol=signal.symbol
            )

            if size <= 0:
                return

            lev = self.risk.dynamic_leverage(signal.atr, signal.entry)

            try:
                self.exchange.set_leverage(signal.symbol, lev)
            except:
                pass

            order = self.exchange.create_market_order(
                signal.symbol,
                signal.side,
                size,
                signal.market
            )

            if not order:
                return

            trade = {
                "entry": signal.entry,
                "size": size,
                "side": signal.side,
                "stop_loss": signal.stop_loss,
                "take_profit": signal.take_profit,
                "atr": signal.atr
            }

            self.risk.register_open(signal.symbol, trade, signal.market)

        finally:

            self.risk.release_symbol(signal.symbol)


# ==========================================================
# POSITION MONITOR
# ==========================================================

    def _monitor_positions(self):

        trades = self.risk.all_open_trades()

        for trade in trades:

            try:

                ticker = self.exchange.fetch_ticker(trade["symbol"], trade["market"])

                price = float(ticker["last"])

                close, reason = self.risk.should_close(trade, price)

                if close:

                    self._close_position(
                        trade["symbol"],
                        trade["market"],
                        trade,
                        price,
                        reason
                    )

            except Exception as e:

                logger.warning(f"[MONITOR] {trade['symbol']} {e}")


# ==========================================================
# CLOSE
# ==========================================================

    def _close_position(self, symbol, market, trade, price, reason):

        try:

            side = "sell" if trade["side"] == "buy" else "buy"

            self.exchange.create_market_order(
                symbol,
                side,
                trade["size"],
                market,
                {"reduceOnly": True}
            )

            entry = trade["entry"]

            direction = 1 if trade["side"] == "buy" else -1

            pnl_usdt = (price - entry) * trade["size"] * direction

            pnl_pct = ((price - entry) / entry) * 100 * direction

            self.risk.register_close(symbol, pnl_pct, market, reason)

            logger.info(f"CLOSE {symbol} {pnl_pct:.2f}%")

        except Exception as e:

            logger.error(f"[CLOSE] {symbol} {e}")


# ==========================================================
# SUPPORT
# ==========================================================

    def _check_regime(self):

        try:
            self.regime.evaluate(self)
        except Exception as e:
            logger.error(f"[REGIME] {e}")


    def _scan_emerging(self):

        try:

            coins = self.emerging.scan()

            if coins:
                logger.info(f"[EMERGING] {len(coins)} coins")

        except Exception as e:

            logger.error(f"[EMERGING] {e}")


    def _sync_balance(self):

        try:

            s = self.exchange.get_usdt_balance("spot")
            f = self.exchange.get_usdt_balance("futures")

            self.risk.session_start_balance = s + f
            self.risk.peak_balance = s + f

        except Exception as e:

            logger.warning(e)


    def _health_check(self):

        try:

            stats = self.risk.stats()

            logger.info(
                f"[HEALTH] open={stats['open_trades']} "
                f"wins={stats['wins']} "
                f"losses={stats['losses']}"
            )

        except Exception as e:

            logger.warning(e)


# ==========================================================

if __name__ == "__main__":

    TradingBot().start()