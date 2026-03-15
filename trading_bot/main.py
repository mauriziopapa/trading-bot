"""
Bitget Trading Bot — Main Orchestrator v5
Production Stable + Dashboard + Telegram
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

from trading_bot.models.database import DB

from trading_bot.strategies.rsi_macd import RSIMACDStrategy
from trading_bot.strategies.bollinger import BollingerStrategy
from trading_bot.strategies.breakout import BreakoutStrategy
from trading_bot.strategies.scalping import ScalpingStrategy

from trading_bot.utils.sentiment_analyzer import SentimentAnalyzer
from trading_bot.utils.emerging_scanner import EmergingScanner
from trading_bot.utils.regime_detector import RegimeDetector


# --------------------------------------------------
# DASHBOARD
# --------------------------------------------------

if settings.ENABLE_DASHBOARD:
    try:
        from trading_bot.dashboard.state_writer import write_state
        DASHBOARD_ENABLED = True
    except:
        DASHBOARD_ENABLED = False
else:
    DASHBOARD_ENABLED = False

_bot_ref = None


# --------------------------------------------------
# LOGGER
# --------------------------------------------------

def _setup_logger():

    os.makedirs("logs", exist_ok=True)

    logger.remove()

    logger.add(
        "logs/bot.log",
        rotation="50 MB",
        retention="14 days",
        level=settings.LOG_LEVEL,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",
    )

    logger.add(
        lambda msg: print(msg, end=""),
        level=settings.LOG_LEVEL,
        colorize=True,
    )

    def _dash_log(msg):

        if _bot_ref is None:
            return

        _bot_ref._recent_logs.append({
            "ts": msg.record["time"].isoformat(),
            "level": msg.record["level"].name,
            "msg": msg.record["message"]
        })

        _bot_ref._recent_logs = _bot_ref._recent_logs[-200:]

    logger.add(_dash_log, level="DEBUG")


# --------------------------------------------------
# BOT
# --------------------------------------------------

class TradingBot:

    def __init__(self):

        self.exchange = BitgetExchange()
        self.risk = RiskManager()

        self.notifier = TelegramNotifier()
        self.db = DB()

        self._sentiment = SentimentAnalyzer()
        self._emerging = EmergingScanner()
        self._regime = RegimeDetector()

        self._recent_logs = []
        self._recent_signals = []

        self.strategies = []

        if settings.ENABLE_RSI_MACD:
            self.strategies.append(RSIMACDStrategy())

        if settings.ENABLE_BOLLINGER:
            self.strategies.append(BollingerStrategy())

        if settings.ENABLE_BREAKOUT:
            self.strategies.append(BreakoutStrategy())

        if settings.ENABLE_SCALPING:
            self.strategies.append(ScalpingStrategy())

        self._running = True


# --------------------------------------------------
# START
# --------------------------------------------------

    def start(self):

        global _bot_ref
        _bot_ref = self

        _setup_logger()

        logger.info("════════════════════════════════════")
        logger.info("BITGET TRADING BOT")
        logger.info("════════════════════════════════════")

        if DASHBOARD_ENABLED:
            self._start_dashboard()

        self.exchange.initialize()
        self.db.connect()

        self.risk.recover_from_db()

        self._sync_balance()

        # TELEGRAM START MESSAGE
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

        self._setup_scheduler()

        if DASHBOARD_ENABLED:
            self._update_dashboard()

        logger.info("Bot operativo")

        while self._running:

            try:

                schedule.run_pending()

            except Exception as e:

                logger.error(f"[MAIN LOOP] {e}")

                try:
                    self.notifier.error(f"MAIN LOOP ERROR\n{e}")
                except:
                    pass

            time.sleep(3)


# --------------------------------------------------
# DASHBOARD
# --------------------------------------------------

    def _start_dashboard(self):

        import uvicorn

        port = int(os.environ.get("PORT", settings.DASHBOARD_PORT))

        config = uvicorn.Config(
            "trading_bot.dashboard.server:app",
            host="0.0.0.0",
            port=port,
            log_level="warning",
            access_log=False
        )

        server = uvicorn.Server(config)

        thread = threading.Thread(
            target=server.run,
            daemon=True
        )

        thread.start()

        logger.info(f"[DASHBOARD] running on :{port}")


    def _update_dashboard(self):

        if not DASHBOARD_ENABLED:
            return

        try:

            write_state(self)

        except Exception as e:

            logger.warning(f"[DASHBOARD] update error {e}")


# --------------------------------------------------
# SCHEDULER
# --------------------------------------------------

    def _setup_scheduler(self):

        schedule.every(1).minutes.do(self._scan_swing_if_candle_closed)

        if settings.ENABLE_BREAKOUT:
            schedule.every(5).minutes.do(self._scan_breakout)

        if settings.ENABLE_SCALPING:
            schedule.every(45).seconds.do(self._scan_scalping)

        schedule.every(30).seconds.do(self._monitor_positions)

        schedule.every(3).minutes.do(self._scan_emerging)

        schedule.every(5).minutes.do(self._check_regime)

        schedule.every(10).minutes.do(self._auto_rebalance)

        schedule.every(1).hours.do(self._health_check)

        if DASHBOARD_ENABLED:
            schedule.every(20).seconds.do(self._update_dashboard)


# --------------------------------------------------
# SCAN SWING
# --------------------------------------------------

    def _scan_swing_if_candle_closed(self):

        now = datetime.now(timezone.utc)

        if now.minute % 15 == 0 and now.second < 5:

            time.sleep(3)

            self._scan_swing()


    def _scan_swing(self):

        strategies = [s for s in self.strategies if s.NAME in ("RSI_MACD","BOLLINGER")]

        if not strategies:
            return

        for symbol in settings.SPOT_SYMBOLS:

            try:

                df = ohlcv_to_df(
                    self.exchange.fetch_ohlcv(
                        symbol,
                        settings.TF_SWING,
                        300,
                        "spot"
                    )
                )

                for strat in strategies:

                    signal = strat.analyze(df, symbol, "spot")

                    if signal:
                        logger.info(f"[SIGNAL] {symbol}")

            except Exception as e:

                logger.error(f"[SWING] {symbol} {e}")


# --------------------------------------------------
# BREAKOUT
# --------------------------------------------------

    def _scan_breakout(self):

        logger.debug("[SCAN] breakout")


# --------------------------------------------------
# SCALPING
# --------------------------------------------------

    def _scan_scalping(self):

        logger.debug("[SCAN] scalping")


# --------------------------------------------------
# EMERGING
# --------------------------------------------------

    def _scan_emerging(self):

        try:

            coins = self._emerging.scan()

            if coins:
                logger.info(f"[EMERGING] {len(coins)} coins")

        except Exception as e:

            logger.error(f"[EMERGING] {e}")


# --------------------------------------------------
# POSITIONS
# --------------------------------------------------

    def _monitor_positions(self):

        try:

            trades = self.risk.all_open_trades()

            for t in trades:

                logger.debug(f"[MONITOR] {t['symbol']}")

        except Exception as e:

            logger.error(f"[MONITOR] {e}")


# --------------------------------------------------
# REGIME
# --------------------------------------------------

    def _check_regime(self):

        try:

            self._regime.evaluate(self)

        except Exception as e:

            logger.error(f"[REGIME] {e}")


# --------------------------------------------------
# REBALANCE
# --------------------------------------------------

    def _auto_rebalance(self):

        try:

            self.exchange.auto_rebalance()

        except Exception as e:

            logger.debug(f"[REBALANCE] {e}")


# --------------------------------------------------
# BALANCE
# --------------------------------------------------

    def _sync_balance(self):

        try:

            s = self.exchange.get_usdt_balance("spot")
            f = self.exchange.get_usdt_balance("futures")

            self.risk.session_start_balance = s + f
            self.risk.peak_balance = s + f

        except Exception as e:

            logger.warning(e)


# --------------------------------------------------
# HEALTH
# --------------------------------------------------

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


# --------------------------------------------------

if __name__ == "__main__":

    TradingBot().start()