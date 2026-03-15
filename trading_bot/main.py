"""
Bitget Trading Bot — Main Orchestrator v6.6
Stable + Dashboard + Emerging Trading
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

    logger.add(lambda msg: print(msg, end=""), level=settings.LOG_LEVEL)

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
        logger.info("BITGET TRADING BOT v6.6")
        logger.info("════════════════════════════════════")

        if DASHBOARD_ENABLED:
            self._start_dashboard()

        self.exchange.initialize()
        self.db.connect()

        self.risk.recover_from_db()

        self._sync_balance()

        self._setup_scheduler()

        logger.info("Bot operativo")

        while self._running:

            try:
                schedule.run_pending()
            except Exception as e:
                logger.error(f"[MAIN LOOP] {e}")

            time.sleep(2)


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


# --------------------------------------------------
# SCHEDULER
# --------------------------------------------------

    def _setup_scheduler(self):

        schedule.every(1).minutes.do(self._scan_swing_if_candle_closed)

        schedule.every(45).seconds.do(self._scan_scalping)

        schedule.every(3).minutes.do(self._scan_emerging)

        schedule.every(30).seconds.do(self._monitor_positions)

        schedule.every(5).minutes.do(self._check_regime)

        if DASHBOARD_ENABLED:
            schedule.every(20).seconds.do(self._update_dashboard)


# --------------------------------------------------
# SWING SCAN
# --------------------------------------------------

    def _scan_swing_if_candle_closed(self):

        now = datetime.now(timezone.utc)

        if now.minute % 15 == 0 and now.second < 5:

            time.sleep(2)

            self._scan_swing()


    def _scan_swing(self):

        strategies = [s for s in self.strategies if s.NAME in ("RSI_MACD", "BOLLINGER")]

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

                        self._track_signal(signal)

                        self._process_signal(signal)

            except Exception as e:

                logger.error(f"[SWING] {symbol} {e}")


# --------------------------------------------------
# EMERGING SCAN (COLLEGATO AL TRADING)
# --------------------------------------------------

    def _scan_emerging(self):

        try:

            coins = self._emerging.scan()

            if not coins:
                return

            self._emerging.last_scan = coins

            for coin in coins[:10]:

                symbol = coin["symbol"] + "/USDT"

                if symbol not in settings.SPOT_SYMBOLS:
                    continue

                df = ohlcv_to_df(
                    self.exchange.fetch_ohlcv(
                        symbol,
                        settings.TF_SCALP,
                        150,
                        "spot"
                    )
                )

                for strat in self.strategies:

                    signal = strat.analyze(df, symbol, "spot")

                    if signal:

                        logger.info(f"[EMERGING SIGNAL] {symbol}")

                        self._track_signal(signal)

                        self._process_signal(signal)

        except Exception as e:

            logger.error(f"[EMERGING] {e}")


# --------------------------------------------------
# SIGNAL TRACKING
# --------------------------------------------------

    def _track_signal(self, signal):

        self._recent_signals.append({
            "symbol": signal.symbol,
            "side": signal.side,
            "strategy": signal.strategy,
            "confidence": signal.confidence
        })

        self._recent_signals = self._recent_signals[-50:]


# --------------------------------------------------
# PROCESS SIGNAL
# --------------------------------------------------

    def _process_signal(self, signal):

        try:

            if not self.risk.reserve_symbol(signal.symbol):
                return

            self._execute_signal(signal)

        finally:

            self.risk.release_symbol(signal.symbol)


# --------------------------------------------------
# EXECUTE TRADE
# --------------------------------------------------

    def _execute_signal(self, signal):

        try:

            balance = self.exchange.get_usdt_balance(signal.market)

            if balance < 10:
                return

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

            order = self.exchange.create_market_order(
                symbol=signal.symbol,
                side=signal.side,
                amount=size,
                market=signal.market
            )

            if not order:
                return

            trade_data = {
                "order_id": order.get("id"),
                "side": signal.side,
                "entry": signal.entry,
                "size": size,
                "stop_loss": signal.stop_loss,
                "take_profit": signal.take_profit,
                "atr": signal.atr
            }

            self.risk.register_open(
                signal.symbol,
                trade_data,
                signal.market
            )

        except Exception as e:

            logger.error(f"[TRADE] {signal.symbol} {e}")


# --------------------------------------------------
# POSITION MONITOR
# --------------------------------------------------

    def _monitor_positions(self):

        trades = self.risk.all_open_trades()

        for trade in trades:

            try:

                symbol = trade["symbol"]
                market = trade["market"]

                ticker = self.exchange.fetch_ticker(symbol, market)

                price = float(ticker["last"])

                close, reason = self.risk.should_close(trade, price)

                if close:

                    self._close_position(
                        symbol,
                        market,
                        trade,
                        price,
                        reason
                    )

            except Exception as e:

                logger.error(f"[MONITOR] {e}")


# --------------------------------------------------
# CLOSE POSITION
# --------------------------------------------------

    def _close_position(self, symbol, market, trade, exit_price, reason):

        try:

            side = "sell" if trade["side"] == "buy" else "buy"

            self.exchange.create_market_order(
                symbol,
                side,
                trade["size"],
                market
            )

            entry = trade["entry"]

            pnl_pct = ((exit_price - entry) / entry) * 100

            if trade["side"] == "sell":
                pnl_pct *= -1

            self.risk.register_close(symbol, pnl_pct, market, reason)

            logger.info(f"CLOSE {symbol} {pnl_pct:+.2f}%")

        except Exception as e:

            logger.error(f"[CLOSE] {symbol} {e}")


# --------------------------------------------------
# SUPPORT
# --------------------------------------------------

    def _check_regime(self):

        try:
            self._regime.evaluate(self)
        except Exception as e:
            logger.error(f"[REGIME] {e}")


    def _update_dashboard(self):

        try:
            write_state(self)
        except Exception:
            pass


    def _sync_balance(self):

        try:

            s = self.exchange.get_usdt_balance("spot")
            f = self.exchange.get_usdt_balance("futures")

            self.risk.session_start_balance = s + f
            self.risk.peak_balance = s + f

        except Exception as e:

            logger.warning(e)


# --------------------------------------------------

if __name__ == "__main__":

    TradingBot().start()