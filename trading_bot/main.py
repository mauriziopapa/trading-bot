"""
Bitget Trading Bot — Main Orchestrator v6.1
Trading Enabled + Dashboard + Telegram
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
        logger.info("BITGET TRADING BOT v6.1")
        logger.info("════════════════════════════════════")

        if DASHBOARD_ENABLED:
            self._start_dashboard()

        try:
            self.exchange.initialize()
            self.db.connect()
            self.risk.recover_from_db()
        except Exception as e:
            logger.error(f"startup error {e}")

        self._sync_balance()

        # Telegram startup
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
                    self.notifier.error(str(e))
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

        threading.Thread(
            target=server.run,
            daemon=True
        ).start()

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

        schedule.every(30).seconds.do(self._monitor_positions)

        schedule.every(3).minutes.do(self._scan_emerging)

        schedule.every(5).minutes.do(self._check_regime)

        schedule.every(10).minutes.do(self._auto_rebalance)

        schedule.every(1).hours.do(self._health_check)

        if DASHBOARD_ENABLED:
            schedule.every(20).seconds.do(self._update_dashboard)


# --------------------------------------------------
# SWING SCAN
# --------------------------------------------------

    def _scan_swing_if_candle_closed(self):

        now = datetime.now(timezone.utc)

        if now.minute % 15 == 0 and now.second < 5:

            time.sleep(3)

            self._scan_swing()


    def _scan_swing(self):

        strategies = [s for s in self.strategies if s.NAME in ("RSI_MACD", "BOLLINGER")]

        if not strategies:
            return

        symbols = settings.FUTURES_SYMBOLS if settings.FUTURES_SYMBOLS else settings.SPOT_SYMBOLS

        market = "futures" if settings.FUTURES_SYMBOLS else "spot"

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

                for strat in strategies:

                    signal = strat.analyze(df, symbol, market)

                    if signal:

                        logger.info(f"[SIGNAL] {symbol}")

                        self._process_signal(signal)

            except Exception as e:

                logger.error(f"[SWING] {symbol} {e}")


# --------------------------------------------------
# PROCESS SIGNAL
# --------------------------------------------------

    def _process_signal(self, signal):

        if not self.risk.reserve_symbol(signal.symbol):
            return

        try:
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
                balance=balance,
                entry=signal.entry,
                stop_loss=signal.stop_loss,
                atr=signal.atr,
                market=signal.market,
                symbol=signal.symbol
            )

            if size <= 0:
                return

            params = {}

            if signal.market == "futures":
                params = {
                    "reduceOnly": False,
                    "marginMode": settings.MARGIN_MODE
                }

            order = self.exchange.create_market_order(
                symbol=signal.symbol,
                side=signal.side,
                amount=size,
                market=signal.market,
                params=params
            )

            if not order:
                return

            order_id = order.get("id", f"ord_{int(time.time())}")

            trade_data = {
                "order_id": order_id,
                "side": signal.side,
                "entry": signal.entry,
                "size": size,
                "stop_loss": signal.stop_loss,
                "take_profit": signal.take_profit,
                "atr": signal.atr
            }

            self.risk.register_open(signal.symbol, trade_data, signal.market)

            self.notifier.trade_opened(
                symbol=signal.symbol,
                side=signal.side,
                size=size,
                entry=signal.entry,
                stop_loss=signal.stop_loss,
                take_profit=signal.take_profit,
                market=signal.market,
                strategy=signal.strategy,
                confidence=signal.confidence
            )

        except Exception as e:

            logger.error(f"[TRADE] {signal.symbol} {e}")


# --------------------------------------------------
# MONITOR POSITIONS
# --------------------------------------------------

    def _monitor_positions(self):

        try:

            trades = self.risk.all_open_trades()

            for trade in trades:

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

            logger.error(f"[MONITOR] {e}")


# --------------------------------------------------
# CLOSE POSITION
# --------------------------------------------------

    def _close_position(self, symbol, market, trade, exit_price, reason):

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

            entry = trade["entry"]

            pnl_pct = ((exit_price - entry) / entry) * 100

            if trade["side"] == "sell":
                pnl_pct *= -1

            pnl_usdt = trade["size"] * entry * (pnl_pct / 100)

            self.risk.register_close(symbol, pnl_pct, market, reason)

            self.notifier.trade_closed(
                symbol=symbol,
                side=trade["side"],
                entry=entry,
                exit_price=exit_price,
                pnl_pct=pnl_pct,
                pnl_usdt=pnl_usdt,
                reason=reason,
                market=market
            )

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


    def _scan_emerging(self):

        try:

            coins = self._emerging.scan()

            if coins:
                logger.info(f"[EMERGING] {len(coins)} coins")

        except Exception as e:

            logger.error(f"[EMERGING] {e}")


    def _auto_rebalance(self):

        try:
            self.exchange.auto_rebalance()
        except:
            pass


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


# --------------------------------------------------

if __name__ == "__main__":

    TradingBot().start()
