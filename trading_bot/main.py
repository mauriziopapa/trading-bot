"""
Bitget Trading Bot — Main Orchestrator v8.5
Production Ready + Profit Engine + Dashboard + Telegram
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

from trading_bot.utils.emerging_scanner import EmergingScanner
from trading_bot.utils.regime_detector import RegimeDetector
from trading_bot.utils.sentiment_analyzer import SentimentAnalyzer


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
        self.active_strategy = "AUTO"
        self.sentiment = {}
        self._running = True

        self.strategies = [
            RSIMACDStrategy(),
            BollingerStrategy(),
            BreakoutStrategy(),
            ScalpingStrategy()
        ]


# ==========================================================
# START
# ==========================================================

    def start(self):

        self._setup_logger()

        logger.info("════════════════════════════════════")
        logger.info("BITGET TRADING BOT v8.5")
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

        logger.add(
            "logs/bot.log",
            rotation="20 MB",
            retention="10 days",
            level="DEBUG"
        )

        logger.add(lambda msg: print(msg, end=""))

        def _dash_log(msg):

            self._recent_logs.append({
                "ts": msg.record["time"].isoformat(),
                "level": msg.record["level"].name,
                "msg": msg.record["message"]
            })

            self._recent_logs = self._recent_logs[-200:]

        logger.add(_dash_log, level="INFO")


    def _track_signal(self, signal):

        self._recent_signals.append({
            "symbol": signal.symbol,
            "side": signal.side,
            "strategy": signal.strategy,
            "confidence": signal.confidence
        })

        self._recent_signals = self._recent_signals[-50:]

    def _update_sentiment(self):

        try:
            self.sentiment = self._sentiment.get_market_sentiment()
        except Exception:
            self.sentiment = {}

# ==========================================================
# DASHBOARD
# ==========================================================

    def _start_dashboard(self):

        import uvicorn

        port = int(os.environ.get("PORT", 8080))

        thread = threading.Thread(
            target=lambda: uvicorn.run(
                "trading_bot.dashboard.server:app",
                host="0.0.0.0",
                port=port,
                log_level="warning"
            ),
            daemon=True
        )

        thread.start()

        logger.info(f"[DASHBOARD] running on :{port}")


    def _update_dashboard(self):

        if not DASHBOARD_ENABLED:
            return

        try:
            write_state(self)
        except:
            pass


    def _update_strategy_state(self):

        try:

            if settings.ENABLE_SCALPING:
                self.active_strategy = "Blitz"
            elif settings.ENABLE_BREAKOUT:
                self.active_strategy = "Sniper"
            else:
                self.active_strategy = "AUTO"

        except:
            self.active_strategy = "AUTO"

# ==========================================================
# NOTIFY STARTUP
# ==========================================================

    def _notify_startup(self):

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
            logger.warning(f"[TELEGRAM] startup error {e}")


# ==========================================================
# RECOVERY POSITION
# ==========================================================

    def _recover_positions_from_exchange(self):

        try:

            positions = self.exchange.fetch_positions()

            if not positions:
                logger.info("[RECOVERY] no open positions on exchange")
                return

            recovered = 0

            for p in positions:

                symbol = p.get("symbol")
                contracts = safe_float(p.get("contracts"))
                entry = safe_float(p.get("entryPrice"))

                if contracts <= 0 or entry <= 0:
                    continue

                side = "buy" if p.get("side") == "long" else "sell"

                trade = {
                    "symbol": symbol,
                    "side": side,
                    "entry": entry,
                    "size": contracts,
                    "stop_loss": entry * 0.98,
                    "take_profit": entry * 1.02,
                    "created_at": time.time() - 120,  # evita hold block
                    "market": "futures"
                }

                self.risk.register_open(symbol, trade, "futures")

                recovered += 1

                logger.info(f"[RECOVERY] restored {symbol} {side} size={contracts}")

            logger.info(f"[RECOVERY] total recovered: {recovered}")

        except Exception as e:
            logger.error(f"[RECOVERY] error {e}")


# ==========================================================
# SCHEDULER
# ==========================================================

    def _setup_scheduler(self):

        schedule.every(45).seconds.do(self._scan_scalping)
        schedule.every(3).minutes.do(self._scan_emerging)
        schedule.every(30).seconds.do(self._monitor_positions)
        schedule.every(4).minutes.do(self._check_regime)
        schedule.every(2).minutes.do(self._update_sentiment)
        schedule.every(30).seconds.do(self._update_strategy_state)

        if DASHBOARD_ENABLED:
            schedule.every(15).seconds.do(self._update_dashboard)


# ==========================================================
# SCALPING
# ==========================================================

    def _scan_scalping(self):

        try:

            coins = self._emerging.scan() or []

            for coin in coins[:3]:

                if coin.get("volume", 0) < 5_000_000:
                    continue

                symbol = f"{coin['symbol']}/USDT:USDT"

                ohlcv = self.exchange.fetch_ohlcv(symbol, "1m", 120, "futures")

                if not ohlcv or len(ohlcv) < 50:
                    continue

                df = ohlcv_to_df(ohlcv)

                for strat in self.strategies:

                    signal = strat.analyze(df, symbol, "futures")

                    if signal:
                        logger.info(f"[SCALP SIGNAL] {symbol}")

                        self._track_signal(signal)   

                        self._execute_signal(signal)

        except Exception as e:
            logger.error(f"[SCALPING] {e}")


# ==========================================================
# EMERGING
# ==========================================================

    def _scan_emerging(self):

        try:

            coins = self._emerging.scan() or []

            for coin in coins[:5]:

                symbol = f"{coin['symbol']}/USDT:USDT"

                ohlcv = self.exchange.fetch_ohlcv(symbol, "5m", 150, "futures")

                if not ohlcv:
                    continue

                df = ohlcv_to_df(ohlcv)

                for strat in self.strategies:

                    signal = strat.analyze(df, symbol, "futures")

                    if signal:
                        logger.info(f"[EMERGING SIGNAL] {symbol}")

                        self._track_signal(signal)

                        self._execute_signal(signal)

        except Exception as e:
            logger.error(f"[EMERGING] {e}")


# ==========================================================
# EXECUTE TRADE
# ==========================================================

    def _execute_signal(self, signal):

        try:

            symbol = signal.symbol
            side = signal.side

            ticker = self.exchange.fetch_ticker(symbol, "futures")
            if not ticker:
                return

            price = safe_float(ticker.get("last"))
            if price <= 0:
                return

            balance = safe_float(self.exchange.get_usdt_balance("futures"))
            if balance < 5:
                return

            size = safe_float(balance * 0.1 / price)

            order = self.exchange.create_market_order(
                symbol, side, size, "futures"
            )

            if not order:
                return

            trade = {
                "symbol": symbol,
                "side": side,
                "entry": price,
                "size": size,
                "stop_loss": price * 0.985,
                "take_profit": price * 1.02,
                "created_at": time.time(),
                "market": "futures"
            }

            self.db.insert_trade({
                "symbol": symbol,
                "side": side,
                "entry": price,
                "size": size,
                "status": "open",
                "created_at": int(time.time())
            })

            self.risk.register_open(symbol, trade, "futures")

            self.notifier.trade_opened(
                symbol=symbol,
                side=side,
                size=size,
                entry=price,
                stop_loss=trade["stop_loss"],
                take_profit=trade["take_profit"],
                market="futures",
                strategy=signal.strategy,
                confidence=signal.confidence
            )

        except Exception as e:
            logger.error(f"[TRADE] {e}")


# ==========================================================
# MONITOR
# ==========================================================

    def _monitor_positions(self):

        trades = self.risk.all_open_trades() or []

        for trade in trades:

            symbol = trade["symbol"]

            ticker = self.exchange.fetch_ticker(symbol, "futures")
            if not ticker:
                continue

            price = safe_float(ticker.get("last"))
            if price <= 0:
                continue

            # HOLD MINIMO
            if time.time() - trade.get("created_at", 0) < 60:
                continue

            action = self.profit.update_trade(trade, price)

            if action == "partial_close":
                self._close_partial(trade)
                continue

            close, reason = self.risk.should_close(trade, price)

            if close:
                self._close_position(trade, price, reason)


# ==========================================================
# CLOSE
# ==========================================================

    def _close_position(self, trade, price, reason):

        symbol = trade["symbol"]
        side = "sell" if trade["side"] == "buy" else "buy"

        self.exchange.create_market_order(symbol, side, trade["size"], "futures")

        self.risk.register_close(symbol, 0, "futures", reason)

        self.notifier.trade_closed(
            symbol=symbol,
            side=trade["side"],
            entry=trade["entry"],
            exit_price=price,
            pnl_pct=0,
            pnl_usdt=0,
            reason=reason,
            market="futures"
        )
        
        self.db.update_trade_status(symbol, "closed")
        logger.info(f"[CLOSE] {symbol}")


    def _close_partial(self, trade):

        symbol = trade["symbol"]

        size = trade["size"] * 0.5

        self.exchange.create_market_order(symbol, "sell", size, "futures")

        trade["size"] *= 0.5

        logger.info(f"[PARTIAL CLOSE] {symbol}")


# ==========================================================
# REGIME
# ==========================================================

    def _check_regime(self):
        try:
            self._regime.evaluate(self)
        except Exception as e:
            logger.error(f"[REGIME] {e}")


# ==========================================================

if __name__ == "__main__":
    TradingBot().start()