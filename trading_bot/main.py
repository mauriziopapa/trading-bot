"""
Bitget Trading Bot — Main Orchestrator v7.5
Stable + Dashboard + Emerging Momentum Trading
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


def safe_float(x, default=0.0):
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


# --------------------------------------------------
# DASHBOARD
# --------------------------------------------------

if settings.ENABLE_DASHBOARD:
    try:
        from trading_bot.dashboard.state_writer import write_state
        DASHBOARD_ENABLED = True
    except Exception:
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
        logger.info("BITGET TRADING BOT v7.5")
        logger.info("════════════════════════════════════")

        if DASHBOARD_ENABLED:
            self._start_dashboard()

        self.exchange.initialize()

        self.db.connect()
        self.risk.db = self.db
        self.risk.recover_from_db()

        self._sync_balance()

        try:

            spot = self.exchange.get_usdt_balance("spot") or 0
            futures = self.exchange.get_usdt_balance("futures") or 0

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

        logger.info("Bot operativo")

        while self._running:

            try:
                schedule.run_pending()
            except Exception as e:
                logger.error(f"[SCHEDULER] {e}")

            time.sleep(1)


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
        except Exception:
            pass


# --------------------------------------------------
# SCHEDULER
# --------------------------------------------------

    def _setup_scheduler(self):

        schedule.every(1).minutes.do(self._scan_swing_if_candle_closed)

        if settings.ENABLE_SCALPING:
            schedule.every(45).seconds.do(self._scan_scalping)

        schedule.every(3).minutes.do(self._scan_emerging)

        schedule.every(30).seconds.do(self._monitor_positions)

        schedule.every(5).minutes.do(self._check_regime)

        if DASHBOARD_ENABLED:
            schedule.every(20).seconds.do(self._update_dashboard)


# --------------------------------------------------
# BALANCE
# --------------------------------------------------

    def _sync_balance(self):

        try:

            spot = self.exchange.get_usdt_balance("spot") or 0
            futures = self.exchange.get_usdt_balance("futures") or 0

            total = spot + futures

            self.risk.session_start_balance = total
            self.risk.peak_balance = total

            logger.info(f"[BALANCE] {total:.2f} USDT")

        except Exception as e:
            logger.warning(f"[BALANCE] {e}")


# --------------------------------------------------
# SIGNAL TRACK
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
# SCALPING
# --------------------------------------------------

    def _scan_scalping(self):

        now = datetime.now(timezone.utc)

        if now.second > 10:
            return

        logger.info("[SCALPING] scan")

        strategies = [s for s in self.strategies if getattr(s, "NAME", "") == "SCALPING"]

        if not strategies:
            return

        coins = self._emerging.scan(regime="AGGRO") or []

        symbols = []

        for c in coins[:30]:

            if "symbol" not in c:
                continue

            symbol = f"{c['symbol'].upper()}/USDT:USDT"

            if symbol in settings.SPOT_SYMBOLS:
                symbols.append(symbol)

        # fallback
        if not symbols:
            symbols = settings.SPOT_SYMBOLS[:10]

        if not symbols:
            symbols = settings.SPOT_SYMBOLS[:10]

        for symbol in symbols:

            try:

                ohlcv = self.exchange.fetch_ohlcv(
                    symbol,
                    settings.TF_SCALP,
                    120,
                    "futures"
                )

                if not ohlcv or len(ohlcv) < 50:
                    continue

                df = ohlcv_to_df(ohlcv)

                for strat in strategies:

                    signal = strat.analyze(df, symbol, "futures")

                    logger.debug(
                        f"[STRATEGY] {strat.NAME} {symbol} "
                        f"signal={'YES' if signal else 'NO'}"
                    )

                    if not signal:
                        continue

                    logger.info(f"[SCALPING SIGNAL] {symbol}")

                    self._track_signal(signal)
                    self._process_signal(signal)

            except Exception as e:

                logger.debug(f"[SCALPING] skip {symbol} {e}")

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

        if not strategies:
            return

        for symbol in settings.SPOT_SYMBOLS[:20]:

            try:

                ohlcv = self.exchange.fetch_ohlcv(
                    symbol,
                    settings.TF_SWING,
                    300,
                    "futures"
                )

                if not ohlcv:
                    continue

                df = ohlcv_to_df(ohlcv)

                for strat in strategies:

                    signal = strat.analyze(df, symbol, "spot")

                    if signal:

                        logger.info(f"[SWING SIGNAL] {symbol}")

                        self._track_signal(signal)
                        self._process_signal(signal)

            except Exception as e:

                logger.debug(f"[SWING] {symbol} {e}")


# --------------------------------------------------
# EMERGING
# --------------------------------------------------

    def _scan_emerging(self):

        try:

            coins = self._emerging.scan(regime="AGGRO")

            if not coins:
                return

            coins = sorted(coins, key=lambda x: x.get("score", 0), reverse=True)

            top = coins[:10]

            logger.info(f"[EMERGING] top {[c['symbol'] for c in top]}")

            for coin in top:

                symbol = f"{coin['symbol'].upper()}/USDT:USDT"

#                if symbol not in settings.SPOT_SYMBOLS:
#
#                   logger.info(f"[EMERGING] skip {symbol} not in whitelist")
#
#                continue

                ohlcv = self.exchange.fetch_ohlcv(
                    symbol,
                    settings.TF_SCALP,
                    150,
                    "futures"
                )

                if not ohlcv:
                    continue

                df = ohlcv_to_df(ohlcv)

                for strat in self.strategies:

                    signal = strat.analyze(df, symbol, "spot")

                    logger.debug(
                        f"[CHECK] {symbol} {strat.NAME} "
                        f"{'SIGNAL' if signal else 'NO'}"
                    )

                    if signal:

                        logger.info(f"[EMERGING SIGNAL] {symbol}")

                        self._track_signal(signal)
                        self._process_signal(signal)

        except Exception as e:

            logger.error(f"[EMERGING] {e}")


# --------------------------------------------------
# PROCESS SIGNAL
# --------------------------------------------------

    def _process_signal(self, signal):

        try:

            stats = self.risk.stats() or {}

            open_trades = stats.get("open_trades", 0)

            if signal.market == "spot":

                if open_trades >= settings.MAX_POSITIONS_SPOT:
                    return

            else:

                if open_trades >= settings.MAX_POSITIONS_FUTURES:
                    return

            if not self.risk.reserve_symbol(signal.symbol):
                return

            try:
                self._execute_signal(signal)
            finally:
                self.risk.release_symbol(signal.symbol)

        except Exception as e:

            logger.error(f"[PROCESS SIGNAL] {signal.symbol} {e}")


# --------------------------------------------------
# EXECUTE TRADE
# --------------------------------------------------

def _execute_signal(self, signal):

    try:

        symbol = signal.symbol
        side = signal.side

        # ---------------------------------------
        # MARKET ROUTING
        # ---------------------------------------

        market = "futures"
        if signal.side == "buy" and signal.market == "spot":
            market = "spot"

        # ---------------------------------------
        # FETCH LIVE PRICE (CRITICO FIX)
        # ---------------------------------------

        ticker = self.exchange.fetch_ticker(symbol, market)

        if not ticker:
            logger.error(f"[TRADE] {symbol} no ticker")
            return

        price = safe_float(ticker.get("last") or ticker.get("close"))

        if price <= 0:
            logger.error(f"[TRADE] {symbol} invalid price")
            return

        # ---------------------------------------
        # BALANCE
        # ---------------------------------------

        balance = safe_float(self.exchange.get_usdt_balance(market))

        if balance < 5:
            logger.warning("[TRADE] balance too low")
            return

        balance_safe = balance * 0.92

        # ---------------------------------------
        # SAFE SIGNAL VALUES
        # ---------------------------------------

        entry = safe_float(signal.entry, price)
        stop_loss = safe_float(signal.stop_loss, price * 0.98)
        atr = safe_float(signal.atr, price * 0.01)

        # ---------------------------------------
        # POSITION SIZE (SAFE)
        # ---------------------------------------

        try:
            size = self.risk.position_size(
                balance_safe,
                entry,
                stop_loss,
                atr,
                market,
                symbol=symbol
            )
        except Exception:
            size = balance_safe / price * 0.1  # fallback

        size = safe_float(size)

        if size <= 0:
            logger.debug(f"[TRADE] size zero {symbol}")
            return

        trade_value = size * price

        if trade_value > balance_safe:
            size = balance_safe / price

        if trade_value < 5:
            logger.warning(f"[TRADE] too small {symbol}")
            return

        # ---------------------------------------
        # EXECUTE ORDER
        # ---------------------------------------

        params = {}

        if market == "futures":
            params["reduceOnly"] = False

        order = self.exchange.create_market_order(
            symbol,
            side,
            size,
            market,
            params=params
        )

        if not order or not order.get("id"):
            logger.error(f"[TRADE] order failed {symbol}")
            return

        filled = safe_float(order.get("filled"), size)

        # ---------------------------------------
        # REGISTER TRADE
        # ---------------------------------------

        trade_data = {
            "order_id": order.get("id"),
            "side": side,
            "entry": price,
            "size": filled,
            "stop_loss": stop_loss,
            "take_profit": safe_float(signal.take_profit, price * 1.02),
            "atr": atr
        }

        self.risk.register_open(symbol, trade_data, market)

        try:
            self.db.insert_trade({
                "symbol": symbol,
                "market": market,
                "side": side,
                "entry": price,
                "size": filled,
                "stop_loss": stop_loss,
                "take_profit": safe_float(signal.take_profit),
                "order_id": order.get("id"),
                "status": "open",
                "created_at": int(time.time())
            })
        except Exception as e:
            logger.error(f"[DB] save trade failed {e}")

        self.notifier.trade_opened(
            symbol=symbol,
            side=side,
            size=filled,
            entry=price,
            stop_loss=stop_loss,
            take_profit=safe_float(signal.take_profit),
            market=market,
            strategy=signal.strategy,
            confidence=signal.confidence
        )

        logger.info(
            f"[TRADE] OPEN {symbol} {side} value={trade_value:.2f}"
        )

    except Exception as e:
        logger.error(f"[TRADE] {signal.symbol} {e}")


# --------------------------------------------------
# MONITOR POSITIONS
# --------------------------------------------------

    def _monitor_positions(self):

        trades = self.risk.all_open_trades() or []

        for trade in trades:

            try:

                symbol = trade["symbol"]
                market = trade["market"]

                ticker = self.exchange.fetch_ticker(symbol, market)

                if not ticker:
                    continue

                price = safe_float(ticker.get("last"))
                if price <= 0:
                    continue

                if hasattr(self.risk, "should_close"):
                    close, reason = self.risk.should_close(trade, price)
                else:
                    close, reason = False, None

                if close:
                    self._close_position(symbol, market, trade, price, reason)

            except Exception as e:
                logger.error(f"[MONITOR] {e}")
# --------------------------------------------------
# CLOSE POSITION
# --------------------------------------------------

    def _close_position(self, symbol, market, trade, exit_price, reason):

        try:

            side = "sell" if trade["side"] == "buy" else "buy"

            base = symbol.split("/")[0]

            balance = self.exchange.fetch_balance(market)
            
            asset_balance = safe_float(balance.get(base, {}).get("free", 0))
            
            if asset_balance <= 0:
                logger.error(f"[CLOSE] no balance for {symbol}")
                return
            
            size = asset_balance * 0.999
            
            order = self.exchange.create_market_order(symbol, side, size, market)

            if not order:
                logger.error(f"[CLOSE] order failed {symbol}")
                return

            entry = trade["entry"]

            entry = safe_float(entry)
            exit_price = safe_float(exit_price)

            if entry <= 0:
                logger.error(f"[CLOSE] invalid entry {symbol}")
                return

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
# REGIME
# --------------------------------------------------

    def _check_regime(self):

        try:
            self._regime.evaluate(self)
        except Exception as e:
            logger.error(f"[REGIME] {e}")


# --------------------------------------------------

if __name__ == "__main__":
    TradingBot().start()
