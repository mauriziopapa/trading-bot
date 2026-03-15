"""
Bitget Trading Bot — Main Orchestrator v6.1
═══════════════════════════════════════════
Senior Review Fixes
✓ safer dataframe handling
✓ symbol cooldown isolation
✓ robust market filters
✓ safer scheduler
✓ improved logging
"""

import time
import schedule
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


ENTRY_DELAY_SECONDS = 2
TRADE_COOLDOWN = 1800

MIN_ATR_RATIO = 0.004
VOLUME_SPIKE_RATIO = 1.5


class TradingBot:

    def __init__(self):

        self.exchange = BitgetExchange()
        self.risk = RiskManager()
        self.notifier = TelegramNotifier()
        self.db = DB()

        self._sentiment = SentimentAnalyzer()
        self._emerging = EmergingScanner()
        self._regime = RegimeDetector()

        self._last_trade = {}

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

    def start(self):

        logger.info("Starting Trading Bot v6.1")

        self.exchange.initialize()
        self.db.connect()
        self.risk.recover_from_db()

        self._sync_balance()
        self._check_regime()

        schedule.every(1).minutes.do(self._scan_swing_if_candle_closed)
        schedule.every(1).minutes.do(self._scan_breakout_if_candle_closed)

        if settings.ENABLE_SCALPING:
            schedule.every(30).seconds.do(self._scan_scalping)

        schedule.every(10).minutes.do(self._scan_emerging)
        schedule.every(10).seconds.do(self._monitor_positions)

        schedule.every(1).hours.do(self._health_check)
        schedule.every(10).minutes.do(self._auto_rebalance)

        logger.info("Bot operativo")

        while self._running:
            schedule.run_pending()
            time.sleep(2)

    def _scan_swing_if_candle_closed(self):

        now = datetime.now(timezone.utc)

        if now.minute % 15 != 0 or now.second > 4:
            return

        time.sleep(ENTRY_DELAY_SECONDS)
        self._scan_swing()

    def _scan_breakout_if_candle_closed(self):

        now = datetime.now(timezone.utc)

        if now.minute % 5 != 0 or now.second > 4:
            return

        time.sleep(ENTRY_DELAY_SECONDS)
        self._scan_breakout()

    def _scan_swing(self):

        strats = [s for s in self.strategies if s.NAME in ("RSI_MACD", "BOLLINGER")]

        if not strats:
            return

        for market, symbols in self._market_symbol_pairs():

            for symbol in symbols:

                try:

                    df = self._safe_fetch(symbol, market, settings.TF_SWING, 300)

                    if df is None:
                        continue

                    if not self._market_conditions_ok(df):
                        continue

                    for strat in strats:

                        signal = strat.analyze(df, symbol, market)

                        if signal:
                            self._process_signal(signal)

                except Exception as e:
                    logger.error(f"[swing] {symbol}: {e}")

    def _scan_breakout(self):

        strat = next((s for s in self.strategies if s.NAME == "BREAKOUT"), None)

        if not strat:
            return

        for market, symbols in self._market_symbol_pairs():

            for symbol in symbols:

                try:

                    df = self._safe_fetch(symbol, market, settings.TF_BREAKOUT, 150)

                    if df is None:
                        continue

                    if not self._market_conditions_ok(df):
                        continue

                    signal = strat.analyze(df, symbol, market)

                    if signal:
                        self._process_signal(signal)

                except Exception as e:
                    logger.error(f"[breakout] {symbol}: {e}")

    def _scan_scalping(self):

        strat = next((s for s in self.strategies if s.NAME == "SCALPING"), None)

        if not strat:
            return

        for market, symbols in self._market_symbol_pairs():

            for symbol in symbols:

                try:

                    df = self._safe_fetch(symbol, market, settings.TF_SCALP, 100)

                    if df is None:
                        continue

                    signal = strat.analyze(df, symbol, market)

                    if signal:
                        self._process_signal(signal)

                except Exception as e:
                    logger.error(f"[scalp] {symbol}: {e}")

    def _safe_fetch(self, symbol, market, tf, limit):

        try:

            raw = self.exchange.fetch_ohlcv(symbol, tf, limit, market)

            if not raw:
                return None

            df = ohlcv_to_df(raw)

            if df is None or len(df) < 30:
                return None

            return df

        except Exception:
            return None

    def _market_conditions_ok(self, df):

        try:

            close = df["close"]
            volume = df["volume"]

            price = float(close.iloc[-1])

            if price <= 0:
                return False

            atr = close.diff().abs().rolling(14).mean().iloc[-1]

            if atr / price < MIN_ATR_RATIO:
                return False

            avg_vol = volume.rolling(20).mean().iloc[-1]

            if volume.iloc[-1] < avg_vol * VOLUME_SPIKE_RATIO:
                return False

            return True

        except Exception:
            return True

    def _process_signal(self, signal, risk_multiplier=1.0):

        key = f"{signal.market}:{signal.symbol}"

        last = self._last_trade.get(key, 0)

        if time.time() - last < TRADE_COOLDOWN:
            return

        ok, _ = self.risk.can_trade_symbol(signal.symbol, signal.market)

        if not ok:
            return

        try:

            self._execute_signal(signal, risk_multiplier)

            self._last_trade[key] = time.time()

        except Exception as e:
            logger.error(f"[signal] {signal.symbol}: {e}")

    def _execute_signal(self, signal, risk_multiplier=1.0):

        time.sleep(ENTRY_DELAY_SECONDS)

        if signal.confidence < settings.MIN_CONFIDENCE:
            return

        balance = self.exchange.get_usdt_balance(signal.market)

        size = self.risk.position_size(
            balance=balance,
            entry=signal.entry,
            stop_loss=signal.stop_loss,
            atr=signal.atr,
            market=signal.market,
            risk_multiplier=risk_multiplier,
            symbol=signal.symbol,
        )

        if size <= 0:
            return

        params = {}

        if signal.market == "futures":
            params = {"reduceOnly": False, "marginMode": settings.MARGIN_MODE}

        order = self.exchange.create_market_order(
            symbol=signal.symbol,
            side=signal.side,
            amount=size,
            market=signal.market,
            params=params,
        )

        if not order:
            return

        self.risk.register_open(
            signal.symbol,
            {
                "order_id": order.get("id"),
                "entry": signal.entry,
                "size": size,
                "side": signal.side,
                "stop_loss": signal.stop_loss,
                "take_profit": signal.take_profit,
                "atr": signal.atr,
            },
            signal.market,
        )

        logger.info(
            f"TRADE OPEN {signal.symbol} {signal.market} {signal.side} size={size}"
        )

    def _monitor_positions(self):

        for trade in self.risk.all_open_trades():

            symbol = trade["symbol"]
            market = trade["market"]

            try:

                price = float(self.exchange.fetch_ticker(symbol, market)["last"])

                close, reason = self.risk.should_close(trade, price)

                if close:
                    self._close_position(symbol, market, trade, price, reason)

            except Exception as e:
                logger.error(f"[monitor] {symbol}: {e}")

    def _close_position(self, symbol, market, trade, price, reason):

        side = "sell" if trade["side"] == "buy" else "buy"

        order = self.exchange.create_market_order(
            symbol=symbol,
            side=side,
            amount=trade["size"],
            market=market,
            params={"reduceOnly": True} if market == "futures" else {},
        )

        if not order:
            return

        logger.info(f"TRADE CLOSED {symbol} reason={reason}")

    def _scan_emerging(self):
        try:
            coins = self._emerging.scan()
            if not coins:
                return
        except Exception as e:
            logger.debug(f"[emerging] {e}")

    def _check_regime(self):

        try:
            self._regime.evaluate(self)
        except Exception as e:
            logger.error(f"[regime] {e}")

    def _health_check(self):

        logger.info("health check")

    def _sync_balance(self):

        try:

            s = self.exchange.get_usdt_balance("spot")
            f = self.exchange.get_usdt_balance("futures")

            self.risk.session_start_balance = s + f
            self.risk.peak_balance = s + f

        except Exception as e:
            logger.warning(e)

    def _auto_rebalance(self):

        try:
            self.exchange.auto_rebalance(keep_spot_usdt=5)
        except Exception:
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
