"""
Bitget Trading Bot — Main Orchestrator v5
═══════════════════════════════════════════════════════════════
v5 Improvements
 ✓ Candle-close synchronized scans
 ✓ Entry delay anti-fake-breakout
 ✓ Cooldown per symbol
 ✓ Improved scheduler
 ✓ Faster position monitoring
"""

import os
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

from trading_bot.strategies.base import Signal
from trading_bot.strategies.rsi_macd import RSIMACDStrategy
from trading_bot.strategies.bollinger import BollingerStrategy
from trading_bot.strategies.breakout import BreakoutStrategy
from trading_bot.strategies.scalping import ScalpingStrategy

from trading_bot.utils.sentiment_analyzer import SentimentAnalyzer
from trading_bot.utils.emerging_scanner import EmergingScanner
from trading_bot.utils.regime_detector import RegimeDetector


ENTRY_DELAY_SECONDS = 2
TRADE_COOLDOWN = 1800


class TradingBot:

    def __init__(self):

        self.exchange = BitgetExchange()
        self.risk = RiskManager()
        self.notifier = TelegramNotifier()
        self.db = DB()

        self._sentiment = SentimentAnalyzer()
        self._emerging = EmergingScanner()
        self._regime = RegimeDetector()

        self._recent_signals = []
        self._recent_logs = []

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

        logger.info("Starting Trading Bot v5")

        self.exchange.initialize()
        self.db.connect()
        self.risk.recover_from_db()

        self._sync_balance()

        self._check_regime()

        schedule.every().minute.do(self._scan_swing_if_candle_closed)

        schedule.every().minute.do(self._scan_breakout_if_candle_closed)

        if settings.ENABLE_SCALPING:
            schedule.every(45).seconds.do(self._scan_scalping)

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

        if now.minute % 15 == 0 and now.second < 4:
            time.sleep(ENTRY_DELAY_SECONDS)
            self._scan_swing()

    def _scan_breakout_if_candle_closed(self):

        now = datetime.now(timezone.utc)

        if now.minute % 5 == 0 and now.second < 4:
            time.sleep(ENTRY_DELAY_SECONDS)
            self._scan_breakout()

    def _scan_swing(self):

        strats = [s for s in self.strategies if s.NAME in ("RSI_MACD", "BOLLINGER")]

        if not strats:
            return

        for mkt, syms in self._market_symbol_pairs():

            for sym in syms:

                try:

                    df = ohlcv_to_df(
                        self.exchange.fetch_ohlcv(sym, settings.TF_SWING, 300, mkt)
                    )

                    for st in strats:

                        sig = st.analyze(df, sym, mkt)

                        if sig:
                            self._process_signal(sig)

                except Exception as e:
                    logger.error(f"[swing] {sym}: {e}")

    def _scan_breakout(self):

        st = next((s for s in self.strategies if s.NAME == "BREAKOUT"), None)

        if not st:
            return

        for mkt, syms in self._market_symbol_pairs():

            for sym in syms:

                try:

                    df = ohlcv_to_df(
                        self.exchange.fetch_ohlcv(sym, settings.TF_BREAKOUT, 150, mkt)
                    )

                    sig = st.analyze(df, sym, mkt)

                    if sig:
                        self._process_signal(sig)

                except Exception as e:
                    logger.error(f"[breakout] {sym}: {e}")

    def _scan_scalping(self):

        st = next((s for s in self.strategies if s.NAME == "SCALPING"), None)

        if not st:
            return

        for mkt, syms in self._market_symbol_pairs():

            for sym in syms:

                try:

                    df = ohlcv_to_df(
                        self.exchange.fetch_ohlcv(sym, settings.TF_SCALP, 100, mkt)
                    )

                    sig = st.analyze(df, sym, mkt)

                    if sig:
                        self._process_signal(sig)

                except Exception as e:
                    logger.error(f"[scalp] {sym}: {e}")

    def _process_signal(self, signal, risk_multiplier=1.0):

        now = time.time()

        last = self._last_trade.get(signal.symbol, 0)

        if now - last < TRADE_COOLDOWN:
            return

        ok, reason = self.risk.can_trade_symbol(signal.symbol, signal.market)

        if not ok:
            return

        try:

            self._execute_signal(signal, risk_multiplier)

            self._last_trade[signal.symbol] = time.time()

        except Exception as e:
            logger.error(f"[SIG] {signal.symbol}: {e}")

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

        oid = order.get("id", f"unk_{int(time.time())}")

        trade_data = {
            "order_id": oid,
            "side": signal.side,
            "entry": signal.entry,
            "size": size,
            "stop_loss": signal.stop_loss,
            "take_profit": signal.take_profit,
            "strategy": signal.strategy,
            "confidence": signal.confidence,
            "atr": signal.atr,
        }

        self.risk.register_open(signal.symbol, trade_data, signal.market)

        self.db.save_trade_open(
            order_id=oid,
            symbol=signal.symbol,
            market=signal.market,
            strategy=signal.strategy,
            side=signal.side,
            entry=signal.entry,
            size=size,
            stop_loss=signal.stop_loss,
            take_profit=signal.take_profit,
            confidence=signal.confidence,
            atr=signal.atr,
            notes=signal.notes,
            timeframe=signal.timeframe,
            leverage=settings.DEFAULT_LEVERAGE
            if signal.market == "futures"
            else 1,
        )

        logger.info(
            f"TRADE OPEN {signal.symbol} {signal.market} {signal.side} size={size}"
        )

    def _monitor_positions(self):

        for t in self.risk.all_open_trades():

            sym = t["symbol"]
            mkt = t["market"]

            try:

                price = float(self.exchange.fetch_ticker(sym, mkt)["last"])

                ok, reason = self.risk.should_close(t, price)

                if ok:
                    self._close_position(sym, mkt, t, price, reason)

            except Exception as e:
                logger.error(f"[mon] {sym}: {e}")

    def _close_position(self, sym, mkt, trade, price, reason):

        side = "sell" if trade["side"] == "buy" else "buy"

        order = self.exchange.create_market_order(
            symbol=sym,
            side=side,
            amount=trade["size"],
            market=mkt,
            params={"reduceOnly": True} if mkt == "futures" else {},
        )

        if not order:
            return

        logger.info(f"TRADE CLOSED {sym} reason={reason}")

    def _check_regime(self):

        try:
            self._regime.evaluate(self)
        except Exception as e:
            logger.error(f"regime error {e}")

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
