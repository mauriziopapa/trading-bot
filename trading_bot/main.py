"""
Bitget Trading Bot — Main Orchestrator v8.1
═══════════════════════════════════════════

Fix:
✓ remove get_position dependency
✓ stable close logic
✓ ticker safety
✓ exchange sync protection
✓ dashboard log compatibility
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


ENTRY_DELAY_SECONDS = 2
TRADE_COOLDOWN = 1800


class TradingBot:

    def __init__(self):

        self.exchange = BitgetExchange()
        self.risk = RiskManager()
        self.notifier = TelegramNotifier()
        self.db = DB()

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


# ==========================================================
# START
# ==========================================================

    def start(self):

        logger.info("Starting Trading Bot v8.1")

        self.exchange.initialize()
        self.db.connect()

        self.risk.recover_from_db()

        self._sync_balance()

        schedule.every().minute.do(self._scan_swing_if_candle_closed)
        schedule.every().minute.do(self._scan_breakout_if_candle_closed)

        if settings.ENABLE_SCALPING:
            schedule.every(45).seconds.do(self._scan_scalping)

        schedule.every(10).seconds.do(self._monitor_positions)

        logger.info("Bot operativo")

        while self._running:

            schedule.run_pending()

            time.sleep(2)


# ==========================================================
# SCANS
# ==========================================================

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


# ==========================================================
# STRATEGY EXECUTION
# ==========================================================

    def _scan_swing(self):

        for market, symbols in self._market_symbol_pairs():

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

                    for strategy in self.strategies:

                        signal = strategy.analyze(df, symbol, market)

                        if signal:

                            self._process_signal(signal)

                except Exception as e:

                    logger.error(f"[swing] {symbol} {e}")


    def _scan_breakout(self):

        strategy = next((s for s in self.strategies if s.NAME == "BREAKOUT"), None)

        if not strategy:
            return

        for market, symbols in self._market_symbol_pairs():

            for symbol in symbols:

                try:

                    df = ohlcv_to_df(
                        self.exchange.fetch_ohlcv(
                            symbol,
                            settings.TF_BREAKOUT,
                            150,
                            market
                        )
                    )

                    signal = strategy.analyze(df, symbol, market)

                    if signal:

                        self._process_signal(signal)

                except Exception as e:

                    logger.error(f"[breakout] {symbol} {e}")


# ==========================================================
# SIGNAL PROCESSING
# ==========================================================

    def _process_signal(self, signal):

        now = time.time()

        last = self._last_trade.get(signal.symbol, 0)

        if now - last < TRADE_COOLDOWN:
            return

        ok, reason = self.risk.can_trade_symbol(signal.symbol, signal.market)

        if not ok:
            return

        try:

            self._execute_signal(signal)

            self._last_trade[signal.symbol] = time.time()

        except Exception as e:

            logger.error(f"[SIG] {signal.symbol} {e}")


# ==========================================================
# EXECUTION
# ==========================================================

    def _execute_signal(self, signal):

        time.sleep(ENTRY_DELAY_SECONDS)

        if signal.confidence < settings.MIN_CONFIDENCE:
            return

        balance = self.exchange.get_usdt_balance(signal.market)

        size = self.risk.position_size(
            balance,
            signal.entry,
            signal.stop_loss,
            signal.atr,
            signal.market,
            1.0,
            signal.symbol
        )

        if size <= 0:
            return

        params = {}

        if signal.market == "futures":
            params = {"reduceOnly": False}

        order = self.exchange.create_market_order(
            symbol=signal.symbol,
            side=signal.side,
            amount=size,
            market=signal.market,
            params=params,
        )

        if not order:
            return

        trade_data = {

            "entry": signal.entry,
            "size": size,
            "side": signal.side,
            "stop_loss": signal.stop_loss,
            "take_profit": signal.take_profit,
            "atr": signal.atr
        }

        self.risk.register_open(signal.symbol, trade_data, signal.market)

        logger.info(
            f"TRADE OPEN {signal.symbol} {signal.side} size={size}"
        )


# ==========================================================
# POSITION MONITOR
# ==========================================================

    def _monitor_positions(self):

        for trade in self.risk.all_open_trades():

            symbol = trade["symbol"]
            market = trade["market"]

            try:

                ticker = self.exchange.fetch_ticker(symbol, market)

                if not ticker:

                    logger.warning(f"[TICKER] missing {symbol}")

                    continue

                price = float(ticker["last"])

                close, reason = self.risk.should_close(trade, price)

                if close:

                    self._close_position(symbol, market, trade, price, reason)

            except Exception as e:

                logger.error(f"[monitor] {symbol} {e}")


# ==========================================================
# CLOSE POSITION (FIXED)
# ==========================================================

    def _close_position(self, symbol, market, trade, price, reason):

        try:

            side = "sell" if trade["side"] == "buy" else "buy"

            order = self.exchange.create_market_order(
                symbol=symbol,
                side=side,
                amount=trade["size"],
                market=market,
                params={"reduceOnly": True} if market == "futures" else {},
            )

            if not order:

                logger.warning(f"[CLOSE] failed {symbol}")

                return

            entry = trade.get("entry", price)

            if trade["side"] == "buy":

                pnl_pct = (price - entry) / entry * 100

            else:

                pnl_pct = (entry - price) / entry * 100

            self.risk.register_close(symbol, pnl_pct, market, reason)

            logger.info(
                f"TRADE CLOSED {symbol} reason={reason} pnl={pnl_pct:.2f}%"
            )

        except Exception as e:

            logger.error(f"close error {symbol} {e}")

            if "No position to close" in str(e):

                logger.warning(f"[SYNC] forcing close {symbol}")

                self.risk.force_close(symbol, market)


# ==========================================================
# BALANCE
# ==========================================================

    def _sync_balance(self):

        try:

            s = self.exchange.get_usdt_balance("spot")

            f = self.exchange.get_usdt_balance("futures")

            self.risk.session_start_balance = s + f
            self.risk.peak_balance = s + f

        except Exception as e:

            logger.warning(e)


# ==========================================================
# SYMBOLS
# ==========================================================

    def _market_symbol_pairs(self):

        pairs = []

        if "spot" in settings.MARKET_TYPES:
            pairs.append(("spot", settings.SPOT_SYMBOLS))

        if "futures" in settings.MARKET_TYPES:
            pairs.append(("futures", settings.FUTURES_SYMBOLS))

        return pairs


if __name__ == "__main__":

    TradingBot().start()