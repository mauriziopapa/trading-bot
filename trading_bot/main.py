"""
Bitget Trading Bot — Main Orchestrator v5.2
════════════════════════════════════════════

Fix:
✓ dashboard ripristinata
✓ telegram notifier ripristinato
✓ emerging scanner fix
✓ cooldown fix
✓ exchange safety
"""

import os
import time
import schedule
import threading
import uvicorn

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


# dashboard
try:
    from trading_bot.dashboard.state_writer import write_state
    DASHBOARD_ENABLED = True
except:
    DASHBOARD_ENABLED = False


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


# ============================================================
# DASHBOARD SERVER
# ============================================================

    def _start_dashboard_server(self):

        port = int(os.environ.get("PORT", settings.DASHBOARD_PORT))

        config = uvicorn.Config(
            "trading_bot.dashboard.server:app",
            host="0.0.0.0",
            port=port,
            log_level="warning",
            access_log=False,
        )

        server = uvicorn.Server(config)

        thread = threading.Thread(
            target=server.run,
            daemon=True
        )

        thread.start()

        logger.info(f"[DASHBOARD] avviata su porta {port}")


# ============================================================
# START
# ============================================================

    def start(self):

        logger.info("Starting Trading Bot v5.2")

        if DASHBOARD_ENABLED:
            self._start_dashboard_server()

        self.exchange.initialize()
        self.db.connect()
        self.risk.recover_from_db()

        self._sync_balance()

        self._check_regime()

        # telegram startup
        try:

            spot = []
            futures = []

            if "spot" in settings.MARKET_TYPES:
                spot = settings.SPOT_SYMBOLS

            if "futures" in settings.MARKET_TYPES:
                futures = settings.FUTURES_SYMBOLS

            bs = self.exchange.get_usdt_balance("spot")
            bf = self.exchange.get_usdt_balance("futures")

            self.notifier.startup(
                settings.TRADING_MODE,
                spot,
                futures,
                bs,
                bf
            )

        except Exception as e:
            logger.warning(f"telegram startup error {e}")

        schedule.every().minute.do(self._scan_swing_if_candle_closed)

        schedule.every().minute.do(self._scan_breakout_if_candle_closed)

        if settings.ENABLE_SCALPING:
            schedule.every(45).seconds.do(self._scan_scalping)

        schedule.every(10).minutes.do(self._scan_emerging)

        schedule.every(10).seconds.do(self._monitor_positions)

        schedule.every(1).hours.do(self._health_check)

        schedule.every(10).minutes.do(self._auto_rebalance)

        if DASHBOARD_ENABLED:
            schedule.every(20).seconds.do(lambda: write_state(self))

        logger.info("Bot operativo")

        while self._running:
            schedule.run_pending()
            time.sleep(2)


# ============================================================
# SCANS
# ============================================================

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


    def _scan_emerging(self):

        try:

            coins = self._emerging.scan()

            if not coins:
                return

            logger.info(f"[EMERGING] trovate {len(coins)} opportunità")

        except Exception as e:
            logger.error(f"[emerging] {e}")


# ============================================================
# SIGNAL
# ============================================================

    def _process_signal(self, signal, risk_multiplier=1.0):

        key = f"{signal.market}:{signal.symbol}"

        now = time.time()

        last = self._last_trade.get(key, 0)

        if now - last < TRADE_COOLDOWN:
            return

        ok, reason = self.risk.can_trade_symbol(signal.symbol, signal.market)

        if not ok:
            return

        try:

            self._execute_signal(signal, risk_multiplier)

            self._last_trade[key] = time.time()

        except Exception as e:
            logger.error(f"[SIG] {signal.symbol}: {e}")


    def _execute_signal(self, signal, risk_multiplier=1.0):

        time.sleep(ENTRY_DELAY_SECONDS)

        if signal.confidence < settings.MIN_CONFIDENCE:
            return

        balance = self.exchange.get_usdt_balance(signal.market)

        if balance < 5:
            return

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

        self.risk.register_open(signal.symbol, {"order_id": oid}, signal.market)

        try:

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
            logger.warning(f"telegram trade_open error {e}")

        logger.info(f"TRADE OPEN {signal.symbol} {signal.market} size={size}")


# ============================================================
# MONITOR
# ============================================================

    def _monitor_positions(self):

        for t in self.risk.all_open_trades():

            sym = t["symbol"]
            mkt = t["market"]

            try:

                ticker = self.exchange.fetch_ticker(sym, mkt)

                if not ticker:
                    continue

                price = float(ticker["last"])

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

        try:

            self.notifier.trade_closed(
                symbol=sym,
                side=trade["side"],
                entry=trade["entry"],
                exit_price=price,
                pnl_pct=0,
                pnl_usdt=0,
                reason=reason,
                market=mkt
            )

        except:
            pass

        logger.info(f"TRADE CLOSED {sym} reason={reason}")


# ============================================================
# UTILS
# ============================================================

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
