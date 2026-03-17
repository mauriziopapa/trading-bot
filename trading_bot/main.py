"""
Bitget Trading Bot — Main Orchestrator v10 SNIPER MODE
Production Ready + Safe Execution + Anti Overtrading + Asset Filtering
"""

import os
import time
import schedule
import threading
from collections import deque

from datetime import datetime
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
        return float(x) if x is not None else default
    except:
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

        self.sentiment = {}
        self.active_strategy = "SNIPER"
        self._running = True

        # 🔥 CONTROLLO TRADING
        self.max_concurrent_trades = 2
        self.last_trade_time = {}
        self.cooldown_seconds = 120

        # Signal deduplication: {signal_id: timestamp}
        self._executed_signals = {}
        self._signal_dedup_ttl = 300  # 5 min TTL

        # 🔥 SOLO ASSET REALI
        self.allowed_symbols = [
            "BTC/USDT:USDT",
            "ETH/USDT:USDT",
            "SOL/USDT:USDT",
            "AVAX/USDT:USDT",
            "LINK/USDT:USDT",
            "MATIC/USDT:USDT",
            "XRP/USDT:USDT",
            "DOGE/USDT:USDT"
        ]

        self.strategies = [
            ScalpingStrategy(),
            BreakoutStrategy(),
            RSIMACDStrategy(),
            BollingerStrategy()
        ]

        self.runtime = {
            "signals": [],
            "positions": [],
            "sentiment": {},
            "logs": deque(maxlen=100)
        }


# ==========================================================
# START
# ==========================================================

    def start(self):

        self._setup_logger()

        logger.info("════════════════════════════════════")
        logger.info("BITGET TRADING BOT v10 SNIPER MODE")
        logger.info("════════════════════════════════════")

        if DASHBOARD_ENABLED:
            self._start_dashboard()

        self.exchange.initialize()
        self.db.connect()
        self.risk.db = self.db
        self._recover_positions_from_exchange()

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

        logger.add("logs/bot.log", rotation="10 MB", level="DEBUG")
        logger.add(lambda msg: print(msg, end=""))

        def _dash_log(msg):
            self.runtime["logs"].append(msg.record["message"])

        logger.add(_dash_log, level="INFO")


# ==========================================================
# DASHBOARD
# ==========================================================

    def _start_dashboard(self):

        import uvicorn

        thread = threading.Thread(
            target=lambda: uvicorn.run(
                "trading_bot.dashboard.server:app",
                host="0.0.0.0",
                port=int(os.environ.get("PORT", 8080)),
                log_level="warning"
            ),
            daemon=True
        )

        thread.start()

        logger.info("[DASHBOARD] running")


    def _update_dashboard(self):

        if not DASHBOARD_ENABLED:
            return

        try:
            self.runtime["positions"] = self.risk.all_open_trades()
            self.runtime["sentiment"] = self.sentiment
            write_state(self)
        except:
            pass


# ==========================================================
# STARTUP TELEGRAM
# ==========================================================

    def _notify_startup(self):

        try:
            self.notifier.startup("live", [], [], 0, 0)
        except:
            pass


# ==========================================================
# RECOVERY
# ==========================================================

    def _recover_positions_from_exchange(self):
        """
        Exchange is the source of truth.
        Clear risk manager state and rebuild from real exchange positions.
        """

        try:
            positions = self.exchange.fetch_positions()

            # Clear existing futures state — exchange is authoritative
            self.risk.open_futures.clear()

            recovered = 0

            for p in positions:

                size = safe_float(p.get("contracts"))
                entry = safe_float(p.get("entryPrice"))

                if size <= 0:
                    continue

                symbol = p["symbol"]
                side = "buy" if p.get("side") == "long" else "sell"

                trade = {
                    "symbol": symbol,
                    "side": side,
                    "entry": entry,
                    "size": size,
                    "stop_loss": entry * 0.97,
                    "take_profit": entry * 1.04,
                    "created_at": time.time() - 60,
                    "market": "futures"
                }

                self.risk.register_open(symbol, trade, "futures")
                recovered += 1

                logger.info(f"[RECOVERY] {symbol} restored from exchange")

            logger.info(f"[RECOVERY] {recovered} positions recovered from exchange")

            # Sync DB: replace all open positions with exchange state
            try:
                self.db.replace_open_positions(self.risk.all_open_trades())
            except Exception as db_err:
                logger.error(f"[RECOVERY] DB sync failed: {db_err}")

        except Exception as e:
            logger.error(f"[RECOVERY] {e}")


# ==========================================================
# SCHEDULER
# ==========================================================

    def _setup_scheduler(self):

        schedule.every(30).seconds.do(self._scan_scalping)
        schedule.every(2).minutes.do(self._scan_emerging)
        schedule.every(20).seconds.do(self._monitor_positions)
        schedule.every(2).minutes.do(self._update_sentiment)

        if DASHBOARD_ENABLED:
            schedule.every(10).seconds.do(self._update_dashboard)


# ==========================================================
# VALIDATION
# ==========================================================

    def _is_valid_trade(self, symbol):

        # 1. Position guard — NEVER open duplicate
        if self.risk.is_symbol_open(symbol):
            logger.info(f"[GUARD] {symbol} already has open position")
            return False

        # 2. Global risk gate — max positions, global stop, pending lock
        balance = safe_float(self.exchange.get_usdt_balance("futures"))
        if not self.risk.can_trade(symbol, available_balance=balance):
            logger.info(f"[GUARD] {symbol} blocked by risk gate")
            return False

        # 3. Cooldown — prevent rapid re-entry on same symbol
        now = time.time()
        last = self.last_trade_time.get(symbol, 0)

        if now - last < self.cooldown_seconds:
            logger.info(f"[GUARD] {symbol} in cooldown ({self.cooldown_seconds}s)")
            return False

        return True

# ==========================================================
# SCALPING
# ==========================================================

    def _scan_scalping(self):

        try:

            # ==================================================
            # RISK GATE — with balance override
            # ==================================================
            balance = safe_float(self.exchange.get_usdt_balance("futures"))
            if not self.risk.can_trade(available_balance=balance):
                logger.info("[SKIP] Risk gate blocked trading")
                return

            coins = self._emerging.scan() or []
            if not coins:
                return

            self.runtime["signals"] = coins[:5]

            markets = getattr(self.exchange, "_futures_markets", {})
            open_symbols = self.risk.open_symbols()

            for coin in coins[:5]:

                try:

                    symbol = f"{coin['symbol']}/USDT:USDT"

                    if symbol not in markets:
                        continue

                    if symbol in open_symbols:
                        logger.info(f"[SKIP] {symbol} already in open trades")
                        continue

                    # 🔥 anche protezione runtime (importantissima)
                    if symbol in self.runtime.get("active_symbols", set()):
                        logger.info(f"[SKIP] {symbol} already active (runtime)")
                        continue

                    change = coin.get("change", 0)
                    volume = coin.get("volume", 0)

                    # 🔥 meno restrittivo
                    if volume < 1_000_000:
                        continue

                    # ==================================================
                    # DATA
                    # ==================================================
                    ohlcv = self.exchange.fetch_ohlcv(symbol, "1m", 50, "futures")
                    if not ohlcv:
                        continue

                    df = ohlcv_to_df(ohlcv)
                    if df is None or df.empty:
                        continue

                    signal = None

                    # ==================================================
                    # STRATEGY ENGINE
                    # ==================================================
                    for strat in self.strategies:
                        try:
                            signal = strat.analyze(df, symbol, "futures")
                            if signal:
                                break
                        except Exception as e:
                            logger.info(f"[STRAT ERROR] {symbol} {e}")

                    # ==================================================
                    # 🔥 FALLBACK MIGLIORATO
                    # ==================================================
                    if not signal:

                        last = df["close"].iloc[-1]
                        prev = df["close"].iloc[-2]

                        ma5 = df["close"].rolling(5).mean().iloc[-1]
                        ma20 = df["close"].rolling(20).mean().iloc[-1]

                        momentum = (last - prev) / prev

                        # 🔥 filtro più realistico
                        if abs(momentum) < 0.001:  # 0.1%
                            continue

                        # LONG momentum
                        if last > ma5 and momentum > 0:
                            signal = type("Signal", (), {
                                "symbol": symbol,
                                "side": "buy",
                                "strategy": "sniper_momentum_v2",
                                "confidence": 0.7
                            })()

                        # SHORT momentum
                        elif last < ma5 and momentum < 0:
                            signal = type("Signal", (), {
                                "symbol": symbol,
                                "side": "sell",
                                "strategy": "sniper_momentum_v2",
                                "confidence": 0.7
                            })()

                    # ==================================================
                    # EXECUTION
                    # ==================================================
                    if signal:

                        logger.info(
                            f"[ENTRY] {symbol} | {signal.side} | {signal.strategy}"
                        )

                        self.runtime["signals"].append({
                            "symbol": symbol,
                            "side": signal.side,
                            "strategy": signal.strategy
                        })

                        self._execute_signal(signal)

                        # opzionale: lasciare 1 trade per ciclo
                        break

                    else:
                        logger.info(f"[NO SIGNAL] {symbol}")

                except Exception as inner:
                    logger.info(f"[SCALP INNER] {inner}")
                    continue

        except Exception as e:
            logger.error(f"[SCALP] {e}")
# ==========================================================
# EMERGING
# ==========================================================

    def _scan_emerging(self):

        try:

            # ==================================================
            # RISK GATE — with balance override
            # ==================================================
            balance = safe_float(self.exchange.get_usdt_balance("futures"))
            if not self.risk.can_trade(available_balance=balance):
                logger.info("[SKIP] Emerging blocked by risk gate")
                return

            coins = self._emerging.scan() or []
            if not coins:
                return

            markets = getattr(self.exchange, "_futures_markets", {})
            open_symbols = self.risk.open_symbols()

            for coin in coins[:3]:

                if coin.get("volume", 0) < 5_000_000:
                    continue

                symbol = f"{coin['symbol']}/USDT:USDT"

                if symbol not in markets:
                    continue

                # Position guard — skip if already open
                if symbol in open_symbols:
                    logger.info(f"[SKIP] {symbol} already open (emerging)")
                    continue

                ohlcv = self.exchange.fetch_ohlcv(symbol, "5m", 120, "futures")
                if not ohlcv:
                    continue

                df = ohlcv_to_df(ohlcv)

                for strat in self.strategies:

                    signal = strat.analyze(df, symbol, "futures")

                    if signal:
                        logger.info(f"[EMERGING SIGNAL] {symbol}")
                        self._execute_signal(signal)
                        break

        except Exception as e:
            logger.error(f"[EMERGING] {e}")


# ==========================================================
# EXECUTE
# ==========================================================

    def _execute_signal(self, signal):

        try:

            symbol = signal.symbol
            side = signal.side
            strategy = getattr(signal, "strategy", "sniper")

            # ==================================================
            # SIGNAL DEDUP — prevent executing same signal twice
            # ==================================================
            signal_id = f"{symbol}_{strategy}_{side}"
            now = time.time()

            # Clean expired entries
            expired = [k for k, ts in self._executed_signals.items()
                       if now - ts > self._signal_dedup_ttl]
            for k in expired:
                del self._executed_signals[k]

            if signal_id in self._executed_signals:
                logger.info(f"[DEDUP] {signal_id} already executed recently")
                return

            # ==================================================
            # VALIDATION — position guard + risk gate + cooldown
            # ==================================================
            if not self._is_valid_trade(symbol):
                logger.info(f"[SKIP TRADE] {symbol} not allowed or blocked")
                return

            # ==================================================
            # OPPOSITE POSITION GUARD — prevent both LONG+SHORT
            # ==================================================
            if self.risk.has_opposite_position(symbol, side):
                logger.warning(f"[GUARD] {symbol} has opposite position — blocking new {side}")
                return

            # ==================================================
            # RESERVE SYMBOL — atomic lock to prevent race conditions
            # ==================================================
            if not self.risk.reserve_symbol(symbol):
                logger.info(f"[SKIP TRADE] {symbol} already being executed")
                return

            try:

                ticker = self.exchange.fetch_ticker(symbol, "futures")
                if not ticker:
                    return

                price = safe_float(ticker.get("last"))
                if price <= 0:
                    return

                balance = safe_float(self.exchange.get_usdt_balance("futures"))
                leverage = getattr(settings, "DEFAULT_LEVERAGE", 10)

                # ==================================================
                # POSITION SIZING — correct formula with margin check
                # ==================================================
                sizing = self.risk.compute_position_size(
                    balance=balance, price=price, leverage=leverage,
                )
                if sizing is None:
                    logger.warning(f"[BLOCKED] {symbol} sizing failed (balance={balance:.2f})")
                    return

                size = sizing["size"]
                notional = sizing["notional"]
                required_margin = sizing["required_margin"]

                logger.info(
                    f"[SIZE] {symbol} balance={balance:.2f} "
                    f"notional={notional:.2f} margin={required_margin:.2f} "
                    f"lev={leverage} size={size}"
                )

                # ==================================================
                # SINGLE ATTEMPT — no retry loop
                # ==================================================
                order = self.exchange.create_market_order(symbol, side, size, "futures")
                if not order:
                    logger.error(f"[ORDER FAILED] {symbol} size={size} notional={notional:.2f}")
                    return

                trade = {
                    "symbol": symbol,
                    "side": side,
                    "entry": price,
                    "size": size,
                    "stop_loss": price * 0.97,
                    "take_profit": price * 1.04,
                    "created_at": time.time(),
                    "market": "futures"
                }

                # register_open releases the pending lock internally
                self.risk.register_open(symbol, trade, "futures")
                self.last_trade_time[symbol] = time.time()
                self._executed_signals[signal_id] = now

                # Persist to DB
                try:
                    order_id = order.get("id", f"ord_{int(time.time())}_{symbol}")
                    self.db.save_trade_open(
                        order_id=order_id,
                        symbol=symbol, market="futures", strategy=strategy,
                        side=side, entry=price, size=size,
                        stop_loss=trade["stop_loss"],
                        take_profit=trade["take_profit"],
                        confidence=getattr(signal, "confidence", 0.7),
                        atr=0, notes="", timeframe="1m",
                        leverage=leverage,
                    )
                except Exception as db_err:
                    logger.warning(f"[DB] save_trade_open failed: {db_err}")

                self.notifier.trade_opened(
                    symbol=symbol,
                    side=side,
                    entry=price,
                    size=size,
                    stop_loss=trade["stop_loss"],
                    take_profit=trade["take_profit"],
                    market="futures",
                    strategy=strategy,
                    confidence=getattr(signal, "confidence", 0.7)
                )

                logger.info(f"[TRADE OPEN] {symbol}")

            finally:
                # Always release if register_open didn't consume it
                self.risk.release_symbol(symbol)

        except Exception as e:
            logger.error(f"[TRADE] {e}")


# ==========================================================
# MONITOR
# ==========================================================

    def _monitor_positions(self):

        try:

            # ==================================================
            # 🔥 SYNC POSIZIONI REALI EXCHANGE
            # ==================================================

            exchange_positions = self.exchange.fetch_positions()

            # Sync runtime state from exchange (add missing, remove ghosts, update sizes)
            sync_result = self.risk.sync_from_exchange(exchange_positions)
            if sync_result["added"] or sync_result["removed"]:
                logger.info(
                    f"[SYNC] +{len(sync_result['added'])} added, "
                    f"-{len(sync_result['removed'])} removed"
                )
                # Sync DB for ghost removals
                for sym in sync_result["removed"]:
                    try:
                        self.db.close_position_by_symbol(sym, reason="ghost_sync")
                    except Exception:
                        pass
                # Sync DB for newly discovered positions
                for sym in sync_result["added"]:
                    try:
                        trade = self.risk.open_futures.get(sym, {})
                        order_id = f"sync_{int(time.time())}_{sym.replace('/', '_')}"
                        self.db.save_trade_open(
                            order_id=order_id, symbol=sym, market="futures",
                            strategy="exchange_sync", side=trade.get("side", "buy"),
                            entry=safe_float(trade.get("entry")),
                            size=safe_float(trade.get("size")),
                            stop_loss=safe_float(trade.get("stop_loss")),
                            take_profit=safe_float(trade.get("take_profit")),
                            confidence=0, atr=0, notes="synced from exchange",
                            timeframe="", leverage=getattr(settings, "DEFAULT_LEVERAGE", 10),
                        )
                    except Exception:
                        pass

            active_symbols = {p.get("symbol") for p in exchange_positions}
            open_trades = self.risk.all_open_trades()

            logger.info(
                f"[MONITOR] exchange_positions={len(exchange_positions)} "
                f"risk_positions={len(self.risk.open_futures)} "
                f"symbols={list(self.risk.open_futures.keys())}"
            )

            # --------------------------------------------------
            # BATCH FETCH TICKERS (1 API call instead of N)
            # --------------------------------------------------

            trade_symbols = [t["symbol"] for t in open_trades if t["symbol"] in active_symbols]
            tickers = self.exchange.fetch_tickers_batch(trade_symbols, "futures") if trade_symbols else {}

            for trade in open_trades:

                symbol = trade["symbol"]

                # --------------------------------------------------
                # GHOST CLEANUP — position gone from exchange, remove from risk manager
                # --------------------------------------------------

                if symbol not in active_symbols:
                    logger.warning(f"[GHOST] {symbol} not on exchange — removing from tracker")
                    self.risk.register_close(symbol, 0, trade.get("market", "futures"), "ghost_cleanup")
                    continue

                # --------------------------------------------------
                # GET PREZZO FROM BATCH
                # --------------------------------------------------

                ticker = tickers.get(symbol)
                if not ticker:
                    continue

                price = safe_float(ticker.get("last"))

                if price <= 0:
                    continue

                # --------------------------------------------------
                # 🔥 PROFIT ENGINE (CORE)
                # --------------------------------------------------

                action = self.profit.update_trade(trade, price)

                # --------------------------------------------------
                # 🔥 FORCE CLOSE
                # --------------------------------------------------

                if action == "force_close":
                    self._close_position(trade, price, "force_exit", exchange_positions)
                    continue

                # --------------------------------------------------
                # 🔥 PARTIAL CLOSE
                # --------------------------------------------------

                if action and "partial_close" in action:

                    portion = 0.3 if "30" in action else 0.5 if "50" in action else 0.8

                    size = trade.get("size", 0) * portion

                    if size > 0:

                        order = self.exchange.create_market_order(
                            symbol,
                            "sell" if trade["side"] == "buy" else "buy",
                            size,
                            "futures"
                        )

                        if order:
                            trade["size"] -= size

                            logger.info(f"[PARTIAL] {symbol} {portion*100:.0f}%")

                    continue

                # --------------------------------------------------
                # 🔥 HARD STOP / TP BACKUP
                # --------------------------------------------------

                entry = safe_float(trade.get("entry"))

                if entry <= 0:
                    continue

                pnl_pct = (price - entry) / entry * 100

                if trade["side"] == "sell":
                    pnl_pct *= -1

                # fallback sicurezza
                if pnl_pct > 4:
                    self._close_position(trade, price, "hard_tp", exchange_positions)

                elif pnl_pct < -2:
                    self._close_position(trade, price, "hard_sl", exchange_positions)

        except Exception as e:

            logger.error(f"[MONITOR] {e}")


# ==========================================================
# CLOSE
# ==========================================================

    def _close_position(self, trade, price, reason, cached_positions=None):

        try:

            symbol = trade["symbol"]
            side = "sell" if trade["side"] == "buy" else "buy"

            # ==================================================
            # 🔥 FETCH REAL POSITION (ROBUST) — reuse cached if available
            # ==================================================

            positions = cached_positions or self.exchange.fetch_positions()

            real_size = 0

            for p in positions:
                if p.get("symbol") == symbol:

                    # 🔥 usa contracts ma fallback su info
                    real_size = float(
                        p.get("contracts")
                        or p.get("info", {}).get("total")
                        or 0
                    )
                    break

            if real_size <= 0:
                logger.warning(f"[CLOSE] no position {symbol}")
                return

            # ==================================================
            # 🔥 PRECISION NORMALIZATION
            # ==================================================

            try:
                real_size = float(
                    self.exchange.futures.amount_to_precision(symbol, real_size)
                )
            except:
                pass

            # ==================================================
            # CLOSE ORDER — single attempt, no retry loop
            # ==================================================

            order = self.exchange.create_market_order(
                symbol, side, real_size, "futures"
            )

            if not order:
                logger.error(f"[CLOSE FAILED] {symbol} size={real_size}")
                return

            # ==================================================
            # CLOSE VERIFICATION — confirm position is gone
            # ==================================================

            try:
                time.sleep(0.5)
                verify_positions = self.exchange.fetch_positions()
                still_open = any(
                    p.get("symbol") == symbol and safe_float(p.get("contracts")) > 0
                    for p in verify_positions
                )
                if still_open:
                    logger.error(f"[CLOSE VERIFY] {symbol} still open after close order — skipping state update")
                    return
            except Exception as ve:
                logger.warning(f"[CLOSE VERIFY] verification failed: {ve} — proceeding")

            # ==================================================
            # PNL
            # ==================================================

            entry = trade.get("entry", 0)

            pnl_pct = ((price - entry) / entry) * 100
            if trade["side"] == "sell":
                pnl_pct *= -1

            pnl_usdt = pnl_pct * real_size

            # ==================================================
            # UPDATE STATE
            # ==================================================

            self.risk.register_close(symbol, pnl_pct, "futures", reason)

            try:
                self.db.close_position_by_symbol(
                    symbol=symbol, exit_price=price,
                    pnl_pct=pnl_pct, pnl_usdt=pnl_usdt, reason=reason,
                )
            except Exception as db_err:
                logger.warning(f"[DB] close_position_by_symbol failed: {db_err}")

            self.notifier.trade_closed(
                symbol=symbol,
                side=trade["side"],
                entry=entry,
                exit_price=price,
                pnl_pct=pnl_pct,
                pnl_usdt=pnl_usdt,
                reason=reason,
                market="futures"
            )

            logger.info(f"[CLOSE] {symbol} PnL={pnl_pct:.2f}%")

        except Exception as e:

            logger.error(f"[CLOSE ERROR] {e}")


# ==========================================================

    def _update_sentiment(self):
        try:
            self.sentiment = self._sentiment.get_sentiment()
        except Exception as e:
            logger.debug(f"[SENTIMENT] update failed: {e}")
            self.sentiment = {}


# ==========================================================

if __name__ == "__main__":
    TradingBot().start()