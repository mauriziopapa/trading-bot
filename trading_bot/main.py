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

        # 🔥 EXECUTION GOVERNANCE
        self.MAX_POSITIONS = 2          # hard limit — absolute
        self.execution_lock = threading.Lock()  # serialize all trade execution
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
        Exchange is the sole source of truth.
        Sync exchange → risk manager → DB.
        """
        try:
            positions = self.exchange.fetch_positions()
            normalized = self._normalize_positions(positions)

            # Rebuild risk state from exchange
            self.risk.rebuild(normalized)

            # Mirror to DB
            try:
                self.db.replace_open_positions(self.risk.all_open_trades())
            except Exception as db_err:
                logger.error(f"[RECOVERY] DB sync failed: {db_err}")

            logger.info(f"[RECOVERY] {len(normalized)} positions synced from exchange")

        except Exception as e:
            logger.error(f"[RECOVERY] {e}")

    def _normalize_positions(self, exchange_positions):
        """Convert raw exchange positions to normalized format."""
        normalized = []
        for p in exchange_positions:
            size = safe_float(p.get("contracts"))
            if abs(size) <= 0:
                continue
            symbol = p["symbol"]
            side_raw = p.get("side", "long")
            side = "buy" if side_raw == "long" else "sell"
            entry = safe_float(p.get("entryPrice"))
            notional = safe_float(p.get("notional", 0))
            normalized.append({
                "symbol": symbol,
                "side": side,
                "size": abs(size),
                "entry": entry,
                "notional": notional,
            })
        return normalized

    def _sync_positions(self):
        """
        Full exchange → runtime → DB sync. Called before every decision cycle.
        Returns normalized exchange positions.
        """
        try:
            exchange_positions = self.exchange.fetch_positions()
            normalized = self._normalize_positions(exchange_positions)

            # Sync risk manager from exchange
            sync_result = self.risk.sync_from_exchange(exchange_positions)
            if sync_result["added"] or sync_result["removed"]:
                logger.info(
                    f"[SYNC] +{len(sync_result['added'])} added, "
                    f"-{len(sync_result['removed'])} removed"
                )

            # Mirror to DB
            try:
                self.db.replace_open_positions(self.risk.all_open_trades())
            except Exception:
                pass

            return exchange_positions, normalized
        except Exception as e:
            logger.error(f"[SYNC] error: {e}")
            return [], []


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

        # 0. Hard position limit — absolute
        if len(self.risk.open_futures) >= self.MAX_POSITIONS:
            logger.info(f"[GUARD] {symbol} blocked — hard limit {self.MAX_POSITIONS}")
            return False

        # 1. Position guard — NEVER open duplicate
        if self.risk.is_symbol_open(symbol):
            logger.info(f"[GUARD] {symbol} already has open position")
            return False

        # 2. Global risk gate — max positions, global stop, pending lock
        balance = safe_float(self.exchange.get_usdt_balance("futures"))
        if not self.risk.can_trade(symbol, available_balance=balance):
            logger.info(f"[GUARD] {symbol} blocked by risk gate")
            return False

        # 3. Exposure control — block if > 60%
        exchange_positions = self.exchange.fetch_positions()
        if not self.risk.check_exposure(balance, exchange_positions):
            logger.info(f"[GUARD] {symbol} blocked by exposure limit")
            return False

        # 4. Cooldown — prevent rapid re-entry on same symbol
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
            # RISK GATE — with balance override + daily loss check
            # ==================================================
            if not self.risk.check_daily_loss():
                logger.info("[SKIP] Daily loss limit — no new trades")
                return

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
            # RISK GATE — with balance override + daily loss check
            # ==================================================
            if not self.risk.check_daily_loss():
                logger.info("[SKIP] Daily loss limit — no emerging trades")
                return

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

        # ==================================================
        # EXECUTION LOCK — serialize all trade execution
        # ==================================================
        if not self.execution_lock.acquire(blocking=False):
            logger.info("[EXEC] execution lock busy — skipping")
            return

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
            # HARD POSITION LIMIT — absolute check before anything
            # ==================================================
            if len(self.risk.open_futures) >= self.MAX_POSITIONS:
                logger.info(
                    f"[BLOCKED] hard limit {self.MAX_POSITIONS} reached "
                    f"— rejecting {symbol}"
                )
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
                # ==================================================
                # SYNC + RE-VALIDATE — exchange truth before opening
                # ==================================================
                exchange_positions, _ = self._sync_positions()

                # Check again after sync
                if len(self.risk.open_futures) >= self.MAX_POSITIONS:
                    logger.info(f"[BLOCKED] position limit after sync — {symbol}")
                    return

                # Duplicate check (exchange may have this symbol already)
                if any(p.get("symbol") == symbol and safe_float(p.get("contracts")) > 0
                       for p in exchange_positions):
                    logger.info(f"[BLOCKED] {symbol} already on exchange")
                    return

                balance = safe_float(self.exchange.get_usdt_balance("futures"))
                if not self.risk.check_exposure(balance, exchange_positions):
                    logger.info(f"[BLOCKED] exposure limit — {symbol}")
                    return

                ticker = self.exchange.fetch_ticker(symbol, "futures")
                if not ticker:
                    return

                price = safe_float(ticker.get("last"))
                if price <= 0:
                    return

                leverage = getattr(settings, "DEFAULT_LEVERAGE", 10)

                # ==================================================
                # POSITION SIZING — dynamic risk + capital usage cap
                # (balance and exchange_positions already fetched above)
                # ==================================================
                sizing = self.risk.compute_position_size(
                    balance=balance, price=price, leverage=leverage,
                    exchange_positions=exchange_positions,
                )
                if sizing is None:
                    logger.warning(f"[BLOCKED] {symbol} sizing failed (balance={balance:.2f})")
                    return

                size = sizing["size"]
                notional = sizing["notional"]
                required_margin = sizing["required_margin"]

                risk_used = sizing.get("risk_pct", 0)
                logger.info(
                    f"[SIZE] {symbol} balance={balance:.2f} "
                    f"notional={notional:.2f} margin={required_margin:.2f} "
                    f"lev={leverage} risk={risk_used:.2%} size={size}"
                )

                # ==================================================
                # SINGLE ATTEMPT — no retry loop
                # ==================================================
                order = self.exchange.create_market_order(symbol, side, size, "futures")
                if not order:
                    logger.error(f"[ORDER FAILED] {symbol} size={size} notional={notional:.2f}")
                    return

                # SL/TP: 0.5% stop loss, 0.5% take profit (tight scalping)
                if side == "buy":
                    stop_loss = price * 0.995
                    take_profit = price * 1.005
                else:
                    stop_loss = price * 1.005
                    take_profit = price * 0.995

                trade = {
                    "symbol": symbol,
                    "side": side,
                    "entry": price,
                    "size": size,
                    "stop_loss": stop_loss,
                    "take_profit": take_profit,
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

                # Post-open sync — confirm position on exchange
                self._sync_positions()

            finally:
                # Always release if register_open didn't consume it
                self.risk.release_symbol(symbol)

        except Exception as e:
            logger.error(f"[TRADE] {e}")
        finally:
            self.execution_lock.release()


# ==========================================================
# MONITOR
# ==========================================================

    def _monitor_positions(self):

        try:

            # ==================================================
            # SYNC EXCHANGE STATE — BEFORE any decisions
            # ==================================================

            exchange_positions, normalized = self._sync_positions()

            active_symbols = {p.get("symbol") for p in exchange_positions}
            open_trades = self.risk.all_open_trades()

            # ==================================================
            # BALANCE + EXPOSURE (notional / equity)
            # ==================================================
            balance = safe_float(self.exchange.get_usdt_balance("futures"))
            self.risk.update_balance(balance)
            self.risk.check_global_risk(balance)

            exposure = self.risk.calculate_exposure(exchange_positions, balance)

            logger.info(
                f"[STATE] positions={len(open_trades)} "
                f"balance={balance:.2f} exposure={exposure:.2f} "
                f"symbols={list(self.risk.open_futures.keys())}"
            )

            # ==================================================
            # EMERGENCY RESET — abnormal margin state
            # ==================================================
            used_margin = self.risk.get_used_margin(exchange_positions)
            if used_margin > balance * 1.5 and balance > 0:
                logger.warning(
                    f"[EMERGENCY RESET] margin={used_margin:.2f} > 1.5x balance={balance:.2f}"
                )
                self.exchange.bootstrap_clean()
                self._sync_positions()
                return

            # ==================================================
            # AUTO CLEANUP — if positions exceed MAX, close worst PnL
            # ==================================================
            if len(open_trades) > self.MAX_POSITIONS:
                logger.warning(
                    f"[CLEANUP] {len(open_trades)} positions > max {self.MAX_POSITIONS}"
                )
                trades_with_pnl = []
                for t in open_trades:
                    sym = t["symbol"]
                    tk = self.exchange.fetch_ticker(sym, "futures")
                    if tk:
                        p = safe_float(tk.get("last"))
                        e = safe_float(t.get("entry"))
                        if e > 0 and p > 0:
                            pnl = (p - e) / e * 100
                            if t.get("side") == "sell":
                                pnl *= -1
                            trades_with_pnl.append((t, p, pnl))

                if trades_with_pnl:
                    trades_with_pnl.sort(key=lambda x: x[2])
                    excess = len(open_trades) - self.MAX_POSITIONS
                    for i in range(min(excess, len(trades_with_pnl))):
                        worst_trade, worst_price, worst_pnl = trades_with_pnl[i]
                        logger.info(
                            f"[CLEANUP] closing {worst_trade['symbol']} pnl={worst_pnl:.2f}%"
                        )
                        self._close_position(
                            worst_trade, worst_price, "risk_cleanup", exchange_positions
                        )
                    open_trades = self.risk.all_open_trades()

            # ==================================================
            # GHOST CLEANUP — DB positions not on exchange
            # ==================================================
            try:
                db_trades = self.db.get_open_trades() if self.db.enabled else []
                for db_pos in db_trades:
                    if db_pos["symbol"] not in active_symbols:
                        self.db.close_position_by_symbol(db_pos["symbol"], reason="ghost_db")
                        self.risk.register_close(db_pos["symbol"], 0, "futures", "ghost_db")
                        logger.warning(f"[GHOST CLEANED] {db_pos['symbol']}")
            except Exception:
                pass

            # Daily loss check
            if not self.risk.check_daily_loss():
                logger.warning("[MONITOR] daily loss limit reached — closing all positions")
                for trade in open_trades:
                    symbol = trade["symbol"]
                    if symbol in active_symbols:
                        ticker_data = self.exchange.fetch_ticker(symbol, "futures")
                        if ticker_data:
                            p = safe_float(ticker_data.get("last"))
                            if p > 0:
                                self._close_position(trade, p, "daily_loss_limit", exchange_positions)
                return

            if not open_trades:
                return

            # --------------------------------------------------
            # BATCH FETCH TICKERS (1 API call instead of N)
            # --------------------------------------------------

            trade_symbols = [t["symbol"] for t in open_trades if t["symbol"] in active_symbols]
            tickers = self.exchange.fetch_tickers_batch(trade_symbols, "futures") if trade_symbols else {}

            for trade in open_trades:

                symbol = trade["symbol"]

                # --------------------------------------------------
                # GHOST CLEANUP
                # --------------------------------------------------

                if symbol not in active_symbols:
                    logger.warning(f"[GHOST] {symbol} not on exchange — removing from tracker")
                    self.risk.register_close(symbol, 0, trade.get("market", "futures"), "ghost_cleanup")
                    continue

                # --------------------------------------------------
                # GET PRICE
                # --------------------------------------------------

                ticker = tickers.get(symbol)
                if not ticker:
                    continue

                price = safe_float(ticker.get("last"))
                if price <= 0:
                    continue

                entry = safe_float(trade.get("entry"))
                if entry <= 0:
                    continue

                # --------------------------------------------------
                # PNL CALCULATION
                # --------------------------------------------------

                pnl_pct = (price - entry) / entry * 100
                if trade["side"] == "sell":
                    pnl_pct *= -1

                age_s = time.time() - trade.get("created_at", time.time())
                age_m = age_s / 60

                logger.info(
                    f"[CHECK] {symbol} side={trade['side']} "
                    f"entry={entry:.6f} price={price:.6f} "
                    f"pnl={pnl_pct:.2f}% age={age_m:.0f}m "
                    f"sl={safe_float(trade.get('stop_loss')):.6f} "
                    f"tp={safe_float(trade.get('take_profit')):.6f}"
                )

                # ==================================================
                # EXIT ENGINE — Priority order:
                # 1. SL/TP price levels (risk.should_close)
                # 2. ProfitEngine (trailing, partials, force_close)
                # 3. Time exit
                # 4. Hard fallback limits
                # ==================================================

                # --- 1. SL/TP PRICE LEVEL CHECK (HIGHEST PRIORITY) ---
                should_exit, exit_reason = self.risk.should_close(trade, price)
                if should_exit:
                    logger.info(f"[EXIT] {symbol} reason={exit_reason} pnl={pnl_pct:.2f}%")
                    self._close_position(trade, price, exit_reason, exchange_positions)
                    continue

                # --- 2. PROFIT ENGINE (trailing stops, partials, force close) ---
                action = self.profit.update_trade(trade, price)

                if action == "force_close":
                    logger.info(f"[EXIT] {symbol} reason=profit_engine_force pnl={pnl_pct:.2f}%")
                    self._close_position(trade, price, "force_exit", exchange_positions)
                    continue

                if action and "partial_close" in action:
                    portion = 0.3 if "30" in action else 0.5 if "50" in action else 0.8
                    size = trade.get("size", 0) * portion
                    if size > 0:
                        order = self.exchange.create_market_order(
                            symbol,
                            "sell" if trade["side"] == "buy" else "buy",
                            size,
                            "futures",
                            params={"reduceOnly": True}
                        )
                        if order:
                            trade["size"] -= size
                            logger.info(f"[PARTIAL] {symbol} {portion*100:.0f}% pnl={pnl_pct:.2f}%")
                    continue

                # --- 3. TIME EXIT — 180s max hold ---
                max_hold = 180  # 3 minutes
                if age_s > max_hold:
                    logger.info(f"[EXIT] {symbol} reason=timeout age={age_s:.0f}s pnl={pnl_pct:.2f}%")
                    self._close_position(trade, price, "timeout", exchange_positions)
                    continue

                # --- 4. HARD FALLBACK — absolute limits ---
                if pnl_pct >= 1.0:
                    logger.info(f"[EXIT] {symbol} reason=hard_tp pnl={pnl_pct:.2f}%")
                    self._close_position(trade, price, "hard_tp", exchange_positions)

                elif pnl_pct <= -1.0:
                    logger.info(f"[EXIT] {symbol} reason=hard_sl pnl={pnl_pct:.2f}%")
                    self._close_position(trade, price, "hard_sl", exchange_positions)

        except Exception as e:

            logger.error(f"[MONITOR] {e}")


# ==========================================================
# CLOSE
# ==========================================================

    def _close_position(self, trade, price, reason, cached_positions=None):

        try:

            symbol = trade["symbol"]

            # ==================================================
            # FETCH FRESH EXCHANGE POSITION — exchange is truth
            # ==================================================

            positions = self.exchange.fetch_positions()

            exchange_pos = None
            for p in positions:
                if p.get("symbol") == symbol and safe_float(p.get("contracts")) > 0:
                    exchange_pos = p
                    break

            if not exchange_pos:
                logger.warning(f"[CLOSE] {symbol} not on exchange — cleaning state")
                self.risk.register_close(symbol, 0, "futures", "not_on_exchange")
                try:
                    self.db.close_position_by_symbol(symbol, reason="not_on_exchange")
                except Exception:
                    pass
                return

            # Real size and side from exchange
            real_size = safe_float(exchange_pos.get("contracts"))
            exchange_side = exchange_pos.get("side", "")  # "long" or "short"
            close_side = "sell" if exchange_side == "long" else "buy"

            logger.info(
                f"[CLOSE] {symbol} side={exchange_side} "
                f"close_side={close_side} size={real_size} reason={reason}"
            )

            # Precision normalization
            try:
                real_size = float(
                    self.exchange.futures.amount_to_precision(symbol, real_size)
                )
            except Exception:
                pass

            # ==================================================
            # CLOSE ORDER — reduceOnly, retry with size reduction
            # ==================================================

            order = None
            close_size = real_size

            for attempt in range(3):
                params = {"reduceOnly": True}
                order = self.exchange.create_market_order(
                    symbol, close_side, close_size, "futures", params=params
                )
                if order:
                    break

                close_size = round(close_size * 0.95, 6)
                logger.warning(
                    f"[CLOSE RETRY] {symbol} attempt={attempt+1} size={close_size}"
                )
                if close_size <= 0:
                    break

            if not order:
                logger.error(f"[CLOSE FAILED] {symbol} all attempts exhausted")

            # ==================================================
            # VERIFY — but NEVER block state update
            # ==================================================

            verified = False
            try:
                for _ in range(3):
                    time.sleep(0.5)
                    verify_positions = self.exchange.fetch_positions()
                    still_open = any(
                        p.get("symbol") == symbol and safe_float(p.get("contracts")) > 0
                        for p in verify_positions
                    )
                    if not still_open:
                        verified = True
                        break
                if not verified:
                    logger.warning(f"[CLOSE VERIFY] {symbol} still open after close — state updated anyway")
            except Exception as ve:
                logger.warning(f"[CLOSE VERIFY] error: {ve}")

            # ==================================================
            # PNL — always calculate
            # ==================================================

            entry = safe_float(trade.get("entry", 0))
            if entry > 0:
                pnl_pct = ((price - entry) / entry) * 100
                if exchange_side == "short":
                    pnl_pct *= -1
            else:
                pnl_pct = 0

            pnl_usdt = pnl_pct / 100 * real_size * entry

            # ==================================================
            # ALWAYS UPDATE STATE — even if verify failed
            # ==================================================

            self.risk.register_close(symbol, pnl_pct, "futures", reason)

            try:
                self.db.close_position_by_symbol(
                    symbol=symbol, exit_price=price,
                    pnl_pct=pnl_pct, pnl_usdt=pnl_usdt, reason=reason,
                )
            except Exception as db_err:
                logger.warning(f"[DB] close failed: {db_err}")

            self.notifier.trade_closed(
                symbol=symbol, side=trade["side"],
                entry=entry, exit_price=price,
                pnl_pct=pnl_pct, pnl_usdt=pnl_usdt,
                reason=reason, market="futures"
            )

            logger.info(
                f"[CLOSE OK] {symbol} pnl={pnl_pct:.2f}% "
                f"pnl_usdt={pnl_usdt:.2f} reason={reason} verified={verified}"
            )

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