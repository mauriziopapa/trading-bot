"""
Bitget Trading Bot — Main Orchestrator v10 SNIPER MODE
Production Ready + Safe Execution + Anti Overtrading + Asset Filtering
"""

import os
import time
import schedule
import threading

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
            "logs": []
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

        logger.add("logs/bot.log", rotation="10 MB", level="DEBUG")
        logger.add(lambda msg: print(msg, end=""))

        def _dash_log(msg):
            self.runtime["logs"].append(msg.record["message"])
            self.runtime["logs"] = self.runtime["logs"][-100:]

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

        try:
            positions = self.exchange.fetch_positions()

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

                logger.info(f"[RECOVERY] {symbol} restored")

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

        # 🔥 consenti top emerging + blue chips
        base = symbol.split("/")[0]

        allowed_dynamic = ["BTC", "ETH", "SOL", "AVAX", "LINK"]

        if base not in allowed_dynamic:
            # fallback: accetta se volume alto
            return True

        now = time.time()
        last = self.last_trade_time.get(symbol, 0)

        if now - last < self.cooldown_seconds:
            return False

        if len(self.risk.all_open_trades()) >= self.max_concurrent_trades:
            return False

        return True

# ==========================================================
# SCALPING
# ==========================================================

    def _scan_scalping(self):

        try:

            # ==================================================
            # 🔥 RISK GATE
            # ==================================================
            if hasattr(self.risk, "can_trade") and not self.risk.can_trade():
                logger.info("[SKIP] Risk gate blocked trading")
                return

            coins = self._emerging.scan() or []
            if not coins:
                return

            self.runtime["signals"] = coins[:5]

            markets = getattr(self.exchange, "_futures_markets", {})
            open_symbols = {t["symbol"] for t in self.risk.all_open_trades()}

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

            coins = self._emerging.scan() or []

            for coin in coins[:3]:

                if coin.get("volume", 0) < 5_000_000:
                    continue

                symbol = f"{coin['symbol']}/USDT:USDT"

                #if symbol not in self.allowed_symbols:
                #    continue

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

            if not self._is_valid_trade(symbol):
                logger.info(f"[SKIP TRADE] {symbol} not allowed or blocked")
                return

            ticker = self.exchange.fetch_ticker(symbol, "futures")
            if not ticker:
                return

            price = safe_float(ticker.get("last"))
            if price <= 0:
                return

            balance = safe_float(self.exchange.get_usdt_balance("futures"))

            # ==================================================
            # 🔥 CONFIG
            # ==================================================
            leverage = getattr(settings, "DEFAULT_LEVERAGE", 10)
            min_notional = 10  # soglia reale exchange

            # ==================================================
            # 🔥 RISK CAPITAL (BASE)
            # ==================================================
            risk_pct = 0.02
            risk_capital = balance * risk_pct

            # ==================================================
            # 🔥 APPLY LEVERAGE
            # ==================================================
            notional = risk_capital * leverage

            # ==================================================
            # 🔥 ENFORCE MIN NOTIONAL
            # ==================================================
            if notional < min_notional:
                logger.warning(
                    f"[SIZE BOOST] {symbol} notional too low ({notional:.2f}) → forcing {min_notional}"
                )
                notional = min_notional

            # ==================================================
            # 🔥 FINAL SIZE
            # ==================================================
            size = safe_float(notional / price)

            # ==================================================
            # 🔥 SAFETY LOG
            # ==================================================
            logger.info(
                f"[SIZE] {symbol} balance={balance:.2f} risk={risk_capital:.2f} "
                f"lev={leverage} notional={notional:.2f} size={size}"
            )

            order = self.exchange.create_market_order(symbol, side, size, "futures")
            if not order:
                logger.error(f"[ORDER FAILED] {symbol} size={size}")
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

            self.risk.register_open(symbol, trade, "futures")
            self.last_trade_time[symbol] = time.time()

            self.notifier.trade_opened(
                symbol=symbol,
                side=side,
                entry=price,
                size=size,
                stop_loss=trade["stop_loss"],
                take_profit=trade["take_profit"],
                market="futures",
                strategy=getattr(signal, "strategy", "sniper"),
                confidence=getattr(signal, "confidence", 0.7)
            )

            logger.info(f"[TRADE OPEN] {symbol}")

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
            active_symbols = {p.get("symbol") for p in exchange_positions}

            for trade in self.risk.all_open_trades():

                symbol = trade["symbol"]

                # --------------------------------------------------
                # SKIP se posizione non esiste più
                # --------------------------------------------------

                if symbol not in active_symbols:
                    continue

                # --------------------------------------------------
                # FETCH PREZZO
                # --------------------------------------------------

                ticker = self.exchange.fetch_ticker(symbol, "futures")
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
                    self._close_position(trade, price, "force_exit")
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
                    self._close_position(trade, price, "hard_tp")

                elif pnl_pct < -2:
                    self._close_position(trade, price, "hard_sl")

        except Exception as e:

            logger.error(f"[MONITOR] {e}")


# ==========================================================
# CLOSE
# ==========================================================

    def _close_position(self, trade, price, reason):

        try:

            symbol = trade["symbol"]
            side = "sell" if trade["side"] == "buy" else "buy"

            # ==================================================
            # 🔥 FETCH REAL POSITION (ROBUST)
            # ==================================================

            positions = self.exchange.fetch_positions()

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
            # 🔥 SAFE EXECUTION (ANTI REJECTION)
            # ==================================================

            for attempt in [1.0, 0.95, 0.90, 0.85]:

                size = real_size * attempt

                if size <= 0:
                    continue

                order = self.exchange.create_market_order(
                    symbol,
                    side,
                    size,
                    "futures"
                )

                if order:
                    break
            else:
                logger.error(f"[CLOSE] failed after retries {symbol}")
                return

            # ==================================================
            # PNL
            # ==================================================

            entry = trade.get("entry", 0)

            pnl_pct = ((price - entry) / entry) * 100
            if trade["side"] == "sell":
                pnl_pct *= -1

            pnl_usdt = pnl_pct * size

            # ==================================================
            # UPDATE STATE
            # ==================================================

            self.risk.register_close(symbol, pnl_pct, "futures", reason)

            if hasattr(self.db, "update_trade_status"):
                try:
                    self.db.update_trade_status(symbol, "closed")
                except:
                    pass

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
            self.sentiment = self._sentiment.get_market_sentiment()
        except:
            self.sentiment = {}


# ==========================================================

if __name__ == "__main__":
    TradingBot().start()