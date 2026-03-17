"""
Risk Manager v8.0 SNIPER HARDENED
Production ready + trailing + session protection
"""

import time
import threading
from collections import deque
from loguru import logger
from trading_bot.config import settings


# ==========================================================
# SAFE FLOAT
# ==========================================================

def safe_float(x, default=0.0):
    try:
        return float(x) if x is not None else default
    except:
        return default


class RiskManager:

    def __init__(self):

        self._lock = threading.Lock()

        self.open_spot = {}
        self.open_futures = {}

        self._pending_symbols = set()

        self.session_start_balance = 0
        self.peak_balance = 0
        self._estimated_balance = 0

        self.wins = 0
        self.losses = 0

        self._recent_pnls = deque(maxlen=100)

        self.db = None

        # 🔥 NEW
        self.max_concurrent_trades = 2
        self.global_stop = False


# ==========================================================
# BALANCE
# ==========================================================

    @property
    def estimated_balance(self):

        if self._estimated_balance > 0:
            return self._estimated_balance

        return self.session_start_balance


    def update_balance(self, balance):

        balance = safe_float(balance)

        self._estimated_balance = balance

        if self.session_start_balance == 0:
            self.session_start_balance = balance

        if balance > self.peak_balance:
            self.peak_balance = balance

# ==========================================================
# RECOVERY FROM DB (COMPATIBILITY FIX)
# ==========================================================

    def recover_from_db(self):

        try:

            if not self.db:
                logger.info("[RISK] DB non collegato — skip recovery")
                return

            if not hasattr(self.db, "get_open_trades"):
                logger.warning("[RISK] DB missing get_open_trades")
                return

            trades = self.db.get_open_trades()

            if not trades:
                logger.info("[RISK] recovered 0 open trades")
                return

            for t in trades:

                symbol = t.get("symbol")
                market = t.get("market", "futures")

                trade_data = {
                    "symbol": symbol,
                    "side": t.get("side"),
                    "entry": safe_float(t.get("entry")),
                    "size": safe_float(t.get("size")),
                    "stop_loss": safe_float(t.get("stop_loss")),
                    "take_profit": safe_float(t.get("take_profit")),
                    "created_at": t.get("created_at", time.time())
                }

                if market == "spot":
                    self.open_spot[symbol] = trade_data
                else:
                    self.open_futures[symbol] = trade_data

            logger.info(f"[RISK] recovered {len(trades)} open trades")

        except Exception as e:

            logger.error(f"[RISK] recover_from_db error {e}")



# ==========================================================
# GLOBAL RISK CONTROL (NEW)
# ==========================================================

    def can_trade(self):

        if self.global_stop:
            return False

        if len(self.open_futures) >= self.max_concurrent_trades:
            return False

        return True


    def check_global_risk(self, balance):

        if self.drawdown_exceeded(balance):
            self.global_stop = True
            logger.error("[RISK] GLOBAL STOP ACTIVATED")
            return False

        return True


# ==========================================================
# POSITION SIZE
# ==========================================================

    def position_size(self, balance, entry, stop_loss, atr=None, market="spot", symbol=""):

        try:

            balance = safe_float(balance)
            entry = safe_float(entry)
            stop_loss = safe_float(stop_loss)

            if balance <= 0 or entry <= 0:
                return 0

            risk_pct = safe_float(settings.MAX_RISK_PCT) / 100
            risk_amount = balance * risk_pct

            risk_per_unit = abs(entry - stop_loss)

            if risk_per_unit <= 0:
                risk_per_unit = entry * 0.01

            size = risk_amount / risk_per_unit

            max_notional = balance * (safe_float(settings.MAX_NOTIONAL_PCT) / 100)

            if size * entry > max_notional:
                size = max_notional / entry

            if size * entry < 20:
                return 0

            return round(size, 6)

        except Exception as e:

            logger.error(f"[RISK] position_size error {e}")
            return 0


# ==========================================================
# TRAILING STOP (NEW)
# ==========================================================

    def apply_trailing(self, trade, price):

        try:

            entry = safe_float(trade.get("entry"))
            stop = safe_float(trade.get("stop_loss"))
            side = trade.get("side")

            if entry <= 0 or price <= 0:
                return trade

            profit_pct = (price - entry) / entry * 100

            if side == "sell":
                profit_pct = -profit_pct

            # trailing attivo sopra 1.5%
            if profit_pct > 1.5:

                new_stop = price * 0.99

                if side == "buy":
                    trade["stop_loss"] = max(stop, new_stop)
                else:
                    trade["stop_loss"] = min(stop, new_stop)

            return trade

        except Exception as e:

            logger.error(f"[RISK] trailing error {e}")
            return trade


# ==========================================================
# SYMBOL LOCK
# ==========================================================

    def reserve_symbol(self, symbol):

        with self._lock:

            if symbol in self._pending_symbols:
                return False

            self._pending_symbols.add(symbol)
            return True


    def release_symbol(self, symbol):

        with self._lock:
            self._pending_symbols.discard(symbol)


# ==========================================================
# TRADE REGISTRY
# ==========================================================

    def register_open(self, symbol, trade, market):

        try:

            if symbol in self.open_futures:
                return  # 🔥 anti duplicate

            if market == "spot":
                self.open_spot[symbol] = trade
            else:
                self.open_futures[symbol] = trade

        except Exception as e:

            logger.error(f"[RISK] register_open error {e}")


    def register_close(self, symbol, pnl_pct, market, reason):

        try:

            pnl_pct = safe_float(pnl_pct)

            if pnl_pct > 0:
                self.wins += 1
            else:
                self.losses += 1

            self._recent_pnls.append(pnl_pct)

            if market == "spot":
                self.open_spot.pop(symbol, None)
            else:
                self.open_futures.pop(symbol, None)

        except Exception as e:

            logger.error(f"[RISK] register_close error {e}")


# ==========================================================
# OPEN TRADES
# ==========================================================

    def all_open_trades(self):

        trades = []

        for sym, t in self.open_spot.items():

            trade = t.copy()
            trade["symbol"] = sym
            trade["market"] = "spot"

            trades.append(trade)

        for sym, t in self.open_futures.items():

            trade = t.copy()
            trade["symbol"] = sym
            trade["market"] = "futures"

            trades.append(trade)

        return trades


# ==========================================================
# CLOSE CONDITIONS (ENHANCED)
# ==========================================================

    def should_close(self, trade, price):

        try:

            trade = self.apply_trailing(trade, price)

            entry = safe_float(trade.get("entry"))
            stop = safe_float(trade.get("stop_loss"))
            tp = safe_float(trade.get("take_profit"))
            price = safe_float(price)

            side = trade.get("side")

            if entry <= 0 or price <= 0:
                return False, None

            if side == "buy":

                if stop > 0 and price <= stop:
                    return True, "stop_loss"

                if tp > 0 and price >= tp:
                    return True, "take_profit"

            else:

                if stop > 0 and price >= stop:
                    return True, "stop_loss"

                if tp > 0 and price <= tp:
                    return True, "take_profit"

            return False, None

        except Exception as e:

            logger.error(f"[RISK] should_close error {e}")
            return False, None


# ==========================================================
# STATS
# ==========================================================

    def stats(self):

        return {
            "open_trades": len(self.open_spot) + len(self.open_futures),
            "wins": self.wins,
            "losses": self.losses
        }