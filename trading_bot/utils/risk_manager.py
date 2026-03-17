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
        """
        DEPRECATED: Exchange is the sole source of truth.
        Kept for backward compatibility but does nothing.
        """
        logger.info("[RISK] recover_from_db skipped — exchange is sole source of truth")

# ==========================================================
# EXCHANGE SYNC (NEW)
# ==========================================================

    def sync_from_exchange(self, exchange_positions: list):
        """
        Rebuild open_futures from real exchange positions.
        Exchange is the single source of truth.

        Returns dict with sync results: {"added": [...], "removed": [...], "updated": [...]}
        """
        result = {"added": [], "removed": [], "updated": []}

        try:
            # Build map of exchange positions: symbol -> position data
            exchange_map = {}
            for p in exchange_positions:
                size = safe_float(p.get("contracts"))
                if size <= 0:
                    continue
                symbol = p.get("symbol", "")
                exchange_map[symbol] = {
                    "side": p.get("side", ""),  # "long" or "short"
                    "size": size,
                    "entry": safe_float(p.get("entryPrice")),
                }

            with self._lock:
                # 1. Remove ghosts: in runtime but not on exchange
                ghosts = [s for s in self.open_futures if s not in exchange_map]
                for symbol in ghosts:
                    self.open_futures.pop(symbol, None)
                    self._pending_symbols.discard(symbol)
                    result["removed"].append(symbol)
                    logger.warning(f"[SYNC] ghost removed: {symbol}")

                # 2. Add missing: on exchange but not in runtime
                for symbol, edata in exchange_map.items():
                    if symbol not in self.open_futures:
                        side = "buy" if edata["side"] == "long" else "sell"
                        entry = edata["entry"]
                        trade = {
                            "symbol": symbol,
                            "side": side,
                            "entry": entry,
                            "size": edata["size"],
                            "stop_loss": entry * (0.97 if side == "buy" else 1.03),
                            "take_profit": entry * (1.04 if side == "buy" else 0.96),
                            "created_at": time.time(),
                            "market": "futures",
                        }
                        self.open_futures[symbol] = trade
                        result["added"].append(symbol)
                        logger.info(f"[SYNC] added missing: {symbol} {edata['side']}")
                    else:
                        # 3. Update size if exchange differs (exchange authoritative)
                        current = self.open_futures[symbol]
                        if abs(current.get("size", 0) - edata["size"]) > 1e-8:
                            current["size"] = edata["size"]
                            result["updated"].append(symbol)

        except Exception as e:
            logger.error(f"[SYNC] sync_from_exchange error: {e}")

        return result

# ==========================================================
# POSITION SIDE QUERIES (NEW)
# ==========================================================

    def get_position_side(self, symbol):
        """Return current position side ('long'/'short') for a symbol, or None."""
        with self._lock:
            trade = self.open_futures.get(symbol)
            if trade:
                side = trade.get("side", "")
                return "long" if side == "buy" else "short"
            trade = self.open_spot.get(symbol)
            if trade:
                return "long"  # spot is always long
            return None

    def has_opposite_position(self, symbol, new_side):
        """Check if an existing position conflicts with new_side ('buy'/'sell')."""
        existing = self.get_position_side(symbol)
        if existing is None:
            return False
        new_direction = "long" if new_side == "buy" else "short"
        return existing != new_direction



# ==========================================================
# GLOBAL RISK CONTROL (NEW)
# ==========================================================

    def can_trade(self, symbol=None):
        """
        Global risk gate. If symbol is provided, also checks:
        - symbol not already open
        - symbol not pending (being executed)
        """

        if self.global_stop:
            return False

        with self._lock:
            if len(self.open_futures) >= self.max_concurrent_trades:
                return False

            if symbol:
                if symbol in self.open_futures or symbol in self.open_spot:
                    return False
                if symbol in self._pending_symbols:
                    return False

        return True


    def is_symbol_open(self, symbol):
        """Check if a symbol already has an open position."""
        with self._lock:
            return symbol in self.open_futures or symbol in self.open_spot


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

            with self._lock:

                # Thread-safe duplicate guard
                if market == "spot":
                    if symbol in self.open_spot:
                        logger.warning(f"[RISK] duplicate blocked: {symbol} spot already open")
                        return False
                    self.open_spot[symbol] = trade
                else:
                    if symbol in self.open_futures:
                        logger.warning(f"[RISK] duplicate blocked: {symbol} futures already open")
                        return False
                    self.open_futures[symbol] = trade

                # Release pending lock now that position is registered
                self._pending_symbols.discard(symbol)

            return True

        except Exception as e:

            logger.error(f"[RISK] register_open error {e}")
            return False


    def register_close(self, symbol, pnl_pct, market, reason):

        try:

            pnl_pct = safe_float(pnl_pct)

            with self._lock:

                if pnl_pct > 0:
                    self.wins += 1
                else:
                    self.losses += 1

                self._recent_pnls.append(pnl_pct)

                if market == "spot":
                    self.open_spot.pop(symbol, None)
                else:
                    self.open_futures.pop(symbol, None)

                # Ensure pending lock is also released
                self._pending_symbols.discard(symbol)

        except Exception as e:

            logger.error(f"[RISK] register_close error {e}")


# ==========================================================
# OPEN TRADES
# ==========================================================

    def open_symbols(self):
        """Return set of all symbols with open positions."""
        with self._lock:
            return set(self.open_spot.keys()) | set(self.open_futures.keys())

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