"""
Risk Manager v7
Improved execution stability
"""

import time
import threading

from loguru import logger
from trading_bot.config import settings


class RiskManager:

    def __init__(self):

        self._lock = threading.Lock()

        self.open_spot = {}
        self.open_futures = {}

        self._pending_symbols = set()

        self.session_start_balance = 0
        self.peak_balance = 0

        self.wins = 0
        self.losses = 0


# ==========================================================
# DRAWDOWN
# ==========================================================

    def drawdown_exceeded(self, balance):

        if self.peak_balance == 0:
            self.peak_balance = balance

        if balance > self.peak_balance:
            self.peak_balance = balance

        dd = ((self.peak_balance - balance) / self.peak_balance) * 100

        return dd > settings.MAX_DRAWDOWN_PCT


# ==========================================================
# POSITION SIZE
# ==========================================================

    def position_size(self, balance, entry, stop_loss, atr=None, market="spot", symbol=""):

        try:

            risk_pct = settings.MAX_RISK_PCT / 100

            risk_amount = balance * risk_pct

            risk_per_unit = abs(entry - stop_loss)

            if risk_per_unit <= 0:
                return 0

            # quantità asset
            size = risk_amount / risk_per_unit

            # limite esposizione (max 35% capitale)
            max_notional = balance * 0.35

            if size * entry > max_notional:
                size = max_notional / entry

            # minimo trade
            min_notional = 5

            if size * entry < min_notional:
                size = min_notional / entry

            return round(size, 6)

        except Exception as e:

            logger.error(f"[RISK] position_size error {e}")
            return 0


# ==========================================================
# LEVERAGE
# ==========================================================

    def dynamic_leverage(self, atr, entry):

        if atr <= 0 or entry <= 0:
            return 5

        vol = atr / entry

        if vol < 0.01:
            return 15

        if vol < 0.02:
            return 10

        if vol < 0.04:
            return 7

        return 5


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

            if symbol in self._pending_symbols:
                self._pending_symbols.remove(symbol)


# ==========================================================
# TRADE REGISTRY
# ==========================================================

    def register_open(self, symbol, trade, market):

        if market == "spot":
            self.open_spot[symbol] = trade
        else:
            self.open_futures[symbol] = trade


    def register_close(self, symbol, pnl_pct, market, reason):

        if pnl_pct > 0:
            self.wins += 1
        else:
            self.losses += 1

        if market == "spot":
            self.open_spot.pop(symbol, None)
        else:
            self.open_futures.pop(symbol, None)


# ==========================================================
# OPEN TRADES
# ==========================================================

    def all_open_trades(self):

        trades = []

        for sym, t in self.open_spot.items():
            t["symbol"] = sym
            t["market"] = "spot"
            trades.append(t)

        for sym, t in self.open_futures.items():
            t["symbol"] = sym
            t["market"] = "futures"
            trades.append(t)

        return trades


    def stats(self):

        return {
            "open_trades": len(self.open_spot) + len(self.open_futures),
            "wins": self.wins,
            "losses": self.losses
        }