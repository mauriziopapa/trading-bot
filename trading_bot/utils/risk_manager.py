"""
Risk Manager v7.4
Stable production version
Compatible with regime_detector
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
        self._estimated_balance = 0

        self.wins = 0
        self.losses = 0

        self._recent_pnls = []
        self._recent_limit = 100

        self.db = None


# ==========================================================
# BALANCE ACCESS (FIX regime detector)
# ==========================================================

    @property
    def estimated_balance(self):

        if self._estimated_balance > 0:
            return self._estimated_balance

        return self.session_start_balance


    def update_balance(self, balance):

        self._estimated_balance = balance

        if self.session_start_balance == 0:
            self.session_start_balance = balance

        if balance > self.peak_balance:
            self.peak_balance = balance


# ==========================================================
# DRAWDOWN
# ==========================================================

    def drawdown_exceeded(self, balance):

        try:

            if self.peak_balance <= 0:
                self.peak_balance = balance
                return False

            if balance > self.peak_balance:
                self.peak_balance = balance

            dd = ((self.peak_balance - balance) / self.peak_balance) * 100

            return dd > settings.MAX_DRAWDOWN_PCT

        except Exception as e:

            logger.error(f"[RISK] drawdown error {e}")
            return False


# ==========================================================
# POSITION SIZE
# ==========================================================

    def position_size(self, balance, entry, stop_loss, atr=None, market="spot", symbol=""):

        try:

            if entry <= 0:
                return 0

            risk_pct = settings.MAX_RISK_PCT / 100
            risk_amount = balance * risk_pct

            risk_per_unit = abs(entry - stop_loss)

            if risk_per_unit <= 0:
                return 0

            size = risk_amount / risk_per_unit

            max_notional = balance * (settings.MAX_NOTIONAL_PCT / 100)

            if size * entry > max_notional:
                size = max_notional / entry

            min_notional = 5

            if size * entry < min_notional:
                size = min_notional / entry

            return round(size, 6)

        except Exception as e:

            logger.error(f"[RISK] position_size error {e}")
            return 0


# ==========================================================
# RECOVER OPEN TRADES
# ==========================================================

    def recover_from_db(self):

        try:

            if not self.db:

                logger.info("[RISK] DB non collegato — skip recovery")
                return

            trades = self.db.get_open_trades()

            if not trades:

                logger.info("[RISK] recovered 0 open trades")
                return

            for t in trades:

                symbol = t.get("symbol")
                market = t.get("market", "spot")

                trade_data = {

                    "order_id": t.get("order_id"),
                    "side": t.get("side"),
                    "entry": float(t.get("entry")),
                    "size": float(t.get("size")),
                    "stop_loss": float(t.get("stop_loss")),
                    "take_profit": float(t.get("take_profit")),
                    "atr": float(t.get("atr", 0))
                }

                if market == "spot":
                    self.open_spot[symbol] = trade_data
                else:
                    self.open_futures[symbol] = trade_data

            logger.info(f"[RISK] recovered {len(trades)} open trades")

        except Exception as e:

            logger.error(f"[RISK] recover_from_db error {e}")


# ==========================================================
# LEVERAGE
# ==========================================================

    def dynamic_leverage(self, atr, entry):

        try:

            if atr <= 0 or entry <= 0:
                return settings.DEFAULT_LEVERAGE

            vol = atr / entry

            if vol < 0.01:
                return 15

            if vol < 0.02:
                return 10

            if vol < 0.04:
                return 7

            return 5

        except Exception:

            return settings.DEFAULT_LEVERAGE


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

            if market == "spot":
                self.open_spot[symbol] = trade
            else:
                self.open_futures[symbol] = trade

        except Exception as e:

            logger.error(f"[RISK] register_open error {e}")


    def register_close(self, symbol, pnl_pct, market, reason):

        try:

            if pnl_pct > 0:
                self.wins += 1
            else:
                self.losses += 1

            self._recent_pnls.append(pnl_pct)

            if len(self._recent_pnls) > self._recent_limit:
                self._recent_pnls.pop(0)

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
# CLOSE CONDITIONS
# ==========================================================

    def should_close(self, trade, price):

        try:

            entry = float(trade["entry"])
            stop = float(trade["stop_loss"])
            tp = float(trade["take_profit"])
            side = trade["side"]

            if side == "buy":

                if price <= stop:
                    return True, "stop_loss"

                if price >= tp:
                    return True, "take_profit"

            else:

                if price >= stop:
                    return True, "stop_loss"

                if price <= tp:
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


# ==========================================================
# RECENT PERFORMANCE
# ==========================================================

    def recent_stats(self):

        try:

            if not self._recent_pnls:

                return {
                    "avg_pnl": 0,
                    "win_rate": 0,
                    "trades": 0
                }

            trades = len(self._recent_pnls)

            wins = len([p for p in self._recent_pnls if p > 0])

            avg = sum(self._recent_pnls) / trades

            return {

                "avg_pnl": avg,
                "win_rate": wins / trades,
                "trades": trades
            }

        except Exception as e:

            logger.error(f"[RISK] recent_stats error {e}")

            return {

                "avg_pnl": 0,
                "win_rate": 0,
                "trades": 0
            }
