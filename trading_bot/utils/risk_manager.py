"""
Risk Manager v6
Stable production version
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

        risk_pct = settings.MAX_RISK_PCT / 100

        risk_amount = balance * risk_pct

        risk_per_unit = abs(entry - stop_loss)

        if risk_per_unit <= 0:
            return 0

        units = risk_amount / risk_per_unit

        size = units / entry

        max_notional = balance * 0.35

        if size * entry > max_notional:
            size = max_notional / entry

        return round(size, 6)


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

            self._pending_symbols.discard(symbol)


# ==========================================================
# TRADE TRACKING
# ==========================================================

    def register_open(self, symbol, trade, market):

        with self._lock:

            store = self.open_spot if market == "spot" else self.open_futures

            store[symbol] = trade


    def register_close(self, symbol, pnl_pct, market, reason=""):

        with self._lock:

            store = self.open_spot if market == "spot" else self.open_futures

            store.pop(symbol, None)

        if pnl_pct > 0:
            self.wins += 1
        else:
            self.losses += 1


# ==========================================================
# CLOSE LOGIC
# ==========================================================

    def should_close(self, trade, price):

        side = trade["side"]

        sl = trade.get("stop_loss")
        tp = trade.get("take_profit")

        if side == "buy":

            if price <= sl:
                return True, "stop_loss"

            if tp and price >= tp:
                return True, "take_profit"

        else:

            if price >= sl:
                return True, "stop_loss"

            if tp and price <= tp:
                return True, "take_profit"

        return False, ""


# ==========================================================
# OPEN TRADES
# ==========================================================

    def all_open_trades(self):

        trades = []

        for s, t in self.open_spot.items():
            trades.append({**t, "symbol": s, "market": "spot"})

        for s, t in self.open_futures.items():
            trades.append({**t, "symbol": s, "market": "futures"})

        return trades


# ==========================================================
# STATS
# ==========================================================

    def stats(self):

        total_open = len(self.open_spot) + len(self.open_futures)

        return {
            "open_trades": total_open,
            "wins": self.wins,
            "losses": self.losses
        }

# ==========================================================
# DB RECOVERY
# ==========================================================

    def recover_from_db(self):

        """
        Recupera eventuali trade aperti dal database
        quando il bot viene riavviato.
        """

        try:

            from trading_bot.models.database import Trade, DB_AVAILABLE

            if not DB_AVAILABLE:
                return

            from sqlalchemy.orm import Session
            from sqlalchemy import create_engine
            from trading_bot.config import settings

            engine = create_engine(settings.DATABASE_URL, pool_pre_ping=True)

            with Session(engine) as s:

                trades = s.query(Trade).filter(Trade.status == "open").all()

            for t in trades:

                trade_data = {
                    "order_id": t.order_id,
                    "side": t.side,
                    "entry": t.entry_price,
                    "size": t.size,
                    "stop_loss": t.stop_loss,
                    "take_profit": t.take_profit,
                    "atr": t.atr
                }

                if t.market == "spot":

                    self.open_spot[t.symbol] = trade_data

                else:

                    self.open_futures[t.symbol] = trade_data

            logger.info(f"[RISK] recovered {len(trades)} open trades")

        except Exception as e:

            logger.warning(f"[RISK] recovery error {e}")