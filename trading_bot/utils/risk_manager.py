"""
Risk Manager v5.1 — Production Stable
════════════════════════════════════
Drawdown protection added
"""

import time
import threading
from loguru import logger
from trading_bot.config import settings


_CORRELATION_CLUSTERS = {
    "LAYER1": {"BTC","ETH","SOL","AVAX","DOT","NEAR","ATOM","ADA"},
    "DEFI": {"AAVE","UNI","SUSHI","CRV","MKR","COMP","SNX"},
    "MEME": {"DOGE","SHIB","PEPE","FLOKI","BONK","WIF"},
    "L2": {"MATIC","ARB","OP","STRK","ZK","MANTA"},
}


def _normalize_base(symbol: str):

    s = symbol.upper()

    if ":" in s:
        s = s.split(":")[0]

    if "/" in s:
        s = s.split("/")[0]

    if s.endswith("USDT") and len(s) > 4:
        s = s[:-4]

    return s.strip()


def _get_cluster(symbol: str):

    base = _normalize_base(symbol)

    for name, members in _CORRELATION_CLUSTERS.items():

        if base in members:
            return name

    return None


class RiskManager:

    def __init__(self):

        self._lock = threading.Lock()

        self.open_spot = {}
        self.open_futures = {}

        self._pending_symbols = set()

        self.session_start_balance = 0
        self.peak_balance = 0

        self.daily_pnl = 0
        self.daily_reset_ts = 0
        self.daily_trades = 0

        self.wins = 0
        self.losses = 0

        self.total_win_pct = 0
        self.total_loss_pct = 0

        self._consecutive_wins = 0
        self._consecutive_losses = 0

        self._recent_pnls = []

        self._in_recovery = False

        self._sl_cooldown = {}


# ==========================================================
# DRAW DOWN PROTECTION
# ==========================================================

    def drawdown_exceeded(self, current_balance):

        """
        Controlla se il drawdown massimo è stato superato
        """

        try:

            if current_balance <= 0:
                return False

            with self._lock:

                # aggiorna peak equity
                if current_balance > self.peak_balance:
                    self.peak_balance = current_balance

                peak = self.peak_balance

            if peak <= 0:
                return False

            drawdown_pct = ((peak - current_balance) / peak) * 100

            if drawdown_pct >= settings.MAX_DRAWDOWN_PCT:

                logger.error(
                    f"[RISK] MAX DRAWDOWN HIT {drawdown_pct:.2f}% "
                    f"(peak={peak:.2f} balance={current_balance:.2f})"
                )

                return True

            return False

        except Exception as e:

            logger.warning(f"[RISK] drawdown error {e}")

            return False


# ==========================================================
# DRAWDOWN STATS
# ==========================================================

    def current_drawdown(self, current_balance):

        try:

            peak = self.peak_balance

            if peak <= 0:
                return 0

            dd = ((peak - current_balance) / peak) * 100

            return round(dd, 2)

        except:

            return 0


# ==========================================================
# STATS
# ==========================================================

    def stats(self):

        try:

            open_trades = self.all_open_trades()

            total_open = len(open_trades)

            wins = self.wins
            losses = self.losses

            total_closed = wins + losses

            winrate = 0

            if total_closed > 0:
                winrate = round((wins / total_closed) * 100, 2)

            return {
                "open_trades": total_open,
                "wins": wins,
                "losses": losses,
                "winrate": winrate,
                "session_start_balance": self.session_start_balance,
                "peak_balance": self.peak_balance,
                "daily_pnl": self.daily_pnl,
                "daily_trades": self.daily_trades
            }

        except Exception as e:

            logger.warning(f"[RISK] stats error {e}")

            return {
                "open_trades": 0,
                "wins": 0,
                "losses": 0,
                "winrate": 0
            }


# ==========================================================
# SYMBOL LOCK
# ==========================================================

    def reserve_symbol(self, symbol):

        base = _normalize_base(symbol)

        with self._lock:

            if base in self._pending_symbols:
                return False

            self._pending_symbols.add(base)

            return True


    def release_symbol(self, symbol):

        base = _normalize_base(symbol)

        with self._lock:

            self._pending_symbols.discard(base)


# ==========================================================
# POSITION SIZING
# ==========================================================

    def position_size(self, balance, entry, stop_loss, atr=None, market="spot", risk_multiplier=1.0, symbol=""):

        risk_pct = settings.MAX_RISK_PCT / 100

        risk_pct *= risk_multiplier

        risk_pct *= self._correlation_discount(symbol)

        risk_pct = min(risk_pct, 0.12)

        risk_amount = balance * risk_pct

        risk_per_unit = abs(entry - stop_loss)

        if risk_per_unit <= 0:
            return 0

        units = risk_amount / risk_per_unit

        size = units / entry

        max_notional = balance * settings.MAX_NOTIONAL_PCT / 100

        notional = size * entry

        if notional > max_notional:
            size = max_notional / entry

        min_notional = 6

        if size * entry < min_notional:
            return 0

        logger.info(f"[SIZING] {symbol} size={size:.4f}")

        return round(size, 6)


# ==========================================================
# CORRELATION
# ==========================================================

    def _correlation_discount(self, symbol):

        cluster = _get_cluster(symbol)

        if cluster is None:
            return 1

        count = 0

        with self._lock:

            for s in list(self.open_spot) + list(self.open_futures):

                if _get_cluster(s) == cluster:
                    count += 1

        if count >= 3:
            return 0.4

        if count == 2:
            return 0.6

        if count == 1:
            return 0.8

        return 1


# ==========================================================
# TRADE TRACKING
# ==========================================================

    def register_open(self, symbol, trade, market):

        with self._lock:

            store = self.open_spot if market == "spot" else self.open_futures

            store[symbol] = trade

        logger.info(f"[OPEN] {symbol}")


    def register_close(self, symbol, pnl_pct, market, reason=""):

        base = _normalize_base(symbol)

        with self._lock:

            store = self.open_spot if market == "spot" else self.open_futures

            store.pop(symbol, None)

            if reason == "stop_loss":

                cooldown = 3600 if pnl_pct < -3 else 900

                self._sl_cooldown[base] = time.time() + cooldown

        if pnl_pct > 0:

            self.wins += 1

        else:

            self.losses += 1


# ==========================================================
# OPEN TRADES
# ==========================================================

    def all_open_trades(self):

        with self._lock:

            trades = []

            for s, t in self.open_spot.items():
                trades.append({**t, "symbol": s, "market": "spot"})

            for s, t in self.open_futures.items():
                trades.append({**t, "symbol": s, "market": "futures"})

        return trades
