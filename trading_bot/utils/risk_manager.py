"""
Risk Manager v5 — Production Stable
════════════════════════════════════
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


def _get_cluster(symbol):

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
        self.daily_trades = 0

        self.wins = 0
        self.losses = 0

        self._consecutive_wins = 0
        self._consecutive_losses = 0

        self._recent_pnls = []

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
# DRAWDOWN
# ==========================================================

    def drawdown_exceeded(self, current_balance):

        try:

            if current_balance > self.peak_balance:
                self.peak_balance = current_balance

            if self.peak_balance == 0:
                return False

            dd = ((self.peak_balance - current_balance) / self.peak_balance) * 100

            if dd >= settings.MAX_DRAWDOWN_PCT:

                logger.error(f"[RISK] MAX DRAWDOWN {dd:.2f}%")

                return True

            return False

        except Exception as e:

            logger.warning(f"[RISK] drawdown error {e}")

            return False


# ==========================================================
# DB RECOVERY
# ==========================================================

    def recover_from_db(self):

        try:

            from trading_bot.models.database import Trade, DB_AVAILABLE

            if not DB_AVAILABLE:
                return

            from sqlalchemy.orm import Session
            from sqlalchemy import create_engine

            url = settings.DATABASE_URL

            engine = create_engine(url, pool_pre_ping=True)

            with Session(engine) as s:

                trades = s.query(Trade).filter(Trade.status == "open").all()

            with self._lock:

                for t in trades:

                    td = {

                        "order_id": t.order_id,
                        "side": t.side,
                        "entry": t.entry_price,
                        "size": t.size,
                        "stop_loss": t.stop_loss,
                        "take_profit": t.take_profit,
                        "atr": t.atr,
                        "open_ts": time.time()
                    }

                    if t.market == "spot":
                        self.open_spot[t.symbol] = td
                    else:
                        self.open_futures[t.symbol] = td

                    self._pending_symbols.add(_normalize_base(t.symbol))

            logger.info(f"[RISK] recovery {len(trades)} trades")

        except Exception as e:

            logger.error(f"[RISK] recovery error {e}")


# ==========================================================
# SYMBOL LOCK
# ==========================================================

    def reserve_symbol(self, symbol):

        base = _normalize_base(symbol)

        with self._lock:

            if base in self._pending_symbols:
                return False

            if self.symbol_in_cooldown(symbol):
                return False

            self._pending_symbols.add(base)

            return True


    def release_symbol(self, symbol):

        base = _normalize_base(symbol)

        with self._lock:
            self._pending_symbols.discard(base)


# ==========================================================
# EXCHANGE SYNC
# ==========================================================

    def force_close(self, symbol, market):

        with self._lock:

            store = self.open_spot if market == "spot" else self.open_futures

            store.pop(symbol, None)


# ==========================================================
# POSITION SIZE
# ==========================================================

    def position_size(self, balance, entry, stop_loss, atr=None, market="spot", risk_multiplier=1.0, symbol=""):

        risk_pct = settings.MAX_RISK_PCT / 100

        risk_pct *= risk_multiplier
        risk_pct *= self.adaptive_risk_multiplier()
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

        if size * entry < 6:
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
            self._consecutive_wins += 1
            self._consecutive_losses = 0

        else:

            self.losses += 1
            self._consecutive_losses += 1
            self._consecutive_wins = 0


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
