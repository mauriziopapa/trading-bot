"""
Risk Manager v4 — Fix Duplicati + Recovery DB + Sizing Aggressivo
═══════════════════════════════════════════════════════════════
FIX CRITICI:
  ✓ DUPLICATE: normalizza base asset (XRP/USDT:USDT → XRP)
  ✓ DUPLICATE: check cross-market (spot+futures stesso base)
  ✓ DUPLICATE: _pending_symbols previene race condition tra scan
  ✓ DUPLICATE: reserve_symbol/release_symbol pattern
  ✓ DB RECOVERY: carica posizioni aperte dal DB al restart
  ✓ Futures cap sul MARGINE (non nozionale)
  ✓ Kelly min 100 trade
  ✓ Thread-safe con Lock
"""

import time
import threading
from datetime import datetime, timezone
from loguru import logger
from trading_bot.config import settings


_CORRELATION_CLUSTERS = {
    "LAYER1": {"BTC", "ETH", "SOL", "AVAX", "DOT", "NEAR", "ATOM", "ADA"},
    "DEFI":   {"AAVE", "UNI", "SUSHI", "CRV", "MKR", "COMP", "SNX"},
    "MEME":   {"DOGE", "SHIB", "PEPE", "FLOKI", "BONK", "WIF"},
    "L2":     {"MATIC", "ARB", "OP", "STRK", "ZK", "MANTA"},
}


def _normalize_base(symbol: str) -> str:
    """
    Normalizza un simbolo al base asset.
      BTC/USDT       → BTC
      BTC/USDT:USDT  → BTC
      BTCUSDT        → BTC
      BTC            → BTC
    """
    s = symbol.upper()
    if ":" in s:
        s = s.split(":")[0]
    if "/" in s:
        s = s.split("/")[0]
    if s.endswith("USDT") and len(s) > 4:
        s = s[:-4]
    return s.strip()


def _get_cluster(symbol: str) -> str | None:
    base = _normalize_base(symbol)
    for name, members in _CORRELATION_CLUSTERS.items():
        if base in members:
            return name
    return None


class RiskManager:

    def __init__(self):
        self._lock = threading.Lock()
        self.open_spot: dict[str, dict]    = {}
        self.open_futures: dict[str, dict] = {}
        self._pending_symbols: set[str]    = set()

        self.session_start_balance: float = 0.0
        self.peak_balance: float          = 0.0
        self.daily_pnl: float             = 0.0
        self.daily_reset_ts: float        = 0.0
        self.daily_trades: int            = 0

        self.wins: int   = 0
        self.losses: int = 0
        self.total_win_pct: float  = 0.0
        self.total_loss_pct: float = 0.0

        self._consecutive_wins: int   = 0
        self._consecutive_losses: int = 0
        self._recent_pnls: list[float] = []
        self._in_recovery: bool  = False
        self._recovery_ts: float = 0.0

    # ─── DB Recovery ─────────────────────────────────────────────────────────

    def recover_from_db(self):
        """Carica posizioni aperte dal DB. Chiamare DOPO db.connect()."""
        try:
            from trading_bot.models.database import Trade, DB_AVAILABLE
            if not DB_AVAILABLE:
                logger.warning("[RISK] DB non disponibile — no recovery")
                return
            from sqlalchemy.orm import Session
            from sqlalchemy import create_engine
            url = settings.DATABASE_URL
            if not url:
                return
            engine = create_engine(url, pool_pre_ping=True)
            with Session(engine) as s:
                open_trades = s.query(Trade).filter(Trade.status == "open").all()
            if not open_trades:
                logger.info("[RISK] Nessuna posizione aperta nel DB")
                return

            with self._lock:
                for t in open_trades:
                    td = {
                        "order_id":    t.order_id,
                        "side":        t.side,
                        "entry":       t.entry_price,
                        "size":        t.size,
                        "stop_loss":   t.stop_loss,
                        "take_profit": t.take_profit,
                        "strategy":    t.strategy,
                        "confidence":  t.confidence,
                        "open_ts":     t.opened_at.timestamp() if t.opened_at else time.time(),
                        "atr":         t.atr or 0,
                    }
                    if t.market == "spot":
                        self.open_spot[t.symbol] = td
                    else:
                        self.open_futures[t.symbol] = td
                n = len(self.open_spot) + len(self.open_futures)

            logger.info(f"[RISK] Recovery: {n} posizioni caricate dal DB")
            for t in open_trades:
                logger.info(f"  → {t.market} {t.side} {t.symbol} entry={t.entry_price}")
        except Exception as e:
            logger.error(f"[RISK] Recovery fallito: {e}")

    # ─── Guard checks ────────────────────────────────────────────────────────

    def can_trade(self, market: str = "spot") -> tuple[bool, str]:
        self._maybe_reset_daily()
        if self.daily_pnl <= -(settings.MAX_DAILY_LOSS_PCT):
            return False, f"Circuit breaker: daily loss {self.daily_pnl:.1f}%"
        if self.peak_balance > 0:
            dd = (self.peak_balance - self._estimated_balance()) / self.peak_balance * 100
            if dd >= settings.MAX_DRAWDOWN_PCT:
                return False, f"Max drawdown: {dd:.1f}%"
            rt = settings.MAX_DRAWDOWN_PCT * 0.6
            if dd >= rt and not self._in_recovery:
                self._in_recovery = True
                self._recovery_ts = time.time()
                logger.warning(f"[RISK] RECOVERY MODE — DD={dd:.1f}%")
            elif dd < rt * 0.5 and self._in_recovery:
                self._in_recovery = False
        with self._lock:
            if market == "spot":
                if len(self.open_spot) >= settings.MAX_POSITIONS_SPOT:
                    return False, f"Max pos spot ({len(self.open_spot)}/{settings.MAX_POSITIONS_SPOT})"
            else:
                if len(self.open_futures) >= settings.MAX_POSITIONS_FUTURES:
                    return False, f"Max pos futures ({len(self.open_futures)}/{settings.MAX_POSITIONS_FUTURES})"
        return True, ""

    def can_trade_symbol(self, symbol: str, market: str = "spot") -> tuple[bool, str]:
        """
        FIX CRITICO: Check base asset su TUTTI i mercati + pending.
        Previene duplicati come XRP spot + XRP futures + XRP×3 futures.
        """
        base = _normalize_base(symbol)
        with self._lock:
            # Check esatto
            store = self.open_spot if market == "spot" else self.open_futures
            if symbol in store:
                return False, f"Già aperta: {symbol} ({market})"
            # Check base asset cross-market
            for sym in list(self.open_spot.keys()) + list(self.open_futures.keys()):
                if _normalize_base(sym) == base:
                    return False, f"Già aperta su {base} (come {sym})"
            # Check pending
            if base in self._pending_symbols:
                return False, f"{base} ordine in corso"
        return self.can_trade(market)

    def reserve_symbol(self, symbol: str) -> bool:
        """Riserva base asset prima dell'ordine. Ritorna False se già riservato."""
        base = _normalize_base(symbol)
        with self._lock:
            if base in self._pending_symbols:
                return False
            # Ricontrolla anche le posizioni aperte
            for sym in list(self.open_spot.keys()) + list(self.open_futures.keys()):
                if _normalize_base(sym) == base:
                    return False
            self._pending_symbols.add(base)
            return True

    def release_symbol(self, symbol: str):
        base = _normalize_base(symbol)
        with self._lock:
            self._pending_symbols.discard(base)

    # ─── Position Sizing ─────────────────────────────────────────────────────

    def position_size(self, balance: float, entry: float, stop_loss: float,
                      atr: float = None, market: str = "spot",
                      risk_multiplier: float = 1.0, symbol: str = "") -> float:
        risk_pct = settings.MAX_RISK_PCT / 100
        risk_pct *= self._half_kelly() * risk_multiplier

        # Streak
        if self._consecutive_wins >= 5:     risk_pct *= 1.25
        elif self._consecutive_wins >= 3:   risk_pct *= 1.15
        elif self._consecutive_losses >= 3: risk_pct *= 0.70

        if self._in_recovery:
            risk_pct *= 0.50

        risk_pct *= self._correlation_discount(symbol, market)
        # Cap assoluto — 15% per permettere full exposure mode
        risk_pct = min(risk_pct, 0.15)

        if atr is not None and entry > 0:
            atr_pct = atr / entry
            if atr_pct > 0.06:    risk_pct *= 0.5
            elif atr_pct > 0.04:  risk_pct *= 0.7
            elif atr_pct < 0.01:  risk_pct *= 1.3
            elif atr_pct < 0.015: risk_pct *= 1.15

        risk_amount  = balance * risk_pct
        risk_per_unit = abs(entry - stop_loss)
        if risk_per_unit <= 0:
            return 0.0

        size = risk_amount / risk_per_unit
        leverage = settings.DEFAULT_LEVERAGE

        if market == "futures":
            size = size * leverage / entry
        else:
            size = size / entry

        # MAX_NOTIONAL_PCT configurabile dal DB — default 40%
        # Per "full exposure" si alza a 80-90%
        try:
            MAX_NOTIONAL_PCT = float(getattr(settings, 'MAX_NOTIONAL_PCT', 40)) / 100
        except Exception:
            MAX_NOTIONAL_PCT = 0.40
        MAX_NOTIONAL_PCT = max(0.10, min(0.95, MAX_NOTIONAL_PCT))  # clamp 10-95%
        MIN_NOTIONAL_USDT = 6.0

        if market == "futures":
            margin_cost = size * entry / leverage
            max_margin  = balance * MAX_NOTIONAL_PCT
            if margin_cost > max_margin:
                size = max_margin * leverage / entry
            actual_cost = size * entry / leverage
        else:
            actual_cost = size * entry
            if actual_cost > balance * MAX_NOTIONAL_PCT:
                size = balance * MAX_NOTIONAL_PCT / entry
                actual_cost = size * entry

        if actual_cost < MIN_NOTIONAL_USDT:
            return 0.0

        logger.info(
            f"[SIZING] {market} {symbol} bal={balance:.0f} risk={risk_pct*100:.2f}% "
            f"→ size={size:.6f} cost={actual_cost:.2f}"
        )
        return round(size, 6)

    def _correlation_discount(self, symbol: str, market: str) -> float:
        cluster = _get_cluster(symbol)
        if cluster is None:
            return 1.0
        count = 0
        with self._lock:
            for s in list(self.open_spot.keys()) + list(self.open_futures.keys()):
                if _get_cluster(s) == cluster:
                    count += 1
        if count >= 2:   return 0.55
        elif count == 1: return 0.75
        return 1.0

    # ─── Stops ───────────────────────────────────────────────────────────────

    def calculate_stops(self, entry, side, atr, sl_atr_mult=1.8, spread_buffer=0.0):
        sl = atr * sl_atr_mult + spread_buffer
        tp = sl * settings.TAKE_PROFIT_RATIO
        if side == "buy":
            return round(entry - sl, 4), round(entry + tp, 4)
        return round(entry + sl, 4), round(entry - tp, 4)

    def trailing_stop(self, trade, current_price):
        side = trade["side"]
        trail = settings.TRAILING_STOP_PCT / 100
        if side == "buy":
            return max(trade.get("stop_loss", 0), current_price * (1 - trail))
        return min(trade.get("stop_loss", float("inf")), current_price * (1 + trail))

    def should_close(self, trade, current_price):
        side = trade["side"]
        sl, tp = trade.get("stop_loss", 0), trade.get("take_profit", 0)
        if side == "buy":
            if current_price <= sl: return True, "stop_loss"
            if tp and current_price >= tp: return True, "take_profit"
        else:
            if current_price >= sl: return True, "stop_loss"
            if tp and current_price <= tp: return True, "take_profit"
        # Time exit
        ots = trade.get("open_ts", 0)
        if ots > 0 and (time.time() - ots) > 86400:
            entry = trade.get("entry", current_price)
            mult = 1 if side == "buy" else -1
            if abs((current_price - entry) / entry * 100 * mult) < 1.0:
                return True, "time_exit_flat"
        return False, ""

    # ─── Trade Tracking ──────────────────────────────────────────────────────

    def register_open(self, symbol, trade, market="spot"):
        with self._lock:
            store = self.open_spot if market == "spot" else self.open_futures
            store[symbol] = {**trade, "open_ts": time.time()}
            self._pending_symbols.discard(_normalize_base(symbol))
        self.daily_trades += 1
        logger.info(
            f"[OPEN] {market.upper()} {trade.get('side','').upper()} {symbol} "
            f"entry={trade.get('entry',0):.4f} size={trade.get('size',0):.6f} "
            f"strat={trade.get('strategy','')} conf={trade.get('confidence',0):.0f}%"
        )

    def register_close(self, symbol, pnl_pct, market="spot"):
        with self._lock:
            store = self.open_spot if market == "spot" else self.open_futures
            store.pop(symbol, None)
        if pnl_pct > 0:
            self.wins += 1; self.total_win_pct += pnl_pct
            self._consecutive_wins += 1; self._consecutive_losses = 0
        else:
            self.losses += 1; self.total_loss_pct += abs(pnl_pct)
            self._consecutive_losses += 1; self._consecutive_wins = 0
        self._recent_pnls.append(pnl_pct)
        self._recent_pnls = self._recent_pnls[-50:]
        self.daily_pnl += pnl_pct

    def get_open_trade(self, symbol, market="spot"):
        with self._lock:
            store = self.open_spot if market == "spot" else self.open_futures
            t = store.get(symbol)
            return dict(t) if t else None

    def all_open_trades(self):
        with self._lock:
            trades = []
            for s, t in self.open_spot.items():
                trades.append({**t, "symbol": s, "market": "spot"})
            for s, t in self.open_futures.items():
                trades.append({**t, "symbol": s, "market": "futures"})
        return trades

    def update_trade_sl(self, symbol, market, new_sl):
        with self._lock:
            store = self.open_spot if market == "spot" else self.open_futures
            if symbol in store:
                store[symbol]["stop_loss"] = new_sl

    # ─── Stats ───────────────────────────────────────────────────────────────

    def stats(self):
        total = self.wins + self.losses
        wr = self.wins / total if total else 0
        aw = self.total_win_pct / self.wins if self.wins else 0
        al = self.total_loss_pct / self.losses if self.losses else 0
        pf = self.total_win_pct / self.total_loss_pct if self.total_loss_pct > 0 else 0
        with self._lock:
            ns, nf = len(self.open_spot), len(self.open_futures)
        return {
            "total_trades": total, "wins": self.wins, "losses": self.losses,
            "win_rate": round(wr * 100, 1), "avg_win_pct": round(aw, 2),
            "avg_loss_pct": round(al, 2), "daily_pnl": round(self.daily_pnl, 2),
            "daily_trades": self.daily_trades, "profit_factor": round(pf, 2),
            "consecutive_wins": self._consecutive_wins,
            "consecutive_losses": self._consecutive_losses,
            "in_recovery": self._in_recovery, "open_spot": ns, "open_futures": nf,
        }

    def _half_kelly(self):
        t = self.wins + self.losses
        if t < 100: return 1.0
        wr = self.wins / t
        aw = self.total_win_pct / self.wins if self.wins else 1
        al = self.total_loss_pct / self.losses if self.losses else 1
        if al == 0: return 1.0
        b = aw / al
        k = (b * wr - (1 - wr)) / b
        return max(0.3, min(1.8, k / 2))

    def _maybe_reset_daily(self):
        now = datetime.now(timezone.utc)
        if now.timestamp() - self.daily_reset_ts > 86400:
            self.daily_pnl = 0.0
            self.daily_trades = 0
            self.daily_reset_ts = now.timestamp()

    def _estimated_balance(self):
        return self.peak_balance * (1 + self.daily_pnl / 100)
