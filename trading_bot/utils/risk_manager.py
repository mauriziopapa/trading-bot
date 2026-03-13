"""
Risk Manager v3 — Aggressivo con Controllo Adattivo
═══════════════════════════════════════════════════════════════
FIX CRITICI:
  ✓ Thread-safe trailing stop (aggiorna store PRIMA di should_close)
  ✓ Futures notional cap corretto (margine, non nozionale pieno)
  ✓ Kelly Criterion con minimo 100 trade (non 20)
  ✓ Floor notionale dinamico per exchange

OTTIMIZZAZIONI AGGRESSIVE:
  ✓ Correlation-aware sizing (riduce size su asset correlati)
  ✓ Winning streak multiplier (aumenta size dopo serie positiva)
  ✓ Volatility regime detection (ATR regime per sizing adattivo)
  ✓ Drawdown recovery mode (size ridotta dopo drawdown pesante)
  ✓ Spread buffer nel calcolo SL
"""

import time
import threading
from datetime import datetime, timezone
from loguru import logger
from trading_bot.config import settings


# ── Correlation clusters — asset fortemente correlati ──────────────────────
# ρ > 0.80 su timeframe 15m: se apri LONG su BTC e ETH,
# il rischio reale è ~1.7x, non 2x indipendente.
_CORRELATION_CLUSTERS = {
    "LAYER1": {"BTC", "ETH", "SOL", "AVAX", "DOT", "NEAR", "ATOM", "ADA"},
    "DEFI":   {"AAVE", "UNI", "SUSHI", "CRV", "MKR", "COMP", "SNX"},
    "MEME":   {"DOGE", "SHIB", "PEPE", "FLOKI", "BONK", "WIF"},
    "L2":     {"MATIC", "ARB", "OP", "STRK", "ZK", "MANTA"},
}


def _get_cluster(symbol: str) -> str | None:
    """Ritorna il cluster di correlazione per un simbolo."""
    base = symbol.split("/")[0].split(":")[0].upper()
    for cluster_name, members in _CORRELATION_CLUSTERS.items():
        if base in members:
            return cluster_name
    return None


class RiskManager:

    def __init__(self):
        # Lock per thread safety su open positions
        self._lock = threading.Lock()

        # Stato posizioni
        self.open_spot: dict[str, dict]    = {}
        self.open_futures: dict[str, dict] = {}

        # Equity tracking
        self.session_start_balance: float = 0.0
        self.peak_balance: float          = 0.0
        self.daily_pnl: float             = 0.0
        self.daily_reset_ts: float        = 0.0
        self.daily_trades: int            = 0

        # Statistiche win/loss per Kelly
        self.wins: int   = 0
        self.losses: int = 0
        self.total_win_pct: float  = 0.0
        self.total_loss_pct: float = 0.0

        # ── NUOVI: streak tracking per sizing aggressivo ──────────────────
        self._consecutive_wins: int   = 0
        self._consecutive_losses: int = 0
        self._recent_pnls: list[float] = []   # ultimi 50 PnL %

        # ── NUOVO: drawdown recovery state ────────────────────────────────
        self._in_recovery: bool  = False
        self._recovery_ts: float = 0.0

    # ─── Guard checks ────────────────────────────────────────────────────────

    def can_trade(self, market: str = "spot") -> tuple[bool, str]:
        self._maybe_reset_daily()

        # Circuit breaker giornaliero
        if self.daily_pnl <= -(settings.MAX_DAILY_LOSS_PCT):
            return False, f"Circuit breaker: loss giornaliero {self.daily_pnl:.1f}%"

        # Max drawdown
        if self.peak_balance > 0:
            current_dd = (self.peak_balance - self._estimated_balance()) / self.peak_balance * 100
            if current_dd >= settings.MAX_DRAWDOWN_PCT:
                return False, f"Max drawdown raggiunto: {current_dd:.1f}%"

            # ── NUOVO: Recovery mode — dopo drawdown > 60% del max ────────
            recovery_thresh = settings.MAX_DRAWDOWN_PCT * 0.6
            if current_dd >= recovery_thresh and not self._in_recovery:
                self._in_recovery = True
                self._recovery_ts = time.time()
                logger.warning(
                    f"[RISK] RECOVERY MODE attivato — DD={current_dd:.1f}% "
                    f"(soglia {recovery_thresh:.1f}%) — sizing ridotta 50%"
                )
            elif current_dd < recovery_thresh * 0.5 and self._in_recovery:
                self._in_recovery = False
                logger.info("[RISK] Recovery mode disattivato — drawdown rientrato")

        # Max posizioni
        with self._lock:
            if market == "spot":
                if len(self.open_spot) >= settings.MAX_POSITIONS_SPOT:
                    return False, f"Max posizioni spot raggiunte ({settings.MAX_POSITIONS_SPOT})"
            else:
                if len(self.open_futures) >= settings.MAX_POSITIONS_FUTURES:
                    return False, f"Max posizioni futures raggiunte ({settings.MAX_POSITIONS_FUTURES})"

        return True, ""

    def can_trade_symbol(self, symbol: str, market: str = "spot") -> tuple[bool, str]:
        with self._lock:
            store = self.open_spot if market == "spot" else self.open_futures
            if symbol in store:
                return False, f"Posizione già aperta su {symbol} ({market})"
        return self.can_trade(market)

    # ─── Position Sizing v3 ──────────────────────────────────────────────────

    def position_size(self, balance: float, entry: float, stop_loss: float,
                      atr: float = None, market: str = "spot",
                      risk_multiplier: float = 1.0,
                      symbol: str = "") -> float:
        """
        Sizing v3 — Aggressivo con controlli adattivi.
        - Base: rischio fisso (MAX_RISK_PCT % del balance)
        - Modifier ATR: riduce size in alta volatilità
        - Modifier Kelly: aumenta/riduce in base a win rate storico
        - NUOVO: Correlation discount su asset dello stesso cluster
        - NUOVO: Winning streak multiplier (+15% dopo 3+ win consecutive)
        - NUOVO: Recovery mode (-50% sizing dopo drawdown pesante)
        - FIX: Futures cap sul MARGINE, non sul nozionale pieno
        """
        risk_pct = settings.MAX_RISK_PCT / 100

        # ── Kelly parziale ────────────────────────────────────────────────
        kelly_mod = self._half_kelly()
        risk_pct  = risk_pct * kelly_mod * risk_multiplier

        # ── NUOVO: Winning streak boost (aggressivo) ─────────────────────
        # Dopo 3+ win consecutive → +15% size, dopo 5+ → +25%
        # Logica: "hot hand" empirico — quando il mercato ti dà ragione,
        # massimizza l'esposizione prima che il regime cambi.
        if self._consecutive_wins >= 5:
            streak_mult = 1.25
        elif self._consecutive_wins >= 3:
            streak_mult = 1.15
        elif self._consecutive_losses >= 3:
            streak_mult = 0.70   # cool down dopo 3 loss
        else:
            streak_mult = 1.0
        risk_pct *= streak_mult

        # ── NUOVO: Recovery mode ─────────────────────────────────────────
        if self._in_recovery:
            risk_pct *= 0.50
            logger.debug(f"[SIZING] Recovery mode → risk dimezzato a {risk_pct*100:.2f}%")

        # ── NUOVO: Correlation discount ──────────────────────────────────
        # Se ho già posizioni aperte sullo stesso cluster, riduco il sizing
        # perché il rischio reale è correlato.
        corr_discount = self._correlation_discount(symbol, market)
        risk_pct *= corr_discount

        # Cap assoluto: 10% per trade (era 8%, più aggressivo)
        risk_pct = min(risk_pct, 0.10)

        # ── Modifier volatilità ATR (regime-aware) ────────────────────────
        if atr is not None and entry > 0:
            atr_pct = atr / entry
            if atr_pct > 0.06:       # ATR > 6% → alta vol, riduci forte
                risk_pct *= 0.5
            elif atr_pct > 0.04:     # ATR > 4% → riduci
                risk_pct *= 0.7
            elif atr_pct < 0.01:     # ATR < 1% → bassa vol, boost leggero
                risk_pct *= 1.3
            elif atr_pct < 0.015:
                risk_pct *= 1.15

        risk_amount  = balance * risk_pct
        risk_per_unit = abs(entry - stop_loss)

        if risk_per_unit <= 0:
            return 0.0

        size = risk_amount / risk_per_unit

        # Per futures: dimensiona in base alla leva
        leverage = settings.DEFAULT_LEVERAGE
        if market == "futures":
            size = size * leverage / entry
        else:
            size = size / entry

        # ── FIX CRITICO: Cap notionale corretto per futures ──────────────
        # Prima: actual_cost = size * entry (nozionale pieno) → troppo conservativo
        # Ora: per futures il costo reale è il MARGINE (nozionale / leva)
        MAX_NOTIONAL_PCT = 0.40   # aumentato da 0.35 (più aggressivo)
        MIN_NOTIONAL_USDT = 6.0

        if market == "futures":
            notional     = size * entry
            margin_cost  = notional / leverage
            max_margin   = balance * MAX_NOTIONAL_PCT
            if margin_cost > max_margin:
                size = max_margin * leverage / entry
                logger.info(
                    f"[CAP-FUT] margine {margin_cost:.2f} > max {max_margin:.2f} "
                    f"→ size ridotta a {size:.6f}"
                )
            actual_cost = size * entry / leverage
        else:
            actual_cost  = size * entry
            max_notional = balance * MAX_NOTIONAL_PCT
            if actual_cost > max_notional:
                size = max_notional / entry
                actual_cost = size * entry

        # Floor notionale
        min_check = size * entry if market == "spot" else size * entry / leverage
        if min_check < MIN_NOTIONAL_USDT:
            logger.info(f"[SKIP-NOTIONAL] costo={min_check:.2f} < min {MIN_NOTIONAL_USDT}")
            return 0.0

        logger.info(
            f"[SIZING] {market} {symbol} bal={balance:.0f} risk={risk_pct*100:.2f}% "
            f"kelly={kelly_mod:.2f} streak={streak_mult:.2f} corr={corr_discount:.2f} "
            f"SL_dist={risk_per_unit:.4f} → size={size:.6f} "
            f"cost={'margin' if market=='futures' else 'notional'}="
            f"{actual_cost:.2f} USDT"
        )
        return round(size, 6)

    # ─── NUOVA: Correlation discount ─────────────────────────────────────────

    def _correlation_discount(self, symbol: str, market: str) -> float:
        """
        Riduce sizing se ci sono già posizioni aperte su asset correlati.
        - 1 posizione nello stesso cluster → 0.75× (sconto 25%)
        - 2+ posizioni nello stesso cluster → 0.55× (sconto 45%)
        - Nessuna correlazione → 1.0× (nessuno sconto)
        """
        cluster = _get_cluster(symbol)
        if cluster is None:
            return 1.0

        count = 0
        with self._lock:
            for s in list(self.open_spot.keys()) + list(self.open_futures.keys()):
                if _get_cluster(s) == cluster:
                    count += 1

        if count >= 2:
            return 0.55
        elif count == 1:
            return 0.75
        return 1.0

    # ─── Stop Loss & Take Profit ─────────────────────────────────────────────

    def calculate_stops(self, entry: float, side: str, atr: float,
                        sl_atr_mult: float = 1.8,
                        spread_buffer: float = 0.0) -> tuple[float, float]:
        """
        FIX: aggiunto spread_buffer per compensare slippage su altcoin.
        """
        sl_distance = atr * sl_atr_mult + spread_buffer
        tp_distance = sl_distance * settings.TAKE_PROFIT_RATIO

        if side == "buy":
            stop_loss   = entry - sl_distance
            take_profit = entry + tp_distance
        else:
            stop_loss   = entry + sl_distance
            take_profit = entry - tp_distance

        return round(stop_loss, 4), round(take_profit, 4)

    def trailing_stop(self, trade: dict, current_price: float) -> float:
        """
        Aggiorna il trailing stop e ritorna il nuovo valore.
        Thread-safe: il chiamante deve aggiornare il store PRIMA di should_close.
        """
        side = trade["side"]
        trail_pct = settings.TRAILING_STOP_PCT / 100

        if side == "buy":
            new_sl = current_price * (1 - trail_pct)
            return max(trade.get("stop_loss", 0), new_sl)
        else:
            new_sl = current_price * (1 + trail_pct)
            return min(trade.get("stop_loss", float("inf")), new_sl)

    def should_close(self, trade: dict, current_price: float) -> tuple[bool, str]:
        side = trade["side"]
        sl   = trade.get("stop_loss", 0)
        tp   = trade.get("take_profit", 0)

        if side == "buy":
            if current_price <= sl:
                return True, "stop_loss"
            if tp and current_price >= tp:
                return True, "take_profit"
        else:
            if current_price >= sl:
                return True, "stop_loss"
            if tp and current_price <= tp:
                return True, "take_profit"

        # ── NUOVO: Time-based exit — chiudi dopo 24h senza TP/SL ─────────
        # Su scalping e swing, se dopo 24h non ha fatto nulla,
        # probabilmente il setup è invalidato.
        open_ts = trade.get("open_ts", 0)
        if open_ts > 0 and (time.time() - open_ts) > 86400:
            entry = trade.get("entry", current_price)
            side_mult = 1 if side == "buy" else -1
            pnl_pct = (current_price - entry) / entry * 100 * side_mult
            if abs(pnl_pct) < 1.0:  # quasi flat dopo 24h
                return True, "time_exit_flat"

        return False, ""

    # ─── Trade Tracking ──────────────────────────────────────────────────────

    def register_open(self, symbol: str, trade: dict, market: str = "spot"):
        with self._lock:
            store = self.open_spot if market == "spot" else self.open_futures
            store[symbol] = {**trade, "open_ts": time.time()}
            pos_count = len(store)
        self.daily_trades += 1
        logger.info(
            "[OPEN] %s %s %s | entry=%.4f size=%.6f | SL=%.4f TP=%.4f | "
            "strat=%s conf=%.0f%% | pos=%d | streak=W%d/L%d" % (
                market.upper(), trade.get("side", "").upper(), symbol,
                trade.get("entry", 0), trade.get("size", 0),
                trade.get("stop_loss", 0), trade.get("take_profit", 0),
                trade.get("strategy", ""), trade.get("confidence", 0),
                pos_count,
                self._consecutive_wins, self._consecutive_losses,
            )
        )

    def register_close(self, symbol: str, pnl_pct: float, market: str = "spot"):
        with self._lock:
            store = self.open_spot if market == "spot" else self.open_futures
            store.pop(symbol, None)

        # Aggiorna statistiche per Kelly
        if pnl_pct > 0:
            self.wins += 1
            self.total_win_pct += pnl_pct
            self._consecutive_wins += 1
            self._consecutive_losses = 0
        else:
            self.losses += 1
            self.total_loss_pct += abs(pnl_pct)
            self._consecutive_losses += 1
            self._consecutive_wins = 0

        # Recent PnLs per analisi regime
        self._recent_pnls.append(pnl_pct)
        self._recent_pnls = self._recent_pnls[-50:]

        self.daily_pnl += pnl_pct
        logger.info(
            f"[CLOSE] {market.upper()} {symbol} pnl={pnl_pct:+.2f}% | "
            f"daily_pnl={self.daily_pnl:+.2f}% | W={self.wins} L={self.losses} | "
            f"streak={'W' if pnl_pct > 0 else 'L'}{max(self._consecutive_wins, self._consecutive_losses)}"
        )

    def get_open_trade(self, symbol: str, market: str = "spot") -> dict | None:
        with self._lock:
            store = self.open_spot if market == "spot" else self.open_futures
            t = store.get(symbol)
            return dict(t) if t else None  # ritorna copia

    def all_open_trades(self) -> list[dict]:
        """Ritorna COPIE di tutti i trade aperti — thread safe."""
        with self._lock:
            trades = []
            for s, t in self.open_spot.items():
                trades.append({**t, "symbol": s, "market": "spot"})
            for s, t in self.open_futures.items():
                trades.append({**t, "symbol": s, "market": "futures"})
        return trades

    def update_trade_sl(self, symbol: str, market: str, new_sl: float):
        """FIX CRITICO: aggiorna SL direttamente nel store in modo atomico."""
        with self._lock:
            store = self.open_spot if market == "spot" else self.open_futures
            if symbol in store:
                store[symbol]["stop_loss"] = new_sl

    # ─── Stats & Kelly ───────────────────────────────────────────────────────

    def stats(self) -> dict:
        total = self.wins + self.losses
        win_rate = self.wins / total if total else 0
        avg_win  = self.total_win_pct / self.wins if self.wins else 0
        avg_loss = self.total_loss_pct / self.losses if self.losses else 0

        # ── NUOVO: Profit Factor e Sharpe approssimato ────────────────────
        gross_profit = self.total_win_pct
        gross_loss   = self.total_loss_pct
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else 0

        return {
            "total_trades":    total,
            "wins":            self.wins,
            "losses":          self.losses,
            "win_rate":        round(win_rate * 100, 1),
            "avg_win_pct":     round(avg_win, 2),
            "avg_loss_pct":    round(avg_loss, 2),
            "daily_pnl":       round(self.daily_pnl, 2),
            "daily_trades":    self.daily_trades,
            "profit_factor":   round(profit_factor, 2),
            "consecutive_wins": self._consecutive_wins,
            "consecutive_losses": self._consecutive_losses,
            "in_recovery":     self._in_recovery,
        }

    def _half_kelly(self) -> float:
        """
        FIX: minimo 100 trade per Kelly (era 20).
        Con < 100 trade la varianza del win rate è troppo alta.
        """
        total = self.wins + self.losses
        if total < 100:
            return 1.0

        win_rate = self.wins / total
        avg_win  = self.total_win_pct / self.wins if self.wins else 1
        avg_loss = self.total_loss_pct / self.losses if self.losses else 1

        if avg_loss == 0:
            return 1.0

        b = avg_win / avg_loss
        kelly = (b * win_rate - (1 - win_rate)) / b
        half_kelly = max(0.3, min(1.8, kelly / 2))   # clamp 0.3–1.8 (era 1.5, più aggressivo)
        return half_kelly

    def _maybe_reset_daily(self):
        now = datetime.now(timezone.utc)
        if now.timestamp() - self.daily_reset_ts > 86400:
            self.daily_pnl    = 0.0
            self.daily_trades = 0
            self.daily_reset_ts = now.timestamp()

    def _estimated_balance(self) -> float:
        return self.peak_balance * (1 + self.daily_pnl / 100)
