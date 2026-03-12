"""
Risk Manager Aggressivo con Controllo Adattivo
- Sizing basato su ATR (volatilità)
- Circuit breaker giornaliero
- Trailing stop dinamico
- Drawdown tracker
- Kelly Criterion parziale per sizing ottimale
"""

import time
from datetime import datetime, timezone
from loguru import logger
from trading_bot.config import settings


class RiskManager:

    def __init__(self):
        # Stato posizioni
        self.open_spot: dict[str, dict]    = {}   # symbol -> trade info
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

    # ─── Guard checks ────────────────────────────────────────────────────────

    def can_trade(self, market: str = "spot") -> tuple[bool, str]:
        """
        Ritorna (True, "") se il trade è permesso,
        (False, "motivo") altrimenti.
        """
        self._maybe_reset_daily()

        # Circuit breaker giornaliero
        if self.daily_pnl <= -(settings.MAX_DAILY_LOSS_PCT):
            return False, f"Circuit breaker: loss giornaliero {self.daily_pnl:.1f}%"

        # Max drawdown
        if self.peak_balance > 0:
            current_dd = (self.peak_balance - self._estimated_balance()) / self.peak_balance * 100
            if current_dd >= settings.MAX_DRAWDOWN_PCT:
                return False, f"Max drawdown raggiunto: {current_dd:.1f}%"

        # Max posizioni
        if market == "spot":
            if len(self.open_spot) >= settings.MAX_POSITIONS_SPOT:
                return False, f"Max posizioni spot raggiunte ({settings.MAX_POSITIONS_SPOT})"
        else:
            if len(self.open_futures) >= settings.MAX_POSITIONS_FUTURES:
                return False, f"Max posizioni futures raggiunte ({settings.MAX_POSITIONS_FUTURES})"

        return True, ""

    def can_trade_symbol(self, symbol: str, market: str = "spot") -> tuple[bool, str]:
        """Controlla se è già presente una posizione su questo simbolo."""
        store = self.open_spot if market == "spot" else self.open_futures
        if symbol in store:
            return False, f"Posizione già aperta su {symbol} ({market})"
        return self.can_trade(market)

    # ─── Position Sizing ─────────────────────────────────────────────────────

    def position_size(self, balance: float, entry: float, stop_loss: float,
                      atr: float = None, market: str = "spot") -> float:
        """
        Calcola la size ottimale del trade.
        - Base: rischio fisso (MAX_RISK_PCT % del balance)
        - Modifier ATR: riduce size in alta volatilità
        - Modifier Kelly: aumenta/riduce in base a win rate storico
        """
        risk_pct = settings.MAX_RISK_PCT / 100

        # Modifier Kelly parziale (metà Kelly per sicurezza)
        kelly_mod = self._half_kelly()
        risk_pct  = risk_pct * kelly_mod

        # Modifier volatilità ATR
        if atr is not None and entry > 0:
            atr_pct = atr / entry
            if atr_pct > 0.04:       # ATR > 4% → riduci size
                risk_pct *= 0.6
            elif atr_pct < 0.015:    # ATR < 1.5% → puoi aumentare leggermente
                risk_pct *= 1.2

        risk_amount  = balance * risk_pct
        risk_per_unit = abs(entry - stop_loss)

        if risk_per_unit <= 0:
            return 0.0

        size = risk_amount / risk_per_unit

        # Per futures: dimensiona in base alla leva
        if market == "futures":
            size = size * settings.DEFAULT_LEVERAGE / entry
        else:
            size = size / entry    # converti USDT → quantità asset

        # ── A) Cap notionale: evita 43012 / 40762 ───────────────────────────
        # SL stretto → risk_per_unit piccolo → size esplode oltre il balance
        # Regola: nessun trade può costare più del 35% del balance disponibile
        MAX_NOTIONAL_PCT = 0.35
        MIN_NOTIONAL_USDT = 6.0   # Bitget rifiuta ordini < ~5 USDT notionale

        actual_cost = size * entry   # per futures: costo = size * entry (senza leva, è il margine * leva)
        max_notional = balance * MAX_NOTIONAL_PCT

        if actual_cost > max_notional:
            size = max_notional / entry
            logger.info(
                f"[CAP] {market} {entry:.4f} costo_orig={actual_cost:.2f} USDT > max={max_notional:.2f} "
                f"→ size ridotta a {size:.6f}"
            )
            actual_cost = size * entry

        # ── B) Floor notionale: evita "amount must be greater than minimum" ──
        if actual_cost < MIN_NOTIONAL_USDT:
            logger.info(
                f"[SKIP-NOTIONAL] {market} notionale {actual_cost:.2f} USDT < min {MIN_NOTIONAL_USDT} USDT"
            )
            return 0.0   # il chiamante skippa se size == 0

        logger.info(
            f"[SIZING] {market} bal={balance:.0f} risk={risk_pct*100:.1f}% "
            f"SL_dist={risk_per_unit:.4f} → size={size:.6f} cost={actual_cost:.2f} USDT"
        )
        return round(size, 6)

    # ─── Stop Loss & Take Profit ─────────────────────────────────────────────

    def calculate_stops(self, entry: float, side: str, atr: float,
                        sl_atr_mult: float = 1.8) -> tuple[float, float]:
        """
        Calcola SL e TP basati su ATR.
        SL  = entry ± (ATR * sl_atr_mult)
        TP  = entry ± (ATR * sl_atr_mult * TAKE_PROFIT_RATIO)
        """
        sl_distance = atr * sl_atr_mult
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
        Sposta il SL solo in direzione favorevole.
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
        """Controlla se la posizione deve essere chiusa."""
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

        return False, ""

    # ─── Trade Tracking ──────────────────────────────────────────────────────

    def register_open(self, symbol: str, trade: dict, market: str = "spot"):
        store = self.open_spot if market == "spot" else self.open_futures
        store[symbol] = {**trade, "open_ts": time.time()}
        self.daily_trades += 1
        logger.info(
            "[OPEN] %s %s %s | entry=%.4f size=%.6f | SL=%.4f TP=%.4f | strat=%s conf=%.0f%% | pos=%d" % (
                market.upper(), trade.get("side", "").upper(), symbol,
                trade.get("entry", 0), trade.get("size", 0),
                trade.get("stop_loss", 0), trade.get("take_profit", 0),
                trade.get("strategy", ""), trade.get("confidence", 0),
                len(store),
            )
        )

    def register_close(self, symbol: str, pnl_pct: float, market: str = "spot"):
        store = self.open_spot if market == "spot" else self.open_futures
        store.pop(symbol, None)

        # Aggiorna statistiche per Kelly
        if pnl_pct > 0:
            self.wins += 1
            self.total_win_pct += pnl_pct
        else:
            self.losses += 1
            self.total_loss_pct += abs(pnl_pct)

        # Aggiorna PnL giornaliero
        self.daily_pnl += pnl_pct
        logger.info(
            f"[CLOSE] {market.upper()} {symbol} pnl={pnl_pct:+.2f}% | "
            f"daily_pnl={self.daily_pnl:+.2f}% | W={self.wins} L={self.losses}"
        )

    def get_open_trade(self, symbol: str, market: str = "spot") -> dict | None:
        store = self.open_spot if market == "spot" else self.open_futures
        return store.get(symbol)

    def all_open_trades(self) -> list[dict]:
        trades = []
        for s, t in self.open_spot.items():
            trades.append({**t, "symbol": s, "market": "spot"})
        for s, t in self.open_futures.items():
            trades.append({**t, "symbol": s, "market": "futures"})
        return trades

    # ─── Stats & Kelly ───────────────────────────────────────────────────────

    def stats(self) -> dict:
        total = self.wins + self.losses
        win_rate = self.wins / total if total else 0
        avg_win  = self.total_win_pct / self.wins if self.wins else 0
        avg_loss = self.total_loss_pct / self.losses if self.losses else 0
        return {
            "total_trades": total,
            "wins": self.wins,
            "losses": self.losses,
            "win_rate": round(win_rate * 100, 1),
            "avg_win_pct": round(avg_win, 2),
            "avg_loss_pct": round(avg_loss, 2),
            "daily_pnl": round(self.daily_pnl, 2),
            "daily_trades": self.daily_trades,
        }

    def _half_kelly(self) -> float:
        """Mezzo Kelly criterion per position sizing adattivo."""
        total = self.wins + self.losses
        if total < 20:
            return 1.0     # non abbastanza dati, usa rischio pieno

        win_rate = self.wins / total
        avg_win  = self.total_win_pct / self.wins if self.wins else 1
        avg_loss = self.total_loss_pct / self.losses if self.losses else 1

        if avg_loss == 0:
            return 1.0

        b = avg_win / avg_loss      # win/loss ratio
        kelly = (b * win_rate - (1 - win_rate)) / b
        half_kelly = max(0.3, min(1.5, kelly / 2))   # clamp 0.3–1.5
        return half_kelly

    def _maybe_reset_daily(self):
        now = datetime.now(timezone.utc)
        if now.timestamp() - self.daily_reset_ts > 86400:
            self.daily_pnl    = 0.0
            self.daily_trades = 0
            self.daily_reset_ts = now.timestamp()

    def _estimated_balance(self) -> float:
        # Stima semplificata — viene aggiornata dal bot principale
        return self.peak_balance * (1 + self.daily_pnl / 100)
