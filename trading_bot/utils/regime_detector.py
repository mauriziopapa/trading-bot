"""
Regime Detector — Cambio automatico strategia
═══════════════════════════════════════════════════════════════
Analizza 10 segnali per determinare il regime di mercato:

  🛡️ SAFE    — proteggi capitale (conservativa)
  🚀 NORMAL  — crescita bilanciata (raddoppio)
  💎 AGGRO   — massima esposizione (all-in)

Regole:
  - SAFE scatta con UN SOLO segnale negativo (asimmetrico)
  - AGGRO richiede TUTTI i segnali positivi
  - NORMAL è il default (nessun estremo)
  - Cooldown 30 min tra cambi (no oscillazione)
  - Override manuale blocca auto-switch per 2h
  - Ogni cambio viene loggato + notifica Telegram

Chiamato ogni 5 minuti dallo scheduler in main.py.
"""

import time
from loguru import logger


# ── Configurazioni per ogni regime ───────────────────────────────────────────
REGIME_CONFIGS = {
    "safe": {
        "label": "🛡️ Conservativa",
        "config": {
            "MAX_RISK_PCT":           5.0,
            "DEFAULT_LEVERAGE":       8,
            "MAX_DAILY_LOSS_PCT":     12.0,
            "MAX_DRAWDOWN_PCT":       18,
            "TAKE_PROFIT_RATIO":      2.8,
            "TRAILING_STOP_PCT":      1.2,
            "MIN_CONFIDENCE":         55.0,
            "MAX_POSITIONS_SPOT":     5,
            "MAX_POSITIONS_FUTURES":  4,
            "MARGIN_MODE":            "isolated",
            "MAX_NOTIONAL_PCT":       40,
            "ENABLE_RSI_MACD":        True,
            "ENABLE_BOLLINGER":       True,
            "ENABLE_BREAKOUT":        True,
            "ENABLE_SCALPING":        True,
            "ENABLE_EMERGING":        True,
            "SENTIMENT_BYPASS":       False,
            "FEAR_GREED_LONG_MIN":    10,
            "FEAR_GREED_LONG_MAX":    82,
            "FEAR_GREED_SHORT_MIN":   18,
            "FEAR_GREED_SHORT_MAX":   100,
            "EM_MIN_VOLUME_USD":      800000,
            "EM_MIN_CHANGE_24H":      3.0,
            "EM_MIN_VOLUME_SURGE":    1.5,
            "EM_MAX_MARKET_CAP":      3000000000,
            "EM_MIN_MARKET_CAP":      0,
            "EM_MAX_RESULTS":         10,
            "EM_NEW_LISTING_DAYS":    30,
            "EM_EXCLUDE_SYMBOLS":     "",
            "EMERGING_DIRECT_SCORE":  55.0,
            "EMERGING_MOMENTUM_CHG":  8.0,
            "EMERGING_RISK_MULT":     1.4,
            "EMERGING_MAX_SPREAD":    0.8,
        },
    },
    "normal": {
        "label": "🚀 Raddoppio ×2",
        "config": {
            "MAX_RISK_PCT":           6.0,
            "DEFAULT_LEVERAGE":       10,
            "MAX_DAILY_LOSS_PCT":     15.0,
            "MAX_DRAWDOWN_PCT":       25,
            "TAKE_PROFIT_RATIO":      2.2,
            "TRAILING_STOP_PCT":      0.8,
            "MIN_CONFIDENCE":         60.0,
            "MAX_POSITIONS_SPOT":     6,
            "MAX_POSITIONS_FUTURES":  6,
            "MARGIN_MODE":            "isolated",
            "MAX_NOTIONAL_PCT":       40,
            "ENABLE_RSI_MACD":        True,
            "ENABLE_BOLLINGER":       True,
            "ENABLE_BREAKOUT":        True,
            "ENABLE_SCALPING":        True,
            "ENABLE_EMERGING":        True,
            "SENTIMENT_BYPASS":       False,
            "FEAR_GREED_LONG_MIN":    8,
            "FEAR_GREED_LONG_MAX":    85,
            "FEAR_GREED_SHORT_MIN":   15,
            "FEAR_GREED_SHORT_MAX":   100,
            "EM_MIN_VOLUME_USD":      500000,
            "EM_MIN_CHANGE_24H":      2.0,
            "EM_MIN_VOLUME_SURGE":    1.3,
            "EM_MAX_MARKET_CAP":      2000000000,
            "EM_MIN_MARKET_CAP":      0,
            "EM_MAX_RESULTS":         12,
            "EM_NEW_LISTING_DAYS":    30,
            "EM_EXCLUDE_SYMBOLS":     "",
            "EMERGING_DIRECT_SCORE":  48.0,
            "EMERGING_MOMENTUM_CHG":  6.0,
            "EMERGING_RISK_MULT":     1.6,
            "EMERGING_MAX_SPREAD":    1.0,
        },
    },
    "aggro": {
        "label": "💎 ALL-IN",
        "config": {
            "MAX_RISK_PCT":           8.0,
            "DEFAULT_LEVERAGE":       10,
            "MAX_DAILY_LOSS_PCT":     18.0,
            "MAX_DRAWDOWN_PCT":       30,
            "TAKE_PROFIT_RATIO":      2.2,
            "TRAILING_STOP_PCT":      0.8,
            "MIN_CONFIDENCE":         62.0,
            "MAX_POSITIONS_SPOT":     3,
            "MAX_POSITIONS_FUTURES":  3,
            "MARGIN_MODE":            "isolated",
            "MAX_NOTIONAL_PCT":       80,
            "ENABLE_RSI_MACD":        True,
            "ENABLE_BOLLINGER":       True,
            "ENABLE_BREAKOUT":        True,
            "ENABLE_SCALPING":        True,
            "ENABLE_EMERGING":        True,
            "SENTIMENT_BYPASS":       False,
            "FEAR_GREED_LONG_MIN":    5,
            "FEAR_GREED_LONG_MAX":    88,
            "FEAR_GREED_SHORT_MIN":   12,
            "FEAR_GREED_SHORT_MAX":   100,
            "EM_MIN_VOLUME_USD":      500000,
            "EM_MIN_CHANGE_24H":      2.0,
            "EM_MIN_VOLUME_SURGE":    1.3,
            "EM_MAX_MARKET_CAP":      2000000000,
            "EM_MIN_MARKET_CAP":      0,
            "EM_MAX_RESULTS":         12,
            "EM_NEW_LISTING_DAYS":    30,
            "EM_EXCLUDE_SYMBOLS":     "",
            "EMERGING_DIRECT_SCORE":  45.0,
            "EMERGING_MOMENTUM_CHG":  5.0,
            "EMERGING_RISK_MULT":     1.8,
            "EMERGING_MAX_SPREAD":    1.0,
        },
    },
}


class RegimeDetector:
    """
    Analizza le condizioni di mercato e la performance del bot
    per determinare il regime ottimale e applicarlo automaticamente.
    """

    COOLDOWN_SEC = 1800         # 30 minuti tra cambi regime
    MANUAL_OVERRIDE_SEC = 7200  # 2 ore di blocco dopo click manuale

    def __init__(self):
        self.current_regime: str = "normal"
        self._last_switch_ts: float = 0.0
        self._manual_override_ts: float = 0.0
        self._last_signals: dict = {}

    # ─── Public API ──────────────────────────────────────────────────────────

    def evaluate(self, bot) -> dict:
        """
        Valuta il regime ottimale. Chiamato ogni 5 minuti.
        Ritorna un dict con stato completo per la dashboard.

        Parametri letti dal bot:
          - risk_manager: stats, drawdown, streak
          - sentiment_analyzer: F&G, funding, OI
          - performance: win rate, daily PnL
        """
        now = time.time()

        # ── Raccogli segnali ─────────────────────────────────────────────
        signals = self._collect_signals(bot)
        self._last_signals = signals

        # ── Determina regime ideale ──────────────────────────────────────
        ideal = self._compute_ideal_regime(signals)

        # ── Controlla se possiamo cambiare ───────────────────────────────
        can_switch = True
        reason_blocked = ""

        # Cooldown
        if (now - self._last_switch_ts) < self.COOLDOWN_SEC:
            remaining = int(self.COOLDOWN_SEC - (now - self._last_switch_ts))
            can_switch = False
            reason_blocked = f"Cooldown attivo ({remaining}s)"

        # Override manuale
        if (now - self._manual_override_ts) < self.MANUAL_OVERRIDE_SEC:
            remaining = int(self.MANUAL_OVERRIDE_SEC - (now - self._manual_override_ts))
            can_switch = False
            reason_blocked = f"Override manuale attivo ({remaining // 60}min)"

        # ── Applica se necessario ────────────────────────────────────────
        switched = False
        if ideal != self.current_regime and can_switch:
            old = self.current_regime
            self.current_regime = ideal
            self._last_switch_ts = now
            switched = True
            self._apply_regime(bot, ideal, signals)
            logger.warning(
                f"[REGIME] ⚡ CAMBIO: {REGIME_CONFIGS[old]['label']} → "
                f"{REGIME_CONFIGS[ideal]['label']} | Motivo: {self._explain(signals, ideal)}"
            )

        return {
            "current_regime":    self.current_regime,
            "current_label":     REGIME_CONFIGS[self.current_regime]["label"],
            "ideal_regime":      ideal,
            "ideal_label":       REGIME_CONFIGS[ideal]["label"],
            "switched":          switched,
            "can_switch":        can_switch,
            "reason_blocked":    reason_blocked,
            "signals":           signals,
            "auto_enabled":      True,
            "override_until":    self._manual_override_ts + self.MANUAL_OVERRIDE_SEC
                                 if (now - self._manual_override_ts) < self.MANUAL_OVERRIDE_SEC else 0,
        }

    def set_manual_override(self):
        """Chiamato quando l'utente clicca un pulsante strategia manualmente."""
        self._manual_override_ts = time.time()
        logger.info(f"[REGIME] Override manuale attivato — auto-switch bloccato per 2h")

    def get_state(self) -> dict:
        """Stato corrente per la dashboard (senza ricalcolo)."""
        now = time.time()
        override_remaining = max(0, self.MANUAL_OVERRIDE_SEC - (now - self._manual_override_ts))
        cooldown_remaining = max(0, self.COOLDOWN_SEC - (now - self._last_switch_ts))
        return {
            "current_regime":   self.current_regime,
            "current_label":    REGIME_CONFIGS[self.current_regime]["label"],
            "signals":          self._last_signals,
            "auto_enabled":     True,
            "override_remaining_sec": int(override_remaining),
            "cooldown_remaining_sec": int(cooldown_remaining),
        }

    # ─── Segnali ─────────────────────────────────────────────────────────────

    def _collect_signals(self, bot) -> dict:
        """Raccoglie tutti i segnali per la decisione regime."""
        signals = {
            "drawdown_pct":       0.0,
            "daily_pnl":          0.0,
            "win_rate_recent":    50.0,
            "consecutive_wins":   0,
            "consecutive_losses": 0,
            "fear_greed":         50,
            "funding_avg":        0.0,
            "ls_ratio":           1.0,
            "oi_delta":           0.0,
            "open_positions":     0,
            "total_trades":       0,
        }

        # ── Risk manager stats ───────────────────────────────────────────
        try:
            stats = bot.risk.stats()
            signals["daily_pnl"]          = stats.get("daily_pnl", 0)
            signals["consecutive_wins"]   = stats.get("consecutive_wins", 0)
            signals["consecutive_losses"] = stats.get("consecutive_losses", 0)
            signals["total_trades"]       = stats.get("total_trades", 0)
            signals["open_positions"]     = stats.get("open_spot", 0) + stats.get("open_futures", 0)

            # Win rate su ultimi trade (non lifetime)
            recent = bot.risk._recent_pnls[-20:]
            if len(recent) >= 5:
                signals["win_rate_recent"] = len([p for p in recent if p > 0]) / len(recent) * 100
            else:
                signals["win_rate_recent"] = stats.get("win_rate", 50)

            # Drawdown
            peak = bot.risk.peak_balance
            current = bot.risk._estimated_balance()
            if peak > 0:
                signals["drawdown_pct"] = (peak - current) / peak * 100
        except Exception as e:
            logger.debug(f"[REGIME] Stats error: {e}")

        # ── Sentiment ────────────────────────────────────────────────────
        try:
            sent = bot._sentiment.get_sentiment()
            signals["fear_greed"]   = sent.get("fear_greed", 50)
            signals["funding_avg"]  = (sent.get("funding_btc", 0) + sent.get("funding_eth", 0)) / 2
            signals["ls_ratio"]     = sent.get("ls_ratio_btc", 1.0)
            signals["oi_delta"]     = sent.get("oi_change_pct", 0)
        except Exception as e:
            logger.debug(f"[REGIME] Sentiment error: {e}")

        return signals

    # ─── Regime computation ──────────────────────────────────────────────────

    def _compute_ideal_regime(self, s: dict) -> str:
        """
        Logica asimmetrica:
          SAFE: basta UN segnale critico (protezione prioritaria)
          AGGRO: servono TUTTI i segnali positivi (certezza prima di spingere)
          NORMAL: default
        """

        # ══════════ SAFE — UN SOLO trigger basta ══════════════════════════
        safe_reasons = []

        if s["drawdown_pct"] > 12:
            safe_reasons.append(f"DD {s['drawdown_pct']:.1f}% > 12%")
        if s["daily_pnl"] < -8:
            safe_reasons.append(f"Daily PnL {s['daily_pnl']:.1f}% < -8%")
        if s["win_rate_recent"] < 35 and s["total_trades"] >= 10:
            safe_reasons.append(f"WR {s['win_rate_recent']:.0f}% < 35%")
        if s["consecutive_losses"] >= 4:
            safe_reasons.append(f"Losing streak ×{s['consecutive_losses']}")
        if s["fear_greed"] < 10:
            safe_reasons.append(f"F&G={s['fear_greed']} Extreme Fear")
        if s["fear_greed"] > 90:
            safe_reasons.append(f"F&G={s['fear_greed']} Extreme Greed")
        if s["funding_avg"] > 0.12:
            safe_reasons.append(f"Funding {s['funding_avg']:.3f}% overheated")
        if s["ls_ratio"] > 2.5:
            safe_reasons.append(f"L/S {s['ls_ratio']:.2f} extreme long crowding")

        if safe_reasons:
            logger.debug(f"[REGIME] SAFE triggers: {', '.join(safe_reasons)}")
            return "safe"

        # ══════════ AGGRO — TUTTI i criteri devono essere soddisfatti ═════
        aggro_checks = {
            "DD basso":        s["drawdown_pct"] < 3,
            "WR alto":         s["win_rate_recent"] > 58 or s["total_trades"] < 10,
            "Win streak":      s["consecutive_wins"] >= 3 or s["total_trades"] < 5,
            "Daily PnL +":     s["daily_pnl"] > 3,
            "F&G operativo":   20 <= s["fear_greed"] <= 75,
            "Funding ok":      abs(s["funding_avg"]) < 0.08,
            "L/S bilanciato":  0.5 < s["ls_ratio"] < 2.0,
            "No loss streak":  s["consecutive_losses"] < 2,
        }

        all_pass = all(aggro_checks.values())
        if all_pass:
            logger.debug(f"[REGIME] AGGRO — tutti i check passati")
            return "aggro"

        failed = [k for k, v in aggro_checks.items() if not v]
        if len(failed) <= 2:
            logger.debug(f"[REGIME] NORMAL (quasi AGGRO, mancano: {', '.join(failed)})")

        # ══════════ NORMAL — default ══════════════════════════════════════
        return "normal"

    # ─── Apply ───────────────────────────────────────────────────────────────

    def _apply_regime(self, bot, regime: str, signals: dict):
        """Scrive i parametri del regime sul DB via settings.set_many()."""
        try:
            from trading_bot.config import settings

            cfg = REGIME_CONFIGS[regime]["config"]
            changed = settings.set_many(cfg)

            label = REGIME_CONFIGS[regime]["label"]
            logger.info(
                f"[REGIME] {label} applicato — {len(changed)} parametri modificati"
            )

            # Notifica Telegram
            try:
                bot.notifier.send(
                    f"⚡ <b>REGIME AUTO-SWITCH</b>\n"
                    f"Nuova strategia: <b>{label}</b>\n"
                    f"Motivo: {self._explain(signals, regime)}\n"
                    f"Parametri modificati: {len(changed)}\n"
                    f"F&G={signals.get('fear_greed', '?')} | "
                    f"DD={signals.get('drawdown_pct', 0):.1f}% | "
                    f"WR={signals.get('win_rate_recent', 0):.0f}% | "
                    f"Daily={signals.get('daily_pnl', 0):+.1f}%"
                )
            except Exception:
                pass

        except Exception as e:
            logger.error(f"[REGIME] Errore applicazione {regime}: {e}")

    def _explain(self, s: dict, regime: str) -> str:
        """Genera una spiegazione leggibile del cambio regime."""
        if regime == "safe":
            reasons = []
            if s["drawdown_pct"] > 12:     reasons.append(f"DD={s['drawdown_pct']:.1f}%")
            if s["daily_pnl"] < -8:        reasons.append(f"daily={s['daily_pnl']:.1f}%")
            if s["win_rate_recent"] < 35:   reasons.append(f"WR={s['win_rate_recent']:.0f}%")
            if s["consecutive_losses"] >= 4: reasons.append(f"streak L{s['consecutive_losses']}")
            if s["fear_greed"] < 10:        reasons.append(f"F&G={s['fear_greed']}")
            if s["fear_greed"] > 90:        reasons.append(f"F&G={s['fear_greed']}")
            return "Protezione: " + ", ".join(reasons) if reasons else "Segnali negativi"
        elif regime == "aggro":
            return (
                f"Condizioni ottimali: DD={s['drawdown_pct']:.1f}% "
                f"WR={s['win_rate_recent']:.0f}% W{s['consecutive_wins']} "
                f"daily={s['daily_pnl']:+.1f}% F&G={s['fear_greed']}"
            )
        return "Condizioni normali"
