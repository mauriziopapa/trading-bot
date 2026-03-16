"""
Regime Detector — Cambio automatico strategia
Versione ottimizzata per maggiore frequenza di trading
"""

import time
from loguru import logger


REGIME_CONFIGS = {

    "safe": {
        "label": "🎯 Sniper",
        "config": {
            "MAX_RISK_PCT": 10,
            "DEFAULT_LEVERAGE": 12,
            "MAX_DAILY_LOSS_PCT": 15,
            "MAX_DRAWDOWN_PCT": 30,
            "MIN_CONFIDENCE": 60,
            "MAX_POSITIONS_SPOT": 4,
            "MAX_POSITIONS_FUTURES": 4,
            "EM_MIN_VOLUME_USD": 120000,
            "EM_MIN_CHANGE_24H": 1.2,
        },
    },

    "normal": {
        "label": "⚡ Blitz",
        "config": {
            "MAX_RISK_PCT": 12,
            "DEFAULT_LEVERAGE": 15,
            "MAX_DAILY_LOSS_PCT": 25,
            "MAX_DRAWDOWN_PCT": 40,
            "MIN_CONFIDENCE": 46,
            "MAX_POSITIONS_SPOT": 12,
            "MAX_POSITIONS_FUTURES": 12,
            "EM_MIN_VOLUME_USD": 50000,
            "EM_MIN_CHANGE_24H": 0.3,
        },
    },

    "aggro": {
        "label": "🔥 YOLO",
        "config": {
            "MAX_RISK_PCT": 15,
            "DEFAULT_LEVERAGE": 20,
            "MAX_DAILY_LOSS_PCT": 35,
            "MAX_DRAWDOWN_PCT": 50,
            "MIN_CONFIDENCE": 44,
            "MAX_POSITIONS_SPOT": 14,
            "MAX_POSITIONS_FUTURES": 14,
            "EM_MIN_VOLUME_USD": 30000,
            "EM_MIN_CHANGE_24H": 0.2,
        },
    },
}


class RegimeDetector:

    COOLDOWN_SEC = 900
    MANUAL_OVERRIDE_SEC = 7200

    def __init__(self):

        self.current_regime = "normal"
        self._last_switch_ts = 0
        self._manual_override_ts = 0
        self._last_signals = {}

    # ============================================================
    # PUBLIC
    # ============================================================

    def evaluate(self, bot):

        now = time.time()

        signals = self._collect_signals(bot)
        self._last_signals = signals

        ideal = self._compute_ideal_regime(signals)

        can_switch = True
        reason_blocked = ""

        if (now - self._last_switch_ts) < self.COOLDOWN_SEC:
            can_switch = False
            reason_blocked = "cooldown"

        if (now - self._manual_override_ts) < self.MANUAL_OVERRIDE_SEC:
            can_switch = False
            reason_blocked = "manual override"

        switched = False

        if ideal != self.current_regime and can_switch:

            old = self.current_regime
            self.current_regime = ideal
            self._last_switch_ts = now
            switched = True

            self._apply_regime(bot, ideal, signals)

            logger.warning(
                f"[REGIME] ⚡ {REGIME_CONFIGS[old]['label']} → {REGIME_CONFIGS[ideal]['label']}"
            )

        return {
            "current_regime": self.current_regime,
            "ideal_regime": ideal,
            "switched": switched,
            "signals": signals,
            "can_switch": can_switch,
            "reason_blocked": reason_blocked
        }

    def set_manual_override(self):
        self._manual_override_ts = time.time()

    # ============================================================
    # SIGNAL COLLECTION
    # ============================================================

    def _collect_signals(self, bot):

        signals = {
            "drawdown_pct": 0,
            "daily_pnl": 0,
            "win_rate_recent": 50,
            "consecutive_wins": 0,
            "consecutive_losses": 0,
            "fear_greed": 50,
            "funding_avg": 0,
            "ls_ratio": 1,
            "oi_delta": 0,
            "open_positions": 0,
            "total_trades": 0,
        }

        # ---------- risk stats robust ----------
        try:

            stats = {}

            stats_attr = getattr(bot.risk, "stats", None)

            if callable(stats_attr):
                try:
                    stats = stats_attr()
                except Exception:
                    stats = {}

            elif isinstance(stats_attr, dict):
                stats = stats_attr

            # ---------- core stats ----------
            signals["daily_pnl"] = stats.get("daily_pnl", 0)
            signals["consecutive_wins"] = stats.get("consecutive_wins", 0)
            signals["consecutive_losses"] = stats.get("consecutive_losses", 0)
            signals["total_trades"] = stats.get("total_trades", 0)

            signals["open_positions"] = (
                stats.get("open_spot", 0)
                + stats.get("open_futures", 0)
            )

            # ---------- winrate ----------
            recent = getattr(bot.risk, "_recent_pnls", [])

            if isinstance(recent, list):
                recent = recent[-20:]

                if len(recent) >= 5:
                    wins = sum(1 for p in recent if p > 0)
                    signals["win_rate_recent"] = (wins / len(recent)) * 100

            # ---------- drawdown ----------
            peak = getattr(bot.risk, "peak_balance", 0)

            est_balance_fn = getattr(bot.risk, "_estimated_balance", None)

            if callable(est_balance_fn) and peak > 0:

                current = est_balance_fn()

                if current is not None and peak > 0:
                    signals["drawdown_pct"] = max(
                        0,
                        (peak - current) / peak * 100
                    )

        except Exception as e:
            logger.debug(f"[REGIME] Stats error: {e}")

        # ---------- sentiment ----------
        try:

            sentiment_obj = getattr(bot, "_sentiment", None)

            if sentiment_obj:

                sent = sentiment_obj.get_sentiment()

                signals["fear_greed"] = sent.get("fear_greed", 50)

                signals["funding_avg"] = (
                    sent.get("funding_btc", 0)
                    + sent.get("funding_eth", 0)
                ) / 2

                signals["ls_ratio"] = sent.get("ls_ratio_btc", 1.0)

                signals["oi_delta"] = sent.get("oi_change_pct", 0)

        except Exception as e:
            logger.debug(f"[REGIME] Sentiment error: {e}")

        return signals

    # ============================================================
    # REGIME DECISION
    # ============================================================

    def _compute_ideal_regime(self, s):

        safe_reasons = []

        if s["drawdown_pct"] > 18:
            safe_reasons.append("drawdown")

        if s["daily_pnl"] < -10:
            safe_reasons.append("daily loss")

        if s["consecutive_losses"] >= 5:
            safe_reasons.append("loss streak")

        if s["fear_greed"] < 8 or s["fear_greed"] > 92:
            safe_reasons.append("extreme sentiment")

        if safe_reasons:
            logger.info(f"[REGIME] SAFE triggers {safe_reasons}")
            return "safe"

        aggro_checks = {

            "low_dd": s["drawdown_pct"] < 4,
            "good_wr": s["win_rate_recent"] > 55 or s["total_trades"] < 8,
            "win_streak": s["consecutive_wins"] >= 2 or s["total_trades"] < 5,
            "positive_day": s["daily_pnl"] >= 0,
            "sentiment_ok": 18 <= s["fear_greed"] <= 80,
            "funding_ok": abs(s["funding_avg"]) < 0.1,
            "ls_ok": 0.6 < s["ls_ratio"] < 2.2,
            "no_losses": s["consecutive_losses"] < 3,
        }

        passed = sum(aggro_checks.values())

        if passed >= 6:
            logger.info(f"[REGIME] AGGRO score {passed}/8")
            return "aggro"

        return "normal"

    # ============================================================
    # APPLY
    # ============================================================

    def _apply_regime(self, bot, regime, signals):

        try:

            from trading_bot.config import settings

            cfg = REGIME_CONFIGS[regime]["config"]

            changed = settings.set_many(cfg)

            label = REGIME_CONFIGS[regime]["label"]

            logger.info(
                f"[REGIME] {label} applicato ({len(changed)} parametri)"
            )

            try:

                bot.notifier.send(
                    f"⚡ <b>REGIME AUTO</b>\n"
                    f"{label}\n"
                    f"DD {signals.get('drawdown_pct',0):.1f}% "
                    f"WR {signals.get('win_rate_recent',0):.0f}% "
                    f"F&G {signals.get('fear_greed',0)}"
                )

            except Exception:
                pass

        except Exception as e:
            logger.error(f"[REGIME] apply error {e}")
