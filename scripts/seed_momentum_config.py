"""
Seed bot_config table with momentum-only paper mode configuration.
=================================================================
Idempotent: uses ON CONFLICT DO NOTHING — safe to re-run.
Only inserts missing keys; never overwrites existing user overrides.

Usage:
    python scripts/seed_momentum_config.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

MOMENTUM_CONFIG = {
    # ── Strategy enablement ─────────────────────────────────────────────────
    "ENABLE_RSI_MACD":    "false",   # disabled during refactor phase
    "ENABLE_BOLLINGER":   "false",   # disabled during refactor phase
    "ENABLE_BREAKOUT":    "false",   # disabled during refactor phase
    "ENABLE_SCALPING":    "false",   # disabled during refactor phase
    "ENABLE_EMERGING":    "false",   # disabled during refactor phase
    "ENABLE_MOMENTUM":    "true",    # THE only active strategy
    "STRATEGIES_ENABLED": "MOMENTUM",  # comma-separated allowlist for strategy guard

    # ── Momentum strategy tuning ─────────────────────────────────────────────
    "MOMENTUM_MIN_SCORE":         "20",       # SniperScannerV2 score floor
    "MOMENTUM_MIN_VOLUME_USD":    "5000000",  # 5M USD 24h volume minimum
    "MOMENTUM_RISK_PCT":          "1.0",      # risk 1% of balance per trade
    "MOMENTUM_LEVERAGE":          "2",        # 2x leverage (low risk)
    "MOMENTUM_MAX_HOLD_SECONDS":  "14400",    # 4 hours max
    "MOMENTUM_MIN_HOLD_SECONDS":  "600",      # 10 min min (anti-churn)
    "MOMENTUM_SL_ATR_MULT":       "1.0",      # SL = entry ± 1.0 × ATR
    "MOMENTUM_TP_ATR_MULT":       "2.5",      # TP = entry ± 2.5 × ATR (RR 2.5)
    "MOMENTUM_TRAILING_ENABLE":   "true",     # enable trailing stop
    "MOMENTUM_TRAIL_ACTIVATION_R": "1.5",     # activate trail at 1.5R profit
    "MOMENTUM_TRAIL_DIST_ATR":    "0.8",      # trail distance = 0.8 × ATR
    "MOMENTUM_COOLDOWN_LOSS_MIN": "30",       # 30 min cooldown after loss
    "MOMENTUM_COOLDOWN_WIN_MIN":  "5",        # 5 min cooldown after win
    "MOMENTUM_MAX_CONCURRENT":    "1",        # max 1 momentum position at a time

    # ── Risk manager extras ──────────────────────────────────────────────────
    "MANUAL_UNLOCK_REQUIRED":           "true",  # global_stop cannot auto-reset
    "PER_SYMBOL_MAX_LOSS_USDT":         "1.0",   # emergency close if loss > 1 USDT
    "PER_SYMBOL_MAX_CONSEC_LOSSES":     "2",     # stop symbol after 2 consecutive losses
    "PER_STRATEGY_MAX_DRAWDOWN_PCT":    "10.0",  # stop strategy at 10% drawdown
    "STALE_GLOBAL_STOP_ALERT_MIN":      "15",    # Telegram alert after 15 min global_stop
}


def seed(engine=None):
    if engine is None:
        from sqlalchemy import create_engine
        from sqlalchemy.pool import NullPool
        from dotenv import load_dotenv
        load_dotenv()

        db_url = os.getenv("DATABASE_URL", "")
        if not db_url:
            print("[SEED] DATABASE_URL not set — skipping")
            return

        connect_args = {"sslmode": "require"} if "railway" in db_url else {}
        engine = create_engine(db_url, poolclass=NullPool, connect_args=connect_args)

    from sqlalchemy import text
    from datetime import datetime, timezone

    inserted = 0
    skipped = 0

    with engine.connect() as conn:
        for key, value in MOMENTUM_CONFIG.items():
            try:
                conn.execute(
                    text(
                        "INSERT INTO bot_config (key, value, updated_at) "
                        "VALUES (:k, :v, :ts) "
                        "ON CONFLICT (key) DO NOTHING"
                    ),
                    {"k": key, "v": value, "ts": datetime.now(timezone.utc)},
                )
                conn.commit()
                inserted += 1
                print(f"[SEED] inserted  {key} = {value}")
            except Exception as e:
                skipped += 1
                print(f"[SEED] skipped   {key}: {e}")

    print(f"\n[SEED] done — {inserted} inserted, {skipped} skipped")


if __name__ == "__main__":
    seed()
