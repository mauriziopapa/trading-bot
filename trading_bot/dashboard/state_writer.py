"""
State Writer v3 — Atomic JSON write
═══════════════════════════════════════════════════════════════
FIX CRITICO:
  ✓ Atomic write con tempfile + os.rename per evitare letture corrotte
    (il dashboard server legge il file mentre il bot ci scrive)
  ✓ Usa cache emerging/sentiment (no doppia chiamata API)
"""

import json
import os
import time
import tempfile
from datetime import datetime, timezone
from loguru import logger

STATE_FILE = os.path.join(os.path.dirname(__file__), "dashboard_state.json")


def write_state(bot) -> None:
    """
    Serializza lo stato corrente del bot in dashboard_state.json.
    FIX: write atomico con tempfile + rename.
    """
    try:
        from trading_bot.config import settings

        # ── Balance ──────────────────────────────────────────────────────
        spot_bal    = 0.0
        futures_bal = 0.0
        try:
            if "spot" in settings.MARKET_TYPES:
                spot_bal = bot.exchange.get_usdt_balance("spot")
            if "futures" in settings.MARKET_TYPES:
                futures_bal = bot.exchange.get_usdt_balance("futures")
        except Exception:
            pass

        total = spot_bal + futures_bal
        start = getattr(bot.risk, "session_start_balance", total) or total
        pnl_usdt = total - start
        pnl_pct  = pnl_usdt / start * 100 if start > 0 else 0.0

        # ── Posizioni aperte con PnL live ─────────────────────────────────
        positions = []
        for trade in bot.risk.all_open_trades():
            try:
                ticker  = bot.exchange.fetch_ticker(trade["symbol"], trade["market"])
                current = float(ticker["last"])
                lev     = settings.DEFAULT_LEVERAGE if trade["market"] == "futures" else 1
                sign    = 1 if trade["side"] == "buy" else -1
                pnl_p   = (current - trade["entry"]) / trade["entry"] * 100 * lev * sign
                pnl_u   = trade["size"] * trade["entry"] * (pnl_p / 100)
                positions.append({
                    "symbol":    trade["symbol"],
                    "market":    trade["market"],
                    "side":      trade["side"],
                    "entry":     trade["entry"],
                    "current":   current,
                    "size":      trade["size"],
                    "pnl_pct":   round(pnl_p, 2),
                    "pnl_usdt":  round(pnl_u, 2),
                    "strategy":  trade.get("strategy", ""),
                    "opened_at": trade.get("open_ts", 0),
                })
            except Exception:
                pass

        # ── Sentiment (usa cache — NO doppia chiamata API) ───────────────
        sentiment_data = None
        try:
            analyzer = getattr(bot, "_sentiment", None)
            if analyzer:
                # get_sentiment() senza force=True usa la cache
                sentiment_data = analyzer.get_sentiment()
        except Exception:
            pass

        # ── Emerging coins (usa cache — NO doppia chiamata API) ──────────
        emerging_data = []
        try:
            scanner = getattr(bot, "_emerging", None)
            if scanner:
                # scan() senza force=True usa la cache
                emerging_data = scanner.scan()
        except Exception:
            pass

        # ── Assembla state ────────────────────────────────────────────────
        state = {
            "mode":        settings.TRADING_MODE,
            "status":      "running",
            "last_update": datetime.now(timezone.utc).isoformat(),
            "balance": {
                "spot":           round(spot_bal, 2),
                "futures":        round(futures_bal, 2),
                "total":          round(total, 2),
                "pnl_today_pct":  round(pnl_pct, 2),
                "pnl_today_usdt": round(pnl_usdt, 2),
            },
            "positions":  positions,
            "signals":    list(getattr(bot, "_recent_signals", []))[-20:],
            "logs":       list(getattr(bot, "_recent_logs", []))[-50:],
            "stats":      bot.risk.stats(),
            "sentiment":  sentiment_data,
            "emerging":   emerging_data[:8],
        }

        # ── FIX CRITICO: Atomic write ────────────────────────────────────
        # Scrivi su un file temporaneo nella stessa directory, poi rinomina.
        # os.rename() è atomico su Linux (stesso filesystem).
        # Questo previene letture di JSON parziali dal dashboard server.
        dir_path = os.path.dirname(STATE_FILE)
        fd, tmp_path = tempfile.mkstemp(dir=dir_path, suffix=".tmp", prefix=".state_")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(state, f)
            os.replace(tmp_path, STATE_FILE)  # atomico su stesso fs
        except Exception:
            # Pulizia file temporaneo in caso di errore
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    except Exception as e:
        logger.debug(f"[STATE_WRITER] Errore non critico: {e}")
