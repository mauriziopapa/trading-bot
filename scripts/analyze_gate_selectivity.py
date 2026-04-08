"""
Gate selectivity analysis — Q-A.
================================
Measures how aggressively MomentumStrategy rejects candidates through each
confirmation layer (scanner → EMA → MACD → both).

Primary mode: call SniperScannerV2.scan(force=True) with live exchange, then
re-fetch OHLCV and compute EMA8/21 + MACD for each candidate.

Fallback mode: if scanner returns 0 candidates, no exchange is available, or
credentials are missing, generate 10 synthetic candidates with random OHLCV
and fake score=75 (for validation logic only, not production decisions).

Usage:
    python scripts/analyze_gate_selectivity.py
"""

import sys
import os
import math

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pandas as pd


# ── Indicator helpers (duplicated to avoid framework dep) ──────────────────
def _ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def _macd_hist(close: pd.Series) -> float:
    ema12 = _ema(close, 12)
    ema26 = _ema(close, 26)
    macd = ema12 - ema26
    signal = _ema(macd, 9)
    return float((macd - signal).iloc[-1])


def _ema_cross(close: pd.Series) -> tuple[bool, bool]:
    e8 = _ema(close, 8).iloc[-1]
    e21 = _ema(close, 21).iloc[-1]
    return (e8 > e21, e8 < e21)  # (bullish, bearish)


# ── Synthetic candidate generator ──────────────────────────────────────────
def _build_synthetic_candidates(n: int = 10, seed: int = 20260408) -> list[dict]:
    """
    Create n fake candidates with random-walk OHLCV. Half are uptrending (long),
    half downtrending (short). All get score=75 so they pass the scanner gate.
    """
    rng = np.random.default_rng(seed)
    candidates = []
    for i in range(n):
        direction = "long" if i % 2 == 0 else "short"
        drift = 0.05 if direction == "long" else -0.05
        closes = 100 + np.cumsum(rng.normal(drift, 0.5, 120))
        df = pd.DataFrame({
            "open":   closes - 0.1,
            "high":   closes + 0.3,
            "low":    closes - 0.3,
            "close":  closes,
            "volume": rng.uniform(1e6, 3e6, 120),
        })
        candidates.append({
            "symbol":    f"FAKE{i}/USDT:USDT",
            "score":     75.0,
            "direction": direction,
            "volume":    float(df["volume"].sum() * float(closes[-1])),
            "momentum":  float(rng.uniform(-0.05, 0.05)),
            "_df":       df,
        })
    return candidates


# ── Real scanner path ──────────────────────────────────────────────────────
def _try_real_scanner():
    """
    Attempt to call the live scanner. Returns a list of candidates (each with
    '_df' attached) or None if anything fails.
    """
    try:
        from trading_bot.utils.exchange import ExchangeManager
        from trading_bot.utils.sniper_scanner_v2 import SniperScannerV2
        from trading_bot.utils.shared import ohlcv_to_df
    except Exception as e:
        print(f"  [fallback] imports failed: {e}")
        return None

    try:
        ex = ExchangeManager()
    except Exception as e:
        print(f"  [fallback] exchange init failed: {e}")
        return None

    try:
        scanner = SniperScannerV2(exchange=ex)
        results = scanner.scan(force=True) or []
    except Exception as e:
        print(f"  [fallback] scanner.scan() failed: {e}")
        return None

    if not results:
        print("  [fallback] scanner returned 0 candidates")
        return None

    # Attach OHLCV
    enriched = []
    for c in results:
        sym = c.get("symbol", "")
        if not sym:
            continue
        try:
            ohlcv = ex.fetch_ohlcv(sym, "1m", 120, "futures")
            if not ohlcv or len(ohlcv) < 50:
                continue
            c["_df"] = ohlcv_to_df(ohlcv)
            enriched.append(c)
        except Exception:
            continue

    return enriched if enriched else None


# ── Main analysis ──────────────────────────────────────────────────────────
def analyze():
    print("\n══════════════════════════════════════════════════════════════")
    print("  MomentumStrategy gate selectivity analysis (Q-A)")
    print("══════════════════════════════════════════════════════════════\n")

    # Load min_score threshold from settings (falls back to default)
    try:
        from trading_bot.config import settings
        min_score = float(getattr(settings, "MOMENTUM_MIN_SCORE", 20.0))
    except Exception:
        min_score = 20.0

    # Step 1: get candidates (real or synthetic)
    candidates = _try_real_scanner()
    mode = "real scanner (SniperScannerV2.scan)"
    if candidates is None:
        candidates = _build_synthetic_candidates(n=10)
        mode = "SYNTHETIC (10 fake candidates, score=75) — validation only"

    print(f"Data source: {mode}")
    print(f"MOMENTUM_MIN_SCORE threshold: {min_score}")
    print(f"Total candidates received: {len(candidates)}\n")

    if not candidates:
        print("  No candidates to analyze. Exiting.")
        return

    # Step 2: walk each gate
    total = len(candidates)

    pass_scanner_score = 0
    pass_scanner_ema = 0
    pass_scanner_macd = 0
    pass_both_confirmed = 0

    per_candidate = []

    for c in candidates:
        sym = c.get("symbol", "?")
        score = float(c.get("score", 0))
        direction = c.get("direction", "").lower()
        df = c.get("_df")

        scanner_pass = score >= min_score
        if scanner_pass:
            pass_scanner_score += 1

        ema_pass = False
        macd_pass = False

        if scanner_pass and df is not None and len(df) >= 30:
            bull, bear = _ema_cross(df["close"])
            hist = _macd_hist(df["close"])

            if direction == "long":
                ema_pass = bull
                macd_pass = hist > 0
            elif direction == "short":
                ema_pass = bear
                macd_pass = hist < 0
            else:
                ema_pass = bull or bear
                macd_pass = hist != 0

            if ema_pass:
                pass_scanner_ema += 1
            if macd_pass:
                pass_scanner_macd += 1
            if ema_pass and macd_pass:
                pass_both_confirmed += 1

        per_candidate.append({
            "symbol": sym,
            "score":  score,
            "dir":    direction,
            "scanner": "✓" if scanner_pass else "✗",
            "ema":     "✓" if ema_pass else "✗",
            "macd":    "✓" if macd_pass else "✗",
            "both":    "✓" if (ema_pass and macd_pass) else "✗",
        })

    # Step 3: report
    def pct(n, d):
        return f"{(n/d*100):.1f}%" if d > 0 else "—"

    print("── Per-candidate breakdown ────────────────────────────────────────")
    print(f"  {'symbol':<22} {'score':>6} {'dir':>6} {'scan':>5} {'ema':>5} {'macd':>5} {'both':>5}")
    for r in per_candidate:
        print(f"  {r['symbol']:<22} {r['score']:>6.1f} {r['dir']:>6} "
              f"{r['scanner']:>5} {r['ema']:>5} {r['macd']:>5} {r['both']:>5}")

    print("\n── Funnel summary ─────────────────────────────────────────────────")
    print(f"  Total candidates:              {total}")
    print(f"  Pass scanner only:             {pass_scanner_score}/{total}  ({pct(pass_scanner_score, total)})")
    print(f"  Pass scanner + EMA:            {pass_scanner_ema}/{total}  ({pct(pass_scanner_ema, total)})")
    print(f"  Pass scanner + MACD:           {pass_scanner_macd}/{total}  ({pct(pass_scanner_macd, total)})")
    print(f"  Pass scanner + EMA + MACD:     {pass_both_confirmed}/{total}  ({pct(pass_both_confirmed, total)})  ← mandatory gate after [FIX]")

    # Step 4: selectivity interpretation
    reject_rate = (total - pass_both_confirmed) / total * 100 if total > 0 else 0
    print(f"\n  Rejection rate:                {reject_rate:.1f}%")

    if reject_rate > 95:
        print("  ⚠  Rejection rate >95% — strategy may be too tight; expect few trades")
    elif reject_rate < 30:
        print("  ⚠  Rejection rate <30% — strategy may be too loose; expect overtrading")
    else:
        print("  ✓  Rejection rate in expected 30–95% band")

    print("\n══════════════════════════════════════════════════════════════\n")


if __name__ == "__main__":
    analyze()
