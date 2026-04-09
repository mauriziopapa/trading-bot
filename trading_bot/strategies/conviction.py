"""
Conviction Score Calculator (CWPE)
===================================
Pure, state-free scoring layer for the Conviction-Weighted Pyramid Entry
experimental patch.

Composite conviction score is computed as a weighted sum of 6 sub-components,
each in the range 0-100. The weights sum to 100 so the final score is also
0-100 and directly mappable to a tier.

Weights (of final 100):
    scanner     35
    ema         20
    macd        15
    volume      15
    atr         10
    btc_regime   5

Component rules (each produces a 0-100 value):
    scanner      scanner_score, clamped 0-100
    ema          |ema8-ema21|/ema21 * 100 * 50  capped 100
                 zero if EMA alignment is wrong vs scanner direction
    macd         |macd_hist|*10000              capped 100
                 +30% bonus if |hist| > |hist_prev| (accelerating)
                 zero if sign is wrong vs scanner direction
    volume       max(0, (current_vol/avg_vol - 1) * 50)  capped 100
    atr          1.5-3%  → 100
                 1-1.5 or 3-4% → 60
                 0.5-1 or 4-5% → 30
                 else → 0
    btc_regime   aligned → 100,  neutral → 50,  counter → 0

Tiers (total → size_multiplier, risk_pct):
    < 60   reject          0.0x  0.0%
    60-74  normal          1.0x  1.0%
    75-84  amplified       1.5x  1.5%
    85-92  high            2.0x  2.0%
    93+    conviction_play 3.0x  3.0%

Integration into MomentumStrategy is deferred to a later task.
"""

from __future__ import annotations

from dataclasses import dataclass

from loguru import logger


# Public constants
WEIGHT_SCANNER = 0.35
WEIGHT_EMA = 0.20
WEIGHT_MACD = 0.15
WEIGHT_VOLUME = 0.15
WEIGHT_ATR = 0.10
WEIGHT_BTC_REGIME = 0.05


@dataclass
class ConvictionBreakdown:
    """Per-component 0-100 values, weighted total, and action tier."""
    scanner: float
    ema: float
    macd: float
    volume: float
    atr: float
    btc_regime: float
    total: float
    tier: str
    size_multiplier: float
    risk_pct: float


# ─── Component scorers (each returns 0-100) ────────────────────────────

def _component_scanner(scanner_score: float) -> float:
    return max(0.0, min(100.0, float(scanner_score)))


def _component_ema(ema8: float, ema21: float, direction: str) -> float:
    if ema21 == 0:
        return 0.0
    # Wrong-side gate
    if direction == "long" and ema8 <= ema21:
        return 0.0
    if direction == "short" and ema8 >= ema21:
        return 0.0
    delta_pct = abs(ema8 - ema21) / abs(ema21) * 100.0  # percent
    return min(100.0, delta_pct * 50.0)


def _component_macd(
    macd_hist: float, macd_hist_prev: float, direction: str
) -> float:
    # Wrong-sign gate
    if direction == "long" and macd_hist <= 0:
        return 0.0
    if direction == "short" and macd_hist >= 0:
        return 0.0
    base = min(100.0, abs(macd_hist) * 10000.0)
    # Acceleration bonus: magnitude growing vs prior bar with matching sign
    accelerating = (
        macd_hist * macd_hist_prev >= 0
        and abs(macd_hist) > abs(macd_hist_prev)
    )
    if accelerating:
        base *= 1.30
    return min(100.0, base)


def _component_volume(current_volume: float, avg_volume: float) -> float:
    if avg_volume <= 0:
        return 0.0
    ratio = float(current_volume) / float(avg_volume)
    raw = (ratio - 1.0) * 50.0
    if raw <= 0:
        return 0.0
    return min(100.0, raw)


def _component_atr(atr_pct: float) -> float:
    a = float(atr_pct)
    if 1.5 <= a <= 3.0:
        return 100.0
    if (1.0 <= a < 1.5) or (3.0 < a <= 4.0):
        return 60.0
    if (0.5 <= a < 1.0) or (4.0 < a <= 5.0):
        return 30.0
    return 0.0


def _component_btc_regime(btc_regime: str) -> float:
    r = (btc_regime or "").lower()
    if r == "aligned":
        return 100.0
    if r == "counter":
        return 0.0
    # 'neutral', '', unknown → baseline
    return 50.0


# ─── Tier mapping ──────────────────────────────────────────────────────

def _tier_for(total: float) -> tuple[str, float, float]:
    if total < 60:
        return ("reject", 0.0, 0.0)
    if total < 75:
        return ("normal", 1.0, 1.0)
    if total < 85:
        return ("amplified", 1.5, 1.5)
    if total < 93:
        return ("high", 2.0, 2.0)
    return ("conviction_play", 3.0, 3.0)


# ─── Public API ────────────────────────────────────────────────────────

def calculate_conviction(
    *,
    scanner_score: float,
    direction: str,
    ema8: float,
    ema21: float,
    macd_hist: float,
    macd_hist_prev: float,
    current_volume: float,
    avg_volume: float,
    atr_pct: float,
    btc_regime: str,
) -> ConvictionBreakdown:
    """
    Compute composite conviction score + action tier.

    All inputs are kwargs-only to prevent positional mistakes at call sites.
    `direction` must be 'long' or 'short' (it gates EMA and MACD alignment).
    `btc_regime` is 'aligned', 'neutral', or 'counter'.

    Returns a ConvictionBreakdown holding per-component 0-100 values, the
    weighted 0-100 total, and the derived tier with its size + risk.
    """
    s = _component_scanner(scanner_score)
    e = _component_ema(ema8, ema21, direction)
    m = _component_macd(macd_hist, macd_hist_prev, direction)
    v = _component_volume(current_volume, avg_volume)
    a = _component_atr(atr_pct)
    b = _component_btc_regime(btc_regime)

    total = (
        s * WEIGHT_SCANNER
        + e * WEIGHT_EMA
        + m * WEIGHT_MACD
        + v * WEIGHT_VOLUME
        + a * WEIGHT_ATR
        + b * WEIGHT_BTC_REGIME
    )
    # Clamp defensively (max possible = 100)
    total = max(0.0, min(100.0, total))

    tier, size_multiplier, risk_pct = _tier_for(total)

    breakdown = ConvictionBreakdown(
        scanner=round(s, 2),
        ema=round(e, 2),
        macd=round(m, 2),
        volume=round(v, 2),
        atr=round(a, 2),
        btc_regime=round(b, 2),
        total=round(total, 2),
        tier=tier,
        size_multiplier=size_multiplier,
        risk_pct=risk_pct,
    )

    logger.debug(
        f"[CWPE CONVICTION] total={total:.1f} tier={tier} "
        f"scanner={s:.1f} ema={e:.1f} macd={m:.1f} "
        f"vol={v:.1f} atr={a:.1f} btc={b:.1f}"
    )

    return breakdown
