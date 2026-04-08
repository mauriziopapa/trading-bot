"""
Momentum Strategy v1.0
======================
Entry logic:
  - Uses SniperScannerV2 pre-scored candidates (score passed via `scanner_score` kwarg)
  - Confirms momentum direction with short EMA cross + MACD histogram
  - ATR-based SL/TP (configurable multipliers from bot_config)
  - Trailing stop activation at configurable R-multiple

Configuration (all from bot_config DB):
  MOMENTUM_MIN_SCORE          minimum scanner score (default 20)
  MOMENTUM_MIN_VOLUME_USD     minimum 24h volume (default 5_000_000)
  MOMENTUM_SL_ATR_MULT        SL distance multiplier (default 1.0)
  MOMENTUM_TP_ATR_MULT        TP distance multiplier (default 2.5)
  MOMENTUM_LEVERAGE           leverage (default 2)

Designed for paper mode validation only.
"""

from typing import Optional
from datetime import datetime, timezone
import pandas as pd
import numpy as np
from loguru import logger

from trading_bot.strategies.base import BaseStrategy, Signal
from trading_bot.config import settings


def _safe_float(x, default=0.0):
    try:
        return float(x) if x is not None else default
    except Exception:
        return default


class MomentumStrategy(BaseStrategy):
    NAME = "MOMENTUM"
    MIN_CANDLES = 30

    # ── Config keys with defaults ─────────────────────────────────────────────
    _CFG = {
        "min_score":    ("MOMENTUM_MIN_SCORE",      20.0),
        "min_volume":   ("MOMENTUM_MIN_VOLUME_USD",  5_000_000.0),
        "sl_atr_mult":  ("MOMENTUM_SL_ATR_MULT",     1.0),
        "tp_atr_mult":  ("MOMENTUM_TP_ATR_MULT",     2.5),
        "leverage":     ("MOMENTUM_LEVERAGE",         2),
    }

    def _cfg(self, key: str) -> float:
        cfg_key, default = self._CFG[key]
        try:
            return _safe_float(getattr(settings, cfg_key, default), default)
        except Exception:
            return default

    # ──────────────────────────────────────────────────────────────────────────

    def analyze(
        self,
        df: pd.DataFrame,
        symbol: str,
        market: str,
        scanner_score: float = 0.0,
        scanner_direction: str = "",
        scanner_momentum: float = 0.0,
        scanner_volume: float = 0.0,
        scanner_change_24h: float = 0.0,
        scanner_source: str = "sniper_v2",
    ) -> Optional[Signal]:
        """
        Generate a MOMENTUM signal.

        Extra kwargs (from _scan_scalping in main.py):
            scanner_score:      SniperScannerV2 composite score
            scanner_direction:  "long" or "short"
            scanner_momentum:   raw momentum value from scanner
            scanner_volume:     24h volume from scanner (USDT)
            scanner_change_24h: 24h price change percent from scanner
            scanner_source:     scanner identifier ("sniper_v2")
        """
        try:
            if df is None or len(df) < self.MIN_CANDLES:
                return None

            min_score  = self._cfg("min_score")
            min_volume = self._cfg("min_volume")
            sl_mult    = self._cfg("sl_atr_mult")
            tp_mult    = self._cfg("tp_atr_mult")

            # ── Gate 1: scanner pre-filter ─────────────────────────────────
            if scanner_score < min_score:
                logger.debug(
                    f"[MOMENTUM] {symbol} score={scanner_score:.1f} < {min_score} — skip"
                )
                return None

            if scanner_volume > 0 and scanner_volume < min_volume:
                logger.debug(
                    f"[MOMENTUM] {symbol} volume={scanner_volume:.0f} < {min_volume:.0f} — skip"
                )
                return None

            # ── Gate 2: confirm direction with EMA cross ───────────────────
            close = df["close"]
            ema8  = close.ewm(span=8, adjust=False).mean()
            ema21 = close.ewm(span=21, adjust=False).mean()

            ema_long  = ema8.iloc[-1] > ema21.iloc[-1]   # bullish cross
            ema_short = ema8.iloc[-1] < ema21.iloc[-1]   # bearish cross

            # ── Gate 3: MACD histogram direction ──────────────────────────
            ema12  = close.ewm(span=12, adjust=False).mean()
            ema26  = close.ewm(span=26, adjust=False).mean()
            macd   = ema12 - ema26
            signal_line = macd.ewm(span=9, adjust=False).mean()
            hist   = macd - signal_line
            macd_bull = hist.iloc[-1] > 0
            macd_bear = hist.iloc[-1] < 0

            # ── Determine intended side from scanner ───────────────────────
            if scanner_direction == "long":
                side = "buy"
                ema_ok = ema_long
                macd_ok = macd_bull
            elif scanner_direction == "short":
                side = "sell"
                ema_ok = ema_short
                macd_ok = macd_bear
            else:
                # Standalone fallback: use whichever direction EMA+MACD both agree
                if ema_long and macd_bull:
                    side, ema_ok, macd_ok = "buy", True, True
                elif ema_short and macd_bear:
                    side, ema_ok, macd_ok = "sell", True, True
                else:
                    logger.debug(
                        f"[MOMENTUM REJECT] {symbol} reason=no_scanner_direction_and_no_agreement"
                    )
                    return None

            # ── MANDATORY DUAL CONFIRMATION ────────────────────────────────
            # Neither ema_only nor macd_only is accepted. Both must agree with
            # the scanner direction, or the signal is rejected.
            if not ema_ok:
                logger.info(f"[MOMENTUM REJECT] {symbol} reason=ema_not_confirming")
                return None
            if not macd_ok:
                logger.info(f"[MOMENTUM REJECT] {symbol} reason=macd_not_confirming")
                return None

            # ── ATR for SL/TP ──────────────────────────────────────────────
            atr_val = self._atr_value(df)
            if atr_val <= 0:
                return None

            entry = _safe_float(close.iloc[-1])
            if entry <= 0:
                return None

            # ── ATR range gate ─────────────────────────────────────────────
            # Too-low ATR = insufficient room for R:R after fees.
            # Too-high ATR = stop-loss so wide position sizing becomes unsafe.
            atr_pct = (atr_val / entry * 100.0) if entry > 0 else 0.0
            if atr_pct < 0.5:
                logger.info(
                    f"[MOMENTUM REJECT] {symbol} reason=atr_too_low ({atr_pct:.3f}%)"
                )
                return None
            if atr_pct > 5.0:
                logger.info(
                    f"[MOMENTUM REJECT] {symbol} reason=atr_too_high ({atr_pct:.3f}%)"
                )
                return None

            # ── Expected-value gate (EV > 3 × round-trip fees) ─────────────
            # Reference notional cancels in the comparison (ratio-based).
            # expected_profit = notional * tp_mult * atr_pct / 100
            # fee_cost        = notional * 0.0006 * 2   (entry + exit taker)
            # Require expected_profit >= 3 × fee_cost
            ref_notional = 100.0  # arbitrary; both sides scale linearly
            expected_profit_usdt = ref_notional * tp_mult * atr_pct / 100.0
            fee_cost_usdt = ref_notional * 0.0006 * 2
            if expected_profit_usdt < 3 * fee_cost_usdt:
                logger.info(
                    f"[MOMENTUM REJECT] {symbol} reason=ev_below_3x_fees "
                    f"(ep={expected_profit_usdt:.4f} vs 3x_fees={3 * fee_cost_usdt:.4f})"
                )
                return None

            if side == "buy":
                stop_loss   = entry - sl_mult * atr_val
                take_profit = entry + tp_mult * atr_val
            else:
                stop_loss   = entry + sl_mult * atr_val
                take_profit = entry - tp_mult * atr_val

            # ── Confirmation flags per indicator layer ─────────────────────
            # Scanner direction is the primary intent; EMA/MACD either confirm it or not.
            ema_confirms_scanner = (
                (scanner_direction == "long"  and ema_long) or
                (scanner_direction == "short" and ema_short) or
                # Standalone fallback: no scanner_direction → EMA must match chosen side
                (scanner_direction not in ("long", "short") and
                 ((side == "buy" and ema_long) or (side == "sell" and ema_short)))
            )
            macd_confirms_scanner = (
                (scanner_direction == "long"  and macd_bull) or
                (scanner_direction == "short" and macd_bear) or
                (scanner_direction not in ("long", "short") and
                 ((side == "buy" and macd_bull) or (side == "sell" and macd_bear)))
            )
            both_confirmed = ema_confirms_scanner and macd_confirms_scanner

            # Human-readable labels for audit / reporting
            if ema_long and not ema_short:
                ema_cross_direction = "bullish"
            elif ema_short and not ema_long:
                ema_cross_direction = "bearish"
            else:
                ema_cross_direction = "none"
            macd_hist_direction = "positive" if hist.iloc[-1] > 0 else "negative"

            if both_confirmed:
                entry_reason = "scanner+ema+macd"
            elif ema_confirms_scanner:
                entry_reason = "scanner+ema_only"
            elif macd_confirms_scanner:
                entry_reason = "scanner+macd_only"
            else:
                entry_reason = "scanner_only"

            # ── Confidence: base + scanner score bonus ─────────────────────
            confidence_base = int(round(min(70.0 + scanner_score / 10.0, 85.0)))
            confidence = 70.0
            if both_confirmed:
                confidence += 10.0
            confidence += min(15.0, scanner_score / 10)   # up to +15 from score
            confidence = min(confidence, 95.0)
            confidence_final = int(round(confidence))

            # ── Volatility metrics ─────────────────────────────────────────
            atr_pct_of_price = (atr_val / entry * 100.0) if entry > 0 else 0.0

            # ── Build full signal snapshot for attribution ─────────────────
            signal_snapshot = {
                # Scanner layer
                "scanner_score":          round(scanner_score, 2),
                "scanner_direction":      scanner_direction,
                "scanner_source":         scanner_source,
                "scanner_volume_usd":     round(scanner_volume, 2),
                "scanner_change_24h_pct": round(scanner_change_24h, 4),
                "scanner_momentum":       round(scanner_momentum, 6),

                # EMA layer
                "ema8":                   round(float(ema8.iloc[-1]), 6),
                "ema21":                  round(float(ema21.iloc[-1]), 6),
                "ema_cross_direction":    ema_cross_direction,
                "ema_confirms_scanner":   bool(ema_confirms_scanner),

                # MACD layer
                "macd_hist":              round(float(hist.iloc[-1]), 6),
                "macd_hist_direction":    macd_hist_direction,
                "macd_confirms_scanner":  bool(macd_confirms_scanner),

                # Volatility
                "atr_value":              round(atr_val, 6),
                "atr_pct_of_price":       round(atr_pct_of_price, 4),

                # Aggregate
                "confidence_base":        confidence_base,
                "confidence_final":       confidence_final,
                "both_confirmed":         bool(both_confirmed),
                "entry_reason":           entry_reason,

                # Timestamps
                "signal_timestamp":       datetime.now(timezone.utc).isoformat(),
            }

            sig = Signal(
                strategy    = self.NAME,
                symbol      = symbol,
                market      = market,
                side        = side,
                confidence  = confidence,
                entry       = entry,
                stop_loss   = stop_loss,
                take_profit = take_profit,
                atr         = atr_val,
                timeframe   = "1m",
                notes       = f"score={scanner_score:.1f} dir={scanner_direction}",
            )
            # Attach snapshot for persistence in _execute_signal
            sig._snapshot = signal_snapshot

            logger.info(
                f"[MOMENTUM] {symbol} {side} entry={entry:.6f} "
                f"sl={stop_loss:.6f} tp={take_profit:.6f} "
                f"atr={atr_val:.6f} conf={confidence:.0f} "
                f"score={scanner_score:.1f} confirmed={both_confirmed}"
            )
            return sig

        except Exception as e:
            logger.error(f"[MOMENTUM] {symbol} analyze error: {e}")
            return None
