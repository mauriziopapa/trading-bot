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
    ) -> Optional[Signal]:
        """
        Generate a MOMENTUM signal.

        Extra kwargs (from _scan_scalping in main.py):
            scanner_score:     SniperScannerV2 composite score
            scanner_direction: "long" or "short"
            scanner_momentum:  raw momentum value from scanner
            scanner_volume:    24h volume from scanner
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

            # ── Determine side ─────────────────────────────────────────────
            side: Optional[str] = None

            if scanner_direction == "long":
                if ema_long and macd_bull:
                    side = "buy"
                elif ema_long:
                    # Partial confirmation — lower confidence
                    side = "buy"
            elif scanner_direction == "short":
                if ema_short and macd_bear:
                    side = "sell"
                elif ema_short:
                    side = "sell"
            else:
                # No scanner direction — use EMA+MACD standalone
                if ema_long and macd_bull:
                    side = "buy"
                elif ema_short and macd_bear:
                    side = "sell"

            if side is None:
                logger.debug(
                    f"[MOMENTUM] {symbol} no EMA/MACD confirmation "
                    f"(scanner_dir={scanner_direction} ema_long={ema_long} macd_bull={macd_bull})"
                )
                return None

            # ── ATR for SL/TP ──────────────────────────────────────────────
            atr_val = self._atr_value(df)
            if atr_val <= 0:
                return None

            entry = _safe_float(close.iloc[-1])
            if entry <= 0:
                return None

            if side == "buy":
                stop_loss   = entry - sl_mult * atr_val
                take_profit = entry + tp_mult * atr_val
            else:
                stop_loss   = entry + sl_mult * atr_val
                take_profit = entry - tp_mult * atr_val

            # ── Confidence: base + scanner score bonus ─────────────────────
            # 2 confirmations (EMA + MACD) → higher confidence
            both_confirmed = (
                (side == "buy"  and ema_long  and macd_bull) or
                (side == "sell" and ema_short and macd_bear)
            )
            confidence = 70.0
            if both_confirmed:
                confidence += 10.0
            confidence += min(15.0, scanner_score / 10)   # up to +15 from score
            confidence = min(confidence, 95.0)

            # ── Build signal snapshot for attribution ──────────────────────
            signal_snapshot = {
                "ema8":           round(float(ema8.iloc[-1]), 6),
                "ema21":          round(float(ema21.iloc[-1]), 6),
                "macd_hist":      round(float(hist.iloc[-1]), 6),
                "atr":            round(atr_val, 6),
                "scanner_score":  round(scanner_score, 2),
                "scanner_dir":    scanner_direction,
                "scanner_mom":    round(scanner_momentum, 6),
                "both_confirmed": both_confirmed,
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
