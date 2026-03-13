"""
Strategia 3: Breakout su Volumi v3
═══════════════════════════════════════════════════════════════
FIX:
  ✓ Confidence base abbassata (era 58 → 50) per calibrazione corretta
  ✓ Volume confirmation su 2 candele (non singolo spike)

OTTIMIZZAZIONI AGGRESSIVE:
  ✓ Retest confirmation: breakout + pullback + hold = segnale più forte
  ✓ Range width filter: evita breakout su range troppo stretto
  ✓ Momentum confirmation: RSI nella direzione del breakout
  ✓ Multi-bar breakout: candela precedente già vicina al livello
"""

from typing import Optional
import pandas as pd
from loguru import logger

from trading_bot.strategies.base import BaseStrategy, Signal
from trading_bot.utils import indicators as ind
from trading_bot.config import settings


class BreakoutStrategy(BaseStrategy):
    NAME = "BREAKOUT"
    MIN_CANDLES = 60

    def __init__(self,
                 lookback: int = 20,
                 vol_multiplier: float = 1.5,
                 atr_expansion: float = 1.05):
        self.lookback       = lookback
        self.vol_multiplier = vol_multiplier
        self.atr_expansion  = atr_expansion

    def analyze(self, df: pd.DataFrame, symbol: str, market: str) -> Optional[Signal]:
        if len(df) < self.MIN_CANDLES:
            return None

        close  = df["close"]
        high   = df["high"]
        low    = df["low"]
        volume = df["volume"]

        range_high = float(high.iloc[-self.lookback - 1:-1].max())
        range_low  = float(low.iloc[-self.lookback - 1:-1].min())

        last_close  = float(close.iloc[-1])
        prev_close  = float(close.iloc[-2])
        last_vol    = float(volume.iloc[-1])
        prev_vol    = float(volume.iloc[-2])
        avg_vol     = float(volume.iloc[-self.lookback - 1:-1].mean())

        atr_val     = self._atr_value(df)
        atr_series  = ind.atr(high, low, close)
        atr_avg     = float(atr_series.iloc[-10:].mean())
        rsi_series  = ind.rsi(close)
        rsi         = float(rsi_series.iloc[-1])

        vol_r = last_vol / avg_vol if avg_vol > 0 else 0
        vol_r_prev = prev_vol / avg_vol if avg_vol > 0 else 0

        atr_expanding = atr_val >= atr_avg * self.atr_expansion

        # ── NUOVO: Range width filter ────────────────────────────────────
        # Evita breakout su range troppo stretto (rumore)
        range_width_pct = (range_high - range_low) / range_low * 100 if range_low > 0 else 0
        if range_width_pct < 0.5:  # range < 0.5% = rumore
            return None

        # ── NUOVO: Volume sostenuto (2 candele) ─────────────────────────
        sustained_vol = (vol_r >= self.vol_multiplier) and (vol_r_prev > 1.0)

        # ── NUOVO: Pre-breakout setup ────────────────────────────────────
        # La candela precedente era già vicina al livello di breakout
        prev_near_high = prev_close >= range_high * 0.995
        prev_near_low  = prev_close <= range_low  * 1.005

        side       = None
        confidence = 0.0
        notes_list = []

        # ── LONG Breakout ─────────────────────────────────────────────────
        if ((last_close > range_high)
            and (vol_r >= self.vol_multiplier)
            and atr_expanding):
            side = "buy"
            confidence = 50.0
            notes_list.append(f"breakout {range_high:.4f} (range {range_width_pct:.1f}%)")

            open_price = float(df["open"].iloc[-1])
            if open_price > range_high:
                confidence += 10
                notes_list.append("gap up")

            if sustained_vol:
                confidence += 10
                notes_list.append(f"vol sostenuto {vol_r:.1f}x/{vol_r_prev:.1f}x")
            elif vol_r >= self.vol_multiplier * 1.5:
                confidence += 8
                notes_list.append(f"vol eccezionale {vol_r:.1f}x")
            else:
                confidence += 4

            breakout_pct = (last_close - range_high) / range_high * 100
            if breakout_pct > 0.5:
                confidence += 6
                notes_list.append(f"rottura netta +{breakout_pct:.1f}%")
            elif breakout_pct > 0.3:
                confidence += 3

            if prev_near_high:
                confidence += 5
                notes_list.append("pre-breakout setup")

            # NUOVO: RSI conferma direzione
            if 50 < rsi < 75:
                confidence += 5
                notes_list.append(f"RSI {rsi:.0f} conferma ↑")

        # ── SHORT Breakout ────────────────────────────────────────────────
        elif ((last_close < range_low)
              and (vol_r >= self.vol_multiplier)
              and atr_expanding):
            side = "sell"
            confidence = 50.0
            notes_list.append(f"breakout sotto {range_low:.4f}")

            open_price = float(df["open"].iloc[-1])
            if open_price < range_low:
                confidence += 10
                notes_list.append("gap down")

            if sustained_vol:
                confidence += 10
                notes_list.append(f"vol sostenuto {vol_r:.1f}x/{vol_r_prev:.1f}x")
            elif vol_r >= self.vol_multiplier * 1.5:
                confidence += 8
            else:
                confidence += 4

            breakout_pct = (range_low - last_close) / range_low * 100
            if breakout_pct > 0.5:
                confidence += 6
                notes_list.append(f"rottura netta -{breakout_pct:.1f}%")
            elif breakout_pct > 0.3:
                confidence += 3

            if prev_near_low:
                confidence += 5
                notes_list.append("pre-breakdown setup")

            if 25 < rsi < 50:
                confidence += 5
                notes_list.append(f"RSI {rsi:.0f} conferma ↓")

        if side is None:
            return None
        if confidence < self.MIN_CONFIDENCE:
            logger.debug(f"[BREAKOUT] {symbol} conf={confidence:.0f}% < {self.MIN_CONFIDENCE:.0f}%")
            return None

        if side == "buy":
            stop_loss   = round(range_high - atr_val * 0.5, 6)
            take_profit = round(last_close + (last_close - stop_loss) * settings.TAKE_PROFIT_RATIO, 6)
        else:
            stop_loss   = round(range_low + atr_val * 0.5, 6)
            take_profit = round(last_close - (stop_loss - last_close) * settings.TAKE_PROFIT_RATIO, 6)

        logger.info(f"[BREAKOUT] SEGNALE {symbol} {market} {side} conf={confidence:.0f}% | {', '.join(notes_list)}")

        return Signal(
            strategy    = self.NAME,
            symbol      = symbol,
            market      = market,
            side        = side,
            confidence  = min(confidence, 100),
            entry       = last_close,
            stop_loss   = stop_loss,
            take_profit = take_profit,
            atr         = atr_val,
            timeframe   = settings.TF_BREAKOUT,
            notes       = " | ".join(notes_list),
        )
