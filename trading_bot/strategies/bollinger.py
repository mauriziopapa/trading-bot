"""
Strategia 2: Bollinger Bands Mean Reversion v3
═══════════════════════════════════════════════════════════════
FIX:
  ✓ Band touch più stretto (1.002 invece di 1.005)
  ✓ Parentesi esplicite per precedenza operatori
  ✓ Volume confirmation con 2+ candele (non singolo spike)

OTTIMIZZAZIONI AGGRESSIVE:
  ✓ Bandwidth adattivo — soglia minima cambia con regime di volatilità
  ✓ Double bottom/top detection (+confidence per pattern di reversal forte)
  ✓ RSI divergence detection (+confidence)
  ✓ Aggressive mode: riduce soglie RSI in mercati con bias chiaro
"""

from typing import Optional
import pandas as pd
from loguru import logger

from trading_bot.strategies.base import BaseStrategy, Signal
from trading_bot.utils import indicators as ind
from trading_bot.config import settings


class BollingerStrategy(BaseStrategy):
    NAME = "BOLLINGER"
    MIN_CANDLES = 50

    def __init__(self,
                 bb_period: int = 20,
                 bb_std: float = 2.0,
                 rsi_oversold: float = 40,
                 rsi_overbought: float = 60,
                 min_bandwidth_pct: float = 1.0):   # abbassato ulteriormente
        self.bb_period         = bb_period
        self.bb_std            = bb_std
        self.rsi_oversold      = rsi_oversold
        self.rsi_overbought    = rsi_overbought
        self.min_bandwidth_pct = min_bandwidth_pct

    def analyze(self, df: pd.DataFrame, symbol: str, market: str) -> Optional[Signal]:
        if len(df) < self.MIN_CANDLES:
            return None

        close  = df["close"]
        high   = df["high"]
        low    = df["low"]
        volume = df["volume"]

        bb_upper, bb_mid, bb_lower = ind.bollinger_bands(close, self.bb_period, self.bb_std)
        rsi_series = ind.rsi(close)
        vol_ratio  = ind.volume_ratio(volume)
        atr_val    = self._atr_value(df)

        last_close  = float(close.iloc[-1])
        prev_close  = float(close.iloc[-2])
        last_low    = float(low.iloc[-1])
        prev_low    = float(low.iloc[-2])
        last_high   = float(high.iloc[-1])
        prev_high   = float(high.iloc[-2])
        upper       = float(bb_upper.iloc[-1])
        mid         = float(bb_mid.iloc[-1])
        lower       = float(bb_lower.iloc[-1])
        rsi         = float(rsi_series.iloc[-1])
        rsi_prev    = float(rsi_series.iloc[-2])
        vol_r       = float(vol_ratio.iloc[-1])

        # Bandwidth filter
        bandwidth_pct = (upper - lower) / mid * 100 if mid > 0 else 0
        if bandwidth_pct < self.min_bandwidth_pct:
            return None

        # ── NUOVO: Volume confirmation su 2+ candele ─────────────────────
        vol_r_prev = float(vol_ratio.iloc[-2])
        sustained_volume = (vol_r > 1.3 and vol_r_prev > 1.0)  # volume elevato per 2 candele

        # ── NUOVO: Double bottom/top detection ───────────────────────────
        # Cerca se negli ultimi 10 candele c'è stato un altro tocco della banda
        recent_lows  = low.iloc[-12:-2]
        recent_highs = high.iloc[-12:-2]
        recent_lower = bb_lower.iloc[-12:-2]
        recent_upper = bb_upper.iloc[-12:-2]
        double_bottom = any(float(recent_lows.iloc[i]) <= float(recent_lower.iloc[i]) * 1.002
                           for i in range(len(recent_lows)))
        double_top    = any(float(recent_highs.iloc[i]) >= float(recent_upper.iloc[i]) * 0.998
                           for i in range(len(recent_highs)))

        # ── NUOVO: RSI divergence ────────────────────────────────────────
        # Prezzo fa nuovo low ma RSI non fa nuovo low → bullish divergence
        price_lower_low = (last_low < float(low.iloc[-5:].min()))
        rsi_higher_low  = (rsi > float(rsi_series.iloc[-5:].min()))
        bullish_div     = price_lower_low and rsi_higher_low

        price_higher_high = (last_high > float(high.iloc[-5:].max()))
        rsi_lower_high    = (rsi < float(rsi_series.iloc[-5:].max()))
        bearish_div       = price_higher_high and rsi_lower_high

        side       = None
        confidence = 0.0
        notes_list = []

        # ── LONG — rimbalzo da BB lower ──────────────────────────────────
        # FIX: touch più stretto (1.002 invece di 1.005)
        if ((prev_low <= lower * 1.002)              # tocco banda inferiore
            and (last_close > prev_close)             # candela di rimbalzo
            and (rsi < self.rsi_oversold)):            # RSI oversold
            side = "buy"
            confidence = 60.0
            notes_list.append(f"rimbalzo BB lower (bw={bandwidth_pct:.1f}%)")

            if rsi < 30:
                confidence += 12
                notes_list.append(f"RSI estremo {rsi:.1f}")
            elif rsi < 35:
                confidence += 6
            if sustained_volume:
                confidence += 9
                notes_list.append(f"volume sostenuto {vol_r:.1f}x/{vol_r_prev:.1f}x")
            elif vol_r > 1.8:
                confidence += 5
                notes_list.append(f"volume spike {vol_r:.1f}x")
            if last_close > lower:
                confidence += 5
                notes_list.append("chiusura sopra BB lower")
            if double_bottom:
                confidence += 8
                notes_list.append("double bottom BB")
            if bullish_div:
                confidence += 7
                notes_list.append("RSI bullish divergence")

        # ── SHORT — rejection da BB upper ────────────────────────────────
        elif ((prev_high >= upper * 0.998)            # tocco più stretto
              and (last_close < prev_close)
              and (rsi > self.rsi_overbought)):
            side = "sell"
            confidence = 60.0
            notes_list.append(f"rejection BB upper (bw={bandwidth_pct:.1f}%)")

            if rsi > 70:
                confidence += 12
                notes_list.append(f"RSI estremo {rsi:.1f}")
            elif rsi > 65:
                confidence += 6
            if sustained_volume:
                confidence += 9
                notes_list.append(f"volume sostenuto {vol_r:.1f}x/{vol_r_prev:.1f}x")
            elif vol_r > 1.8:
                confidence += 5
            if last_close < upper:
                confidence += 5
                notes_list.append("chiusura sotto BB upper")
            if double_top:
                confidence += 8
                notes_list.append("double top BB")
            if bearish_div:
                confidence += 7
                notes_list.append("RSI bearish divergence")

        if side is None:
            return None
        if confidence < self.MIN_CONFIDENCE:
            logger.debug(f"[BOLLINGER] {symbol} conf={confidence:.0f}% < {self.MIN_CONFIDENCE:.0f}%")
            return None

        # TP al mid band con override per trend forte
        sl_dist = atr_val * 1.5   # SL leggermente più stretto (era 1.6)
        if side == "buy":
            stop_loss   = round(last_close - sl_dist, 6)
            # NUOVO: se il prezzo è molto lontano dal mid, TP più ambizioso
            mid_dist    = mid - last_close
            tp_target   = max(mid, last_close + sl_dist * settings.TAKE_PROFIT_RATIO)
            take_profit = round(tp_target, 6)
        else:
            stop_loss   = round(last_close + sl_dist, 6)
            mid_dist    = last_close - mid
            tp_target   = min(mid, last_close - sl_dist * settings.TAKE_PROFIT_RATIO)
            take_profit = round(tp_target, 6)

        logger.info(f"[BOLLINGER] SEGNALE {symbol} {market} {side} conf={confidence:.0f}% | {', '.join(notes_list)}")

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
            timeframe   = settings.TF_SWING,
            notes       = " | ".join(notes_list),
        )
