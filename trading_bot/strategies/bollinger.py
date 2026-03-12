"""
Strategia 2: Bollinger Bands Mean Reversion
Timeframe: 15m
Logica:
  LONG  — Prezzo tocca BB lower + rimbalzo confermato + RSI oversold + volume spike
  SHORT — Prezzo tocca BB upper + rejection confermata + RSI overbought + volume spike
Filtro squeeze: segnale valido solo se BB larghezza > soglia (evita mercati laterali piatti)
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
                 rsi_oversold: float = 42,        # era 38 — più facile entrare LONG
                 rsi_overbought: float = 58,        # era 62
                 min_bandwidth_pct: float = 1.2):   # era 2.0 — abbassato per mercati meno volatili
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

        bb_upper, bb_mid, bb_lower = ind.bollinger_bands(close, self.bb_period, self.bb_std)
        rsi_series = ind.rsi(close)
        vol_ratio  = ind.volume_ratio(df["volume"])
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
        vol_r       = float(vol_ratio.iloc[-1])

        # Bandwidth filter — evita falsi segnali in squeeze
        bandwidth_pct = (upper - lower) / mid * 100
        if bandwidth_pct < self.min_bandwidth_pct:
            return None

        side       = None
        confidence = 0.0
        notes_list = []

        # ── LONG — rimbalzo da BB lower ──────────────────────────────────────
        if (prev_low <= lower * 1.005               # tocco banda inferiore (era 1.002)
            and last_close > prev_close             # candela di rimbalzo
            and rsi < self.rsi_oversold):
            side = "buy"
            confidence = 62.0
            notes_list.append(f"rimbalzo BB lower (bw={bandwidth_pct:.1f}%)")

            if rsi < 30:
                confidence += 10
                notes_list.append(f"RSI estremo {rsi:.1f}")
            if vol_r > 1.8:
                confidence += 8
                notes_list.append(f"volume spike {vol_r:.1f}x")
            if last_close > lower:
                confidence += 5   # chiusura sopra la banda = pin bar
                notes_list.append("chiusura sopra BB lower")

        # ── SHORT — rejection da BB upper ────────────────────────────────────
        elif (prev_high >= upper * 0.995             # era 0.998
              and last_close < prev_close
              and rsi > self.rsi_overbought):
            side = "sell"
            confidence = 62.0
            notes_list.append(f"rejection BB upper (bw={bandwidth_pct:.1f}%)")

            if rsi > 70:
                confidence += 10
                notes_list.append(f"RSI estremo {rsi:.1f}")
            if vol_r > 1.8:
                confidence += 8
                notes_list.append(f"volume spike {vol_r:.1f}x")
            if last_close < upper:
                confidence += 5
                notes_list.append("chiusura sotto BB upper")

        if side is None:
            return None
        if confidence < self.MIN_CONFIDENCE:
            logger.debug(f"[BOLLINGER] {symbol} conf={confidence:.0f}% < {self.MIN_CONFIDENCE:.0f}% — scartato")
            return None

        # TP al mid band (target naturale del mean reversion)
        sl_dist = atr_val * 1.6
        if side == "buy":
            stop_loss   = round(last_close - sl_dist, 6)
            take_profit = round(min(mid, last_close + sl_dist * settings.TAKE_PROFIT_RATIO), 6)
        else:
            stop_loss   = round(last_close + sl_dist, 6)
            take_profit = round(max(mid, last_close - sl_dist * settings.TAKE_PROFIT_RATIO), 6)

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
