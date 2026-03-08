"""
Strategia 3: Breakout su Volumi
Timeframe: 1h
Logica:
  LONG  — Rottura del massimo delle ultime N candele con volume > 2x media
  SHORT — Rottura del minimo delle ultime N candele con volume > 2x media
Filtro falsi breakout:
  - Chiusura candela > livello rotto (non solo wick)
  - ATR in espansione (volatilità crescente)
  - Nessuna resistenza/supporto maggiore nelle prossimità
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
                 lookback: int = 20,          # periodi per il range
                 vol_multiplier: float = 2.0,  # volume spike richiesto
                 atr_expansion: float = 1.2):  # ATR > media * questo fattore
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

        # Definisce il range escludendo l'ultima candela
        range_high = float(high.iloc[-self.lookback - 1:-1].max())
        range_low  = float(low.iloc[-self.lookback - 1:-1].min())

        last_close  = float(close.iloc[-1])
        last_high   = float(high.iloc[-1])
        last_low    = float(low.iloc[-1])
        last_vol    = float(volume.iloc[-1])
        avg_vol     = float(volume.iloc[-self.lookback - 1:-1].mean())

        atr_val     = self._atr_value(df)
        atr_series  = ind.atr(high, low, close)
        atr_avg     = float(atr_series.iloc[-10:].mean())  # media ultime 10

        vol_r = last_vol / avg_vol if avg_vol > 0 else 0

        # Filtro ATR: breakout valido solo se volatilità in espansione
        atr_expanding = atr_val >= atr_avg * self.atr_expansion

        side       = None
        confidence = 0.0
        notes_list = []

        # ── LONG Breakout ─────────────────────────────────────────────────────
        if (last_close > range_high                         # chiusura sopra il range
            and vol_r >= self.vol_multiplier                # volume spike
            and atr_expanding):                             # ATR in espansione
            side = "buy"
            confidence = 65.0
            notes_list.append(f"breakout {range_high:.4f} (lookback {self.lookback})")
            notes_list.append(f"volume {vol_r:.1f}x avg")

            # Bonus: breakout netto (corpo candela interamente sopra)
            open_price = float(df["open"].iloc[-1])
            if open_price > range_high:
                confidence += 10
                notes_list.append("gap up confermato")

            if vol_r >= self.vol_multiplier * 1.5:
                confidence += 8    # volume eccezionale
                notes_list.append("volume eccezionale")

            breakout_pct = (last_close - range_high) / range_high * 100
            if breakout_pct > 0.3:
                confidence += 5
                notes_list.append(f"rottura netta +{breakout_pct:.1f}%")

        # ── SHORT Breakout ────────────────────────────────────────────────────
        elif (last_close < range_low
              and vol_r >= self.vol_multiplier
              and atr_expanding):
            side = "sell"
            confidence = 65.0
            notes_list.append(f"breakout sotto {range_low:.4f}")
            notes_list.append(f"volume {vol_r:.1f}x avg")

            open_price = float(df["open"].iloc[-1])
            if open_price < range_low:
                confidence += 10
                notes_list.append("gap down confermato")

            if vol_r >= self.vol_multiplier * 1.5:
                confidence += 8
                notes_list.append("volume eccezionale")

            breakout_pct = (range_low - last_close) / range_low * 100
            if breakout_pct > 0.3:
                confidence += 5
                notes_list.append(f"rottura netta -{breakout_pct:.1f}%")

        if side is None or confidence < self.MIN_CONFIDENCE:
            return None

        # SL appena dentro il range rotto, TP proiettato
        if side == "buy":
            stop_loss   = round(range_high - atr_val * 0.5, 6)   # appena sotto il breakout
            take_profit = round(last_close + (last_close - stop_loss) * settings.TAKE_PROFIT_RATIO, 6)
        else:
            stop_loss   = round(range_low + atr_val * 0.5, 6)
            take_profit = round(last_close - (stop_loss - last_close) * settings.TAKE_PROFIT_RATIO, 6)

        logger.debug(f"[BREAKOUT] {symbol} {market} {side} conf={confidence:.0f}% | {', '.join(notes_list)}")

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
