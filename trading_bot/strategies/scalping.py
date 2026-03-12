"""
Strategia 4: Scalping Veloce 1m / 5m
Solo su simboli ultra-liquidi (BTC/USDT default).
Logica:
  LONG  — EMA9 > EMA21, Stochastic esce da oversold, VWAP supporto, volume spike
  SHORT — EMA9 < EMA21, Stochastic esce da overbought, VWAP resistenza, volume spike
TP/SL molto stretti (0.4x ATR), alta frequenza.
"""

from typing import Optional
import pandas as pd
from loguru import logger

from trading_bot.strategies.base import BaseStrategy, Signal
from trading_bot.utils import indicators as ind
from trading_bot.config import settings


class ScalpingStrategy(BaseStrategy):
    NAME = "SCALPING"
    MIN_CANDLES = 60
    # FIX #2: rimossa MIN_CONFIDENCE = 60.0 — class var hardcoded sovrascriveva
    # la @property di BaseStrategy che legge dal DB (bot_config).
    # Ora MIN_CONFIDENCE è ereditata correttamente e sempre aggiornata dal DB.

    def __init__(self,
                 ema_fast: int = 9,
                 ema_slow: int = 21,
                 stoch_k: int = 9,
                 stoch_d: int = 3,
                 stoch_oversold: float = 25,
                 stoch_overbought: float = 75):
        self.ema_fast         = ema_fast
        self.ema_slow         = ema_slow
        self.stoch_k          = stoch_k
        self.stoch_d          = stoch_d
        self.stoch_oversold   = stoch_oversold
        self.stoch_overbought = stoch_overbought

    def analyze(self, df: pd.DataFrame, symbol: str, market: str) -> Optional[Signal]:
        if len(df) < self.MIN_CANDLES:
            return None

        # Solo per simboli scalping configurati
        base_symbol = symbol.split(":")[0]   # rimuove ':USDT' dai futures
        if base_symbol not in settings.SCALPING_SYMBOLS:
            return None

        close  = df["close"]
        high   = df["high"]
        low    = df["low"]
        volume = df["volume"]

        ema_f   = ind.ema(close, self.ema_fast)
        ema_s   = ind.ema(close, self.ema_slow)
        k, d    = ind.stochastic(high, low, close, self.stoch_k, self.stoch_d)
        vwap_s  = ind.vwap(high, low, close, volume)
        vol_r   = ind.volume_ratio(volume)
        atr_val = self._atr_value(df)

        last_close = float(close.iloc[-1])
        ef_now  = float(ema_f.iloc[-1])
        ef_prev = float(ema_f.iloc[-2])
        es_now  = float(ema_s.iloc[-1])
        es_prev = float(ema_s.iloc[-2])
        k_now   = float(k.iloc[-1])
        k_prev  = float(k.iloc[-2])
        d_now   = float(d.iloc[-1])
        vwap_v  = float(vwap_s.iloc[-1])
        vol_now = float(vol_r.iloc[-1])

        side       = None
        confidence = 0.0
        notes_list = []

        # ── LONG Scalp ────────────────────────────────────────────────────────
        if (ef_now > es_now                           # trend EMA bullish
            and ef_prev < es_prev                     # crossover avvenuto
            and k_prev < self.stoch_oversold          # stoch era oversold
            and k_now > self.stoch_oversold           # stoch sale
            and k_now > d_now):                       # K > D
            side = "buy"
            confidence = self.MIN_CONFIDENCE          # @property → legge dal DB
            notes_list.append("EMA9 cross EMA21 bullish")
            notes_list.append(f"Stoch K={k_now:.1f} risale")

            if last_close > vwap_v:
                confidence += 8
                notes_list.append("prezzo sopra VWAP")
            if vol_now > 1.5:
                confidence += 7
                notes_list.append(f"vol {vol_now:.1f}x")
            if k_now < 40:
                confidence += 5
                notes_list.append("stoch zona bassa")

        # ── SHORT Scalp ───────────────────────────────────────────────────────
        elif (ef_now < es_now
              and ef_prev > es_prev
              and k_prev > self.stoch_overbought
              and k_now < self.stoch_overbought
              and k_now < d_now):
            side = "sell"
            confidence = self.MIN_CONFIDENCE          # @property → legge dal DB
            notes_list.append("EMA9 cross EMA21 bearish")
            notes_list.append(f"Stoch K={k_now:.1f} scende")

            if last_close < vwap_v:
                confidence += 8
                notes_list.append("prezzo sotto VWAP")
            if vol_now > 1.5:
                confidence += 7
                notes_list.append(f"vol {vol_now:.1f}x")
            if k_now > 60:
                confidence += 5
                notes_list.append("stoch zona alta")

        if side is None or confidence < self.MIN_CONFIDENCE:
            return None

        # SL/TP strettissimi per scalping
        sl_mult = 0.4
        tp_mult = sl_mult * 1.8    # R:R ridotto ma alta frequenza
        sl_dist = atr_val * sl_mult
        tp_dist = atr_val * tp_mult

        if side == "buy":
            stop_loss   = round(last_close - sl_dist, 6)
            take_profit = round(last_close + tp_dist, 6)
        else:
            stop_loss   = round(last_close + sl_dist, 6)
            take_profit = round(last_close - tp_dist, 6)

        logger.debug(f"[SCALPING] {symbol} {market} {side} conf={confidence:.0f}% | {', '.join(notes_list)}")

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
            timeframe   = settings.TF_SCALP,
            notes       = " | ".join(notes_list),
        )
