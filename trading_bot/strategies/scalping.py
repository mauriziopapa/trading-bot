"""
Strategia 4: Scalping Veloce v3
═══════════════════════════════════════════════════════════════
FIX CRITICI:
  ✓ Confidence NON parte più da MIN_CONFIDENCE (falsi positivi)
  ✓ Richiede almeno 2 conferme (volume + VWAP) per generare segnale
  ✓ R:R migliorato (2.2 invece di 1.8) per compensare spread

OTTIMIZZAZIONI AGGRESSIVE:
  ✓ ADX filter: scalpa solo in mercati trending (ADX > 20)
  ✓ Micro-trend confirmation: 3 candele consecutive nella direzione
  ✓ Volume profile: richiede volume crescente sulle ultime 3 candele
  ✓ TP/SL asimmetrici: TP più largo in trend forte
"""

from typing import Optional
import pandas as pd
import numpy as np
from loguru import logger

from trading_bot.strategies.base import BaseStrategy, Signal
from trading_bot.utils import indicators as ind
from trading_bot.config import settings


class ScalpingStrategy(BaseStrategy):
    NAME = "SCALPING"
    MIN_CANDLES = 60

    def __init__(self,
                 ema_fast: int = 9,
                 ema_slow: int = 21,
                 stoch_k: int = 9,
                 stoch_d: int = 3,
                 stoch_oversold: float = 25,
                 stoch_overbought: float = 75,
                 adx_min: float = 20.0):       # NUOVO: ADX filter
        self.ema_fast         = ema_fast
        self.ema_slow         = ema_slow
        self.stoch_k          = stoch_k
        self.stoch_d          = stoch_d
        self.stoch_oversold   = stoch_oversold
        self.stoch_overbought = stoch_overbought
        self.adx_min          = adx_min

    def analyze(self, df: pd.DataFrame, symbol: str, market: str) -> Optional[Signal]:
        if len(df) < self.MIN_CANDLES:
            return None

        base_symbol = symbol.split(":")[0]
        if len(base_symbol) < 2:
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

        # ── NUOVO: ADX filter — scalpa solo in mercati con trend ──────────
        adx_val = self._adx(high, low, close)
        if adx_val < self.adx_min:
            return None

        # ── NUOVO: Volume crescente sulle ultime 3 candele ───────────────
        vol_trend = (float(volume.iloc[-1]) > float(volume.iloc[-2]) > float(volume.iloc[-3]))

        # ── NUOVO: Micro-trend — 3 candele consecutive nella direzione ───
        micro_bull = all(float(close.iloc[-i]) > float(close.iloc[-i-1]) for i in range(1, 4))
        micro_bear = all(float(close.iloc[-i]) < float(close.iloc[-i-1]) for i in range(1, 4))

        side       = None
        confidence = 0.0
        confirms   = 0         # contatore conferme — servono almeno 2
        notes_list = []

        # ── LONG Scalp ────────────────────────────────────────────────────
        if (ef_now > es_now
            and ef_prev < es_prev
            and k_prev < self.stoch_oversold
            and k_now > self.stoch_oversold
            and k_now > d_now):
            side = "buy"
            # FIX CRITICO: confidence parte da un BASE, non da MIN_CONFIDENCE.
            # MIN_CONFIDENCE è la soglia minima per passare il filtro.
            # La confidence deve PARTIRE sotto e salire con le conferme.
            confidence = 50.0    # base bassa — servono conferme per superare MIN_CONF
            notes_list.append("EMA9×EMA21↑ Stoch K risale")

            if last_close > vwap_v:
                confidence += 10
                confirms += 1
                notes_list.append("sopra VWAP")
            if vol_now > 1.5:
                confidence += 9
                confirms += 1
                notes_list.append(f"vol {vol_now:.1f}x")
            if k_now < 40:
                confidence += 5
                notes_list.append("stoch basso")
            if micro_bull:
                confidence += 7
                confirms += 1
                notes_list.append("micro-trend ↑3")
            if vol_trend:
                confidence += 5
                confirms += 1
                notes_list.append("vol crescente")
            if adx_val > 30:
                confidence += 4
                notes_list.append(f"ADX forte {adx_val:.0f}")

        # ── SHORT Scalp ───────────────────────────────────────────────────
        elif (ef_now < es_now
              and ef_prev > es_prev
              and k_prev > self.stoch_overbought
              and k_now < self.stoch_overbought
              and k_now < d_now):
            side = "sell"
            confidence = 50.0
            notes_list.append("EMA9×EMA21↓ Stoch K scende")

            if last_close < vwap_v:
                confidence += 10
                confirms += 1
                notes_list.append("sotto VWAP")
            if vol_now > 1.5:
                confidence += 9
                confirms += 1
                notes_list.append(f"vol {vol_now:.1f}x")
            if k_now > 60:
                confidence += 5
                notes_list.append("stoch alto")
            if micro_bear:
                confidence += 7
                confirms += 1
                notes_list.append("micro-trend ↓3")
            if vol_trend:
                confidence += 5
                confirms += 1
                notes_list.append("vol crescente")
            if adx_val > 30:
                confidence += 4
                notes_list.append(f"ADX forte {adx_val:.0f}")

        if side is None:
            return None

        # FIX: richiedi almeno 2 conferme per evitare falsi positivi
        if confirms < 2:
            logger.debug(f"[SCALPING] {symbol} solo {confirms} conferme — skip")
            return None

        if confidence < self.MIN_CONFIDENCE:
            logger.debug(f"[SCALPING] {symbol} conf={confidence:.0f}% < {self.MIN_CONFIDENCE:.0f}%")
            return None

        # TP/SL per scalping — R:R migliorato
        sl_mult = 0.4
        tp_mult = sl_mult * 2.2    # era 1.8 → 2.2 (più aggressivo)
        sl_dist = atr_val * sl_mult
        tp_dist = atr_val * tp_mult

        # NUOVO: in trend forte (ADX > 30), allarga il TP
        if adx_val > 30:
            tp_dist *= 1.3

        if side == "buy":
            stop_loss   = round(last_close - sl_dist, 6)
            take_profit = round(last_close + tp_dist, 6)
        else:
            stop_loss   = round(last_close + sl_dist, 6)
            take_profit = round(last_close - tp_dist, 6)

        logger.info(f"[SCALPING] {symbol} {market} {side} conf={confidence:.0f}% ADX={adx_val:.0f} | {', '.join(notes_list)}")

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

    def _adx(self, high: pd.Series, low: pd.Series, close: pd.Series,
             period: int = 14) -> float:
        """ADX semplificato — misura forza del trend (non direzione)."""
        try:
            tr = pd.concat([
                high - low,
                (high - close.shift()).abs(),
                (low  - close.shift()).abs(),
            ], axis=1).max(axis=1)
            atr_s = tr.ewm(com=period - 1, min_periods=period).mean()

            up_move   = high - high.shift()
            down_move = low.shift() - low
            plus_dm   = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
            minus_dm  = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

            plus_di  = 100 * pd.Series(plus_dm, index=close.index).ewm(com=period-1, min_periods=period).mean() / atr_s
            minus_di = 100 * pd.Series(minus_dm, index=close.index).ewm(com=period-1, min_periods=period).mean() / atr_s

            dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
            adx = dx.ewm(com=period - 1, min_periods=period).mean()
            return float(adx.iloc[-1]) if not pd.isna(adx.iloc[-1]) else 0.0
        except Exception:
            return 0.0
