"""
Strategia 1: RSI + MACD Trend Following v3
═══════════════════════════════════════════════════════════════
FIX:
  ✓ Parentesi esplicite per precedenza operatori nel SHORT setup
  ✓ Base confidence calibrata per evitare falsi positivi

OTTIMIZZAZIONI AGGRESSIVE:
  ✓ EMA stack alignment bonus (EMA9 > EMA21 > EMA50 > EMA200 = trend perfetto)
  ✓ MACD histogram acceleration (cambio velocità histogram)
  ✓ RSI momentum: velocità di cambio RSI come conferma
  ✓ Volume-weighted confidence
"""

from typing import Optional
import pandas as pd
from loguru import logger

from trading_bot.strategies.base import BaseStrategy, Signal
from trading_bot.utils import indicators as ind
from trading_bot.config import settings


class RSIMACDStrategy(BaseStrategy):
    NAME = "RSI_MACD"
    MIN_CANDLES = 50

    def __init__(self,
                 rsi_period: int = 14,
                 rsi_oversold: float = 35,
                 rsi_overbought: float = 65,
                 rsi_exit_long: float = 38,
                 rsi_exit_short: float = 62):
        self.rsi_period      = rsi_period
        self.rsi_oversold    = rsi_oversold
        self.rsi_overbought  = rsi_overbought
        self.rsi_exit_long   = rsi_exit_long
        self.rsi_exit_short  = rsi_exit_short

    def analyze(self, df: pd.DataFrame, symbol: str, market: str) -> Optional[Signal]:
        if len(df) < self.MIN_CANDLES:
            return None

        close = df["close"]
        volume = df["volume"]

        rsi_series              = ind.rsi(close, self.rsi_period)
        macd_line, sig_line, hist = ind.macd(close)
        ema_long_period         = 200 if len(df) >= 200 else 100
        ema200                  = ind.ema(close, ema_long_period)
        ema50                   = ind.ema(close, 50)
        ema21                   = ind.ema(close, 21)
        ema9                    = ind.ema(close, 9)
        atr_val                 = self._atr_value(df)
        vol_ratio               = ind.volume_ratio(volume)

        last  = float(close.iloc[-1])
        rsi   = float(rsi_series.iloc[-1])
        rsi_p = float(rsi_series.iloc[-2])
        rsi_pp = float(rsi_series.iloc[-3])
        macd_now  = float(macd_line.iloc[-1])
        macd_prev = float(macd_line.iloc[-2])
        sig_now   = float(sig_line.iloc[-1])
        sig_prev  = float(sig_line.iloc[-2])
        hist_now  = float(hist.iloc[-1])
        hist_prev = float(hist.iloc[-2])
        hist_pp   = float(hist.iloc[-3])
        e200      = float(ema200.iloc[-1])
        e50       = float(ema50.iloc[-1])
        e21       = float(ema21.iloc[-1])
        e9        = float(ema9.iloc[-1])
        vol_r     = float(vol_ratio.iloc[-1])

        # ── NUOVO: RSI momentum (velocità di cambio) ─────────────────────
        rsi_accel = rsi - rsi_p  # positivo = RSI accelera verso l'alto

        # ── NUOVO: MACD histogram acceleration ───────────────────────────
        hist_accel = hist_now - hist_prev  # positivo = momentum crescente

        side       = None
        confidence = 0.0
        notes_list = []

        # ── LONG Setup ───────────────────────────────────────────────────
        macd_cross_bull = (macd_prev < sig_prev) and (macd_now > sig_now)
        macd_bull_zone  = (macd_now > sig_now) and (hist_now > 0)
        if ((rsi_p < self.rsi_oversold)
            and (rsi > self.rsi_exit_long)
            and (rsi > rsi_p)
            and (macd_cross_bull or macd_bull_zone)):
            side = "buy"
            confidence = 52.0

            # EMA alignment — trend perfetto
            if last > e200:
                confidence += 8
                notes_list.append("sopra EMA200")
            if last > e50:
                confidence += 5
                notes_list.append("sopra EMA50")
            # NUOVO: EMA stack (forte trend rialzista)
            if e9 > e21 > e50:
                confidence += 7
                notes_list.append("EMA stack ↑ (9>21>50)")

            # MACD momentum
            if (hist_now > 0) and (abs(hist_now) > abs(hist_pp)):
                confidence += 8
                notes_list.append("MACD hist espansione")
            if hist_accel > 0:
                confidence += 4
                notes_list.append(f"MACD accel +{hist_accel:.4f}")

            # RSI zona
            if rsi < 50:
                confidence += 5
                notes_list.append(f"RSI={rsi:.1f} zona bassa")
            if rsi_accel > 3:
                confidence += 4
                notes_list.append(f"RSI momentum +{rsi_accel:.1f}")

            # Volume
            if vol_r > 1.5:
                confidence += 5
                notes_list.append(f"vol {vol_r:.1f}x")

        # ── SHORT Setup ──────────────────────────────────────────────────
        # FIX: parentesi esplicite per OR/AND
        elif ((rsi_p > self.rsi_overbought)
              and (rsi < self.rsi_exit_short)
              and (((macd_now < sig_now) and (hist_now < 0))
                   or ((macd_prev > sig_prev) and (macd_now < sig_now)))):
            side = "sell"
            confidence = 52.0

            if last < e200:
                confidence += 8
                notes_list.append("sotto EMA200")
            if last < e50:
                confidence += 5
                notes_list.append("sotto EMA50")
            if e9 < e21 < e50:
                confidence += 7
                notes_list.append("EMA stack ↓ (9<21<50)")

            if (hist_now < 0) and (abs(hist_now) > abs(hist_pp)):
                confidence += 8
                notes_list.append("MACD hist espansione ↓")
            if hist_accel < 0:
                confidence += 4

            if rsi > 50:
                confidence += 5
                notes_list.append(f"RSI={rsi:.1f} zona alta")
            if rsi_accel < -3:
                confidence += 4
                notes_list.append(f"RSI momentum {rsi_accel:.1f}")

            if vol_r > 1.5:
                confidence += 5
                notes_list.append(f"vol {vol_r:.1f}x")

        if side is None:
            return None
        if confidence < self.MIN_CONFIDENCE:
            logger.debug(f"[RSI_MACD] {symbol} conf={confidence:.0f}% < {self.MIN_CONFIDENCE:.0f}%")
            return None

        sl_mult = 1.8 if market == "spot" else 1.5
        stop_loss, take_profit = self._stops(last, side, atr_val, sl_mult)

        logger.info(f"[RSI_MACD] SEGNALE {symbol} {market} {side} conf={confidence:.0f}% | {', '.join(notes_list)}")

        return Signal(
            strategy    = self.NAME,
            symbol      = symbol,
            market      = market,
            side        = side,
            confidence  = min(confidence, 100),
            entry       = last,
            stop_loss   = stop_loss,
            take_profit = take_profit,
            atr         = atr_val,
            timeframe   = settings.TF_SWING,
            notes       = " | ".join(notes_list),
        )

    def _stops(self, entry: float, side: str, atr_val: float, sl_mult: float
               ) -> tuple[float, float]:
        sl_dist = atr_val * sl_mult
        tp_dist = sl_dist * settings.TAKE_PROFIT_RATIO
        if side == "buy":
            return round(entry - sl_dist, 6), round(entry + tp_dist, 6)
        return round(entry + sl_dist, 6), round(entry - tp_dist, 6)
