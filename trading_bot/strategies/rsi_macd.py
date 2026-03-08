"""
Strategia 1: RSI + MACD Trend Following
Timeframe: 15m
Logica:
  LONG  — RSI esce da zona oversold + crossover MACD bullish + EMA200 supporto
  SHORT — RSI esce da zona overbought + crossover MACD bearish + EMA200 resistenza
Confidence: aumenta con forza histogram MACD e distanza RSI dalla zona neutrale
"""

from typing import Optional
import pandas as pd
from loguru import logger

from trading_bot.strategies.base import BaseStrategy, Signal
from trading_bot.utils import indicators as ind
from trading_bot.utils.risk_manager import RiskManager
from trading_bot.config import settings


class RSIMACDStrategy(BaseStrategy):
    NAME = "RSI_MACD"
    MIN_CANDLES = 200    # serve EMA200

    def __init__(self,
                 rsi_period: int = 14,
                 rsi_oversold: float = 32,
                 rsi_overbought: float = 68,
                 rsi_exit_long: float = 45,
                 rsi_exit_short: float = 55):
        self.rsi_period      = rsi_period
        self.rsi_oversold    = rsi_oversold
        self.rsi_overbought  = rsi_overbought
        self.rsi_exit_long   = rsi_exit_long    # RSI deve salire sopra questa soglia
        self.rsi_exit_short  = rsi_exit_short   # RSI deve scendere sotto questa soglia

    def analyze(self, df: pd.DataFrame, symbol: str, market: str) -> Optional[Signal]:
        if len(df) < self.MIN_CANDLES:
            return None

        close = df["close"]

        # Calcola indicatori
        rsi_series              = ind.rsi(close, self.rsi_period)
        macd_line, sig_line, hist = ind.macd(close)
        ema200                  = ind.ema(close, 200)
        ema50                   = ind.ema(close, 50)
        atr_val                 = self._atr_value(df)

        last  = close.iloc[-1]
        prev  = close.iloc[-2]
        rsi   = float(rsi_series.iloc[-1])
        rsi_p = float(rsi_series.iloc[-2])
        macd_now  = float(macd_line.iloc[-1])
        macd_prev = float(macd_line.iloc[-2])
        sig_now   = float(sig_line.iloc[-1])
        sig_prev  = float(sig_line.iloc[-2])
        hist_now  = float(hist.iloc[-1])
        e200      = float(ema200.iloc[-1])
        e50       = float(ema50.iloc[-1])

        side       = None
        confidence = 0.0
        notes_list = []

        # ── LONG Setup ───────────────────────────────────────────────────────
        if (rsi_p < self.rsi_oversold and rsi > self.rsi_exit_long   # RSI risale
            and macd_prev < sig_prev and macd_now > sig_now):         # crossover bullish
            side = "buy"
            confidence = 60.0

            if last > e200:         # trend principale rialzista
                confidence += 10
                notes_list.append("sopra EMA200")
            if last > e50:
                confidence += 5
                notes_list.append("sopra EMA50")
            if hist_now > 0 and abs(hist_now) > abs(float(hist.iloc[-3])):
                confidence += 8     # histogram in espansione
                notes_list.append("MACD histogram in espansione")
            if rsi < 50:
                confidence += 5     # RSI ancora in zona bassa = più spazio
                notes_list.append(f"RSI={rsi:.1f} zona bassa")

        # ── SHORT Setup ──────────────────────────────────────────────────────
        elif (rsi_p > self.rsi_overbought and rsi < self.rsi_exit_short
              and macd_prev > sig_prev and macd_now < sig_now):        # crossover bearish
            side = "sell"
            confidence = 60.0

            if last < e200:
                confidence += 10
                notes_list.append("sotto EMA200")
            if last < e50:
                confidence += 5
                notes_list.append("sotto EMA50")
            if hist_now < 0 and abs(hist_now) > abs(float(hist.iloc[-3])):
                confidence += 8
                notes_list.append("MACD histogram in espansione ribassista")
            if rsi > 50:
                confidence += 5
                notes_list.append(f"RSI={rsi:.1f} zona alta")

        if side is None or confidence < self.MIN_CONFIDENCE:
            return None

        # Calcola SL/TP basati su ATR
        sl_mult = 1.8 if market == "spot" else 1.5   # futures: SL più stretto
        stop_loss, take_profit = self._stops(last, side, atr_val, sl_mult)

        logger.debug(f"[RSI_MACD] {symbol} {market} {side} conf={confidence:.0f}% | {', '.join(notes_list)}")

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
