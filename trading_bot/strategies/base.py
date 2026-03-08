"""Base class per tutte le strategie."""

from dataclasses import dataclass, field
from typing import Optional
import pandas as pd


@dataclass
class Signal:
    strategy:   str
    symbol:     str
    market:     str           # spot | futures
    side:       str           # buy | sell
    confidence: float         # 0–100
    entry:      float
    stop_loss:  float
    take_profit: float
    atr:        float
    timeframe:  str
    notes:      str = ""

    @property
    def is_long(self) -> bool:
        return self.side == "buy"

    @property
    def risk_reward(self) -> float:
        if self.side == "buy":
            reward = self.take_profit - self.entry
            risk   = self.entry - self.stop_loss
        else:
            reward = self.entry - self.take_profit
            risk   = self.stop_loss - self.entry
        return reward / risk if risk > 0 else 0


class BaseStrategy:
    NAME = "base"
    MIN_CANDLES = 50
    MIN_CONFIDENCE = 55.0    # soglia minima per generare segnale

    def analyze(self, df: pd.DataFrame, symbol: str, market: str) -> Optional[Signal]:
        """Override nelle sottoclassi. Ritorna Signal o None."""
        raise NotImplementedError

    def _atr_value(self, df: pd.DataFrame) -> float:
        from trading_bot.utils.indicators import atr
        return float(atr(df["high"], df["low"], df["close"]).iloc[-1])
