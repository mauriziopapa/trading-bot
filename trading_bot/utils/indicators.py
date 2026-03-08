"""
Indicatori Tecnici — implementazione pura NumPy/Pandas
Nessuna dipendenza da TA-Lib per massima compatibilità cloud.
"""

import numpy as np
import pandas as pd


def ohlcv_to_df(candles: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(candles)
    df = df.astype({"open": float, "high": float, "low": float, "close": float, "volume": float})
    return df


# ─── Trend Indicators ────────────────────────────────────────────────────────

def rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain  = delta.clip(lower=0).ewm(com=period - 1, min_periods=period).mean()
    loss  = (-delta.clip(upper=0)).ewm(com=period - 1, min_periods=period).mean()
    rs    = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def macd(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9
         ) -> tuple[pd.Series, pd.Series, pd.Series]:
    ema_fast   = close.ewm(span=fast, adjust=False).mean()
    ema_slow   = close.ewm(span=slow, adjust=False).mean()
    macd_line  = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram  = macd_line - signal_line
    return macd_line, signal_line, histogram


def ema(close: pd.Series, period: int) -> pd.Series:
    return close.ewm(span=period, adjust=False).mean()


def sma(close: pd.Series, period: int) -> pd.Series:
    return close.rolling(period).mean()


# ─── Volatility Indicators ───────────────────────────────────────────────────

def bollinger_bands(close: pd.Series, period: int = 20, std_dev: float = 2.0
                    ) -> tuple[pd.Series, pd.Series, pd.Series]:
    mid   = close.rolling(period).mean()
    sigma = close.rolling(period).std()
    upper = mid + std_dev * sigma
    lower = mid - std_dev * sigma
    return upper, mid, lower


def atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low  - close.shift()).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(com=period - 1, min_periods=period).mean()


def keltner_channels(high: pd.Series, low: pd.Series, close: pd.Series,
                     ema_period: int = 20, atr_period: int = 10, mult: float = 2.0
                     ) -> tuple[pd.Series, pd.Series]:
    mid   = ema(close, ema_period)
    _atr  = atr(high, low, close, atr_period)
    return mid + mult * _atr, mid - mult * _atr


# ─── Volume Indicators ───────────────────────────────────────────────────────

def volume_sma(volume: pd.Series, period: int = 20) -> pd.Series:
    return volume.rolling(period).mean()


def volume_ratio(volume: pd.Series, period: int = 20) -> pd.Series:
    """Volume corrente / media mobile — ratio > 2 = spike significativo."""
    avg = volume_sma(volume, period)
    return volume / avg.replace(0, np.nan)


def obv(close: pd.Series, volume: pd.Series) -> pd.Series:
    """On-Balance Volume."""
    direction = np.sign(close.diff()).fillna(0)
    return (direction * volume).cumsum()


def vwap(high: pd.Series, low: pd.Series, close: pd.Series, volume: pd.Series) -> pd.Series:
    typical = (high + low + close) / 3
    return (typical * volume).cumsum() / volume.cumsum()


# ─── Momentum ────────────────────────────────────────────────────────────────

def stochastic(high: pd.Series, low: pd.Series, close: pd.Series,
               k_period: int = 14, d_period: int = 3) -> tuple[pd.Series, pd.Series]:
    lowest  = low.rolling(k_period).min()
    highest = high.rolling(k_period).max()
    k = 100 * (close - lowest) / (highest - lowest).replace(0, np.nan)
    d = k.rolling(d_period).mean()
    return k, d


def williams_r(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    highest = high.rolling(period).max()
    lowest  = low.rolling(period).min()
    return -100 * (highest - close) / (highest - lowest).replace(0, np.nan)


# ─── Breakout Helpers ────────────────────────────────────────────────────────

def donchian(high: pd.Series, low: pd.Series, period: int = 20
             ) -> tuple[pd.Series, pd.Series]:
    return high.rolling(period).max(), low.rolling(period).min()


def squeeze_momentum(high: pd.Series, low: pd.Series, close: pd.Series) -> pd.Series:
    """
    Lazybear Squeeze Momentum semplificato.
    Positivo = momentum rialzista, negativo = ribassista.
    """
    bb_upper, bb_mid, bb_lower = bollinger_bands(close)
    kc_upper, kc_lower = keltner_channels(high, low, close)
    # squeeze = BB dentro KC
    in_squeeze = (bb_lower > kc_lower) & (bb_upper < kc_upper)
    momentum = close - ((high.rolling(20).max() + low.rolling(20).min()) / 2
                        + sma(close, 20)) / 2
    return momentum.where(in_squeeze, other=0)
