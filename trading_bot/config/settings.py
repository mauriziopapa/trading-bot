"""
Centralized configuration — loaded once at startup.
All values read from environment variables with safe defaults.
"""

import os
from dotenv import load_dotenv

load_dotenv()


def _bool(key: str, default: bool = True) -> bool:
    return os.getenv(key, str(default)).lower() in ("true", "1", "yes")


def _float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, default))
    except (ValueError, TypeError):
        return default


def _int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, default))
    except (ValueError, TypeError):
        return default


def _list(key: str, default: str) -> list[str]:
    raw = os.getenv(key, default)
    return [s.strip() for s in raw.split(",") if s.strip()]

# ─── Dashboard ──────────────────────────────────────────────────────────────
ENABLE_DASHBOARD = _bool("ENABLE_DASHBOARD", True)
DASHBOARD_PORT   = _int("DASHBOARD_PORT", 8080)

# ─── Exchange ────────────────────────────────────────────────────────────────
BITGET_API_KEY      = os.getenv("BITGET_API_KEY", "bg_39ebbad09373e4b960361284e67595b7")
BITGET_SECRET       = os.getenv("BITGET_SECRET", "06d4df69a93f9fbb5045dac509a53a2634f1dafa7216796fb42dfffbf55f7fa5")
BITGET_PASSPHRASE   = os.getenv("BITGET_PASSPHRASE", "Tommaso14072024")

# ─── Mode ────────────────────────────────────────────────────────────────────
TRADING_MODE        = os.getenv("TRADING_MODE", "paper")       # paper | live
IS_LIVE             = TRADING_MODE == "live"
MARKET_TYPES        = _list("MARKET_TYPES", "spot,futures")     # spot | futures | both

# ─── Symbols ─────────────────────────────────────────────────────────────────
SPOT_SYMBOLS        = _list("SPOT_SYMBOLS",    "BTC/USDT,ETH/USDT,SOL/USDT")
FUTURES_SYMBOLS     = _list("FUTURES_SYMBOLS", "BTC/USDT:USDT,ETH/USDT:USDT,SOL/USDT:USDT")

# ─── Leverage ────────────────────────────────────────────────────────────────
DEFAULT_LEVERAGE    = _int("DEFAULT_LEVERAGE", 5)
MARGIN_MODE         = os.getenv("MARGIN_MODE", "isolated")

# ─── Risk Management ─────────────────────────────────────────────────────────
MAX_RISK_PCT        = _float("MAX_RISK_PCT",        3.5)
MAX_POSITIONS_SPOT  = _int("MAX_POSITIONS_SPOT",    4)
MAX_POSITIONS_FUTURES = _int("MAX_POSITIONS_FUTURES", 3)
MAX_DAILY_LOSS_PCT  = _float("MAX_DAILY_LOSS_PCT",  8.0)
MAX_DRAWDOWN_PCT    = _float("MAX_DRAWDOWN_PCT",    15.0)
TAKE_PROFIT_RATIO   = _float("TAKE_PROFIT_RATIO",   2.5)   # TP = SL * ratio
TRAILING_STOP_PCT   = _float("TRAILING_STOP_PCT",   1.2)   # % trailing

# ─── Strategies ──────────────────────────────────────────────────────────────
ENABLE_RSI_MACD     = _bool("ENABLE_RSI_MACD",   True)
ENABLE_BOLLINGER    = _bool("ENABLE_BOLLINGER",  True)
ENABLE_BREAKOUT     = _bool("ENABLE_BREAKOUT",   True)
ENABLE_SCALPING     = _bool("ENABLE_SCALPING",   True)
SCALPING_SYMBOLS    = _list("SCALPING_SYMBOLS",  "BTC/USDT")

# ─── Timeframes ──────────────────────────────────────────────────────────────
TF_SWING            = os.getenv("TF_SWING",    "15m")
TF_SCALP            = os.getenv("TF_SCALP",    "1m")
TF_BREAKOUT         = os.getenv("TF_BREAKOUT", "1h")

# ─── Telegram ────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN      = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID", "")
NOTIFY_TRADES       = _bool("NOTIFY_TRADES",       True)
NOTIFY_ERRORS       = _bool("NOTIFY_ERRORS",       True)
NOTIFY_DAILY_REPORT = _bool("NOTIFY_DAILY_REPORT", True)

# ─── Database ────────────────────────────────────────────────────────────────
DATABASE_URL        = os.getenv("DATABASE_URL", "")

# ─── Logging ─────────────────────────────────────────────────────────────────
LOG_LEVEL           = os.getenv("LOG_LEVEL", "INFO")
