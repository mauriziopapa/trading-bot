"""
Centralized configuration — loaded once at startup.
Priorità (dal basso all'alto):
  1. Valori di default hardcoded
  2. Variabili d'ambiente (Railway Variables / .env)
  3. runtime_config.json — aggiornato da /api/config senza riavvio
"""

import os
import json
from dotenv import load_dotenv

load_dotenv()

# ── Runtime config override (da dashboard /api/config) ───────────────────────
# Il file viene scritto dal server FastAPI e letto qui ad ogni import del modulo.
# Per modifiche a runtime senza riavvio, il server usa setattr() direttamente.
_RUNTIME_CONFIG_FILE = os.path.join(
    os.path.dirname(__file__), "..", "dashboard", "runtime_config.json"
)

def _load_runtime_overrides() -> dict:
    try:
        if os.path.exists(_RUNTIME_CONFIG_FILE):
            with open(_RUNTIME_CONFIG_FILE) as f:
                data = json.load(f)
            # Rimuovi le chiavi interne
            return {k: v for k, v in data.items() if not k.startswith("_")}
    except Exception:
        pass
    return {}

_RT = _load_runtime_overrides()


# ── Helpers ──────────────────────────────────────────────────────────────────

def _bool(key: str, default: bool = True) -> bool:
    # Runtime override ha priorità
    if key in _RT:
        return bool(_RT[key])
    return os.getenv(key, str(default)).lower() in ("true", "1", "yes")


def _float(key: str, default: float) -> float:
    if key in _RT:
        try: return float(_RT[key])
        except: pass
    try:
        return float(os.getenv(key, default))
    except (ValueError, TypeError):
        return default


def _int(key: str, default: int) -> int:
    if key in _RT:
        try: return int(_RT[key])
        except: pass
    try:
        return int(os.getenv(key, default))
    except (ValueError, TypeError):
        return default


def _str(key: str, default: str) -> str:
    if key in _RT:
        return str(_RT[key])
    return os.getenv(key, default)


def _list(key: str, default: str) -> list[str]:
    raw = os.getenv(key, default)
    return [s.strip() for s in raw.split(",") if s.strip()]


# ── Dashboard ────────────────────────────────────────────────────────────────
ENABLE_DASHBOARD = _bool("ENABLE_DASHBOARD", True)
DASHBOARD_PORT   = _int("DASHBOARD_PORT", 8080)

# ── Exchange ─────────────────────────────────────────────────────────────────
# ⚠️  NON inserire mai le chiavi reali qui — usare le Railway Variables
BITGET_API_KEY        = os.getenv("BITGET_API_KEY", "")
BITGET_API_SECRET     = os.getenv("BITGET_API_SECRET", "")
BITGET_API_PASSPHRASE = os.getenv("BITGET_API_PASSPHRASE", "")

# ── Mode ─────────────────────────────────────────────────────────────────────
TRADING_MODE  = os.getenv("TRADING_MODE", "paper")   # paper | live
IS_LIVE       = TRADING_MODE == "live"
MARKET_TYPES  = _list("MARKET_TYPES", "spot,futures")

# ── Symbols ──────────────────────────────────────────────────────────────────
SPOT_SYMBOLS    = _list("SPOT_SYMBOLS",    "BTC/USDT,ETH/USDT,SOL/USDT")
FUTURES_SYMBOLS = _list("FUTURES_SYMBOLS", "BTC/USDT:USDT,ETH/USDT:USDT,SOL/USDT:USDT")

# ── Leverage ─────────────────────────────────────────────────────────────────
DEFAULT_LEVERAGE = _int("DEFAULT_LEVERAGE", 5)
MARGIN_MODE      = _str("MARGIN_MODE", "isolated")

# ── Risk Management ──────────────────────────────────────────────────────────
MAX_RISK_PCT          = _float("MAX_RISK_PCT",          3.5)
MAX_POSITIONS_SPOT    = _int("MAX_POSITIONS_SPOT",      4)
MAX_POSITIONS_FUTURES = _int("MAX_POSITIONS_FUTURES",   3)
MAX_DAILY_LOSS_PCT    = _float("MAX_DAILY_LOSS_PCT",    8.0)
MAX_DRAWDOWN_PCT      = _float("MAX_DRAWDOWN_PCT",      15.0)
TAKE_PROFIT_RATIO     = _float("TAKE_PROFIT_RATIO",     2.5)
TRAILING_STOP_PCT     = _float("TRAILING_STOP_PCT",     1.2)

# ── Confidence ───────────────────────────────────────────────────────────────
MIN_CONFIDENCE        = _float("MIN_CONFIDENCE",        65.0)   # soglia minima segnali

# ── Strategies ───────────────────────────────────────────────────────────────
ENABLE_RSI_MACD  = _bool("ENABLE_RSI_MACD",   True)
ENABLE_BOLLINGER = _bool("ENABLE_BOLLINGER",  True)
ENABLE_BREAKOUT  = _bool("ENABLE_BREAKOUT",   True)
ENABLE_SCALPING  = _bool("ENABLE_SCALPING",   True)
ENABLE_EMERGING  = _bool("ENABLE_EMERGING",   True)
SCALPING_SYMBOLS = _list("SCALPING_SYMBOLS",  "BTC/USDT")

# ── Timeframes ───────────────────────────────────────────────────────────────
TF_SWING   = os.getenv("TF_SWING",    "15m")
TF_SCALP   = os.getenv("TF_SCALP",    "1m")
TF_BREAKOUT = os.getenv("TF_BREAKOUT", "1h")

# ── Telegram ─────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN      = os.getenv("TELEGRAM_TOKEN",      "")
TELEGRAM_CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID",    "")
NOTIFY_TRADES       = _bool("NOTIFY_TRADES",       True)
NOTIFY_ERRORS       = _bool("NOTIFY_ERRORS",       True)
NOTIFY_DAILY_REPORT = _bool("NOTIFY_DAILY_REPORT", True)

# ── Database ─────────────────────────────────────────────────────────────────
DATABASE_URL = os.getenv("DATABASE_URL", "")

# ── Logging ──────────────────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
