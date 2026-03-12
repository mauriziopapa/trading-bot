"""
settings.py — Configurazione centralizzata con DynamicSettings singleton.

Priorità (dal basso all'alto):
  1. Default hardcoded
  2. Variabili d'ambiente Railway (env vars)
  3. runtime_config.json — scritto da /api/config, riletto ogni 5s

ARCHITETTURA:
  - NIENTE variabili modulo statiche (vengono lette una sola volta all'import).
  - Tutto passa per l'oggetto `settings` singleton.
  - Ogni `settings.MAX_RISK_PCT` rilegge il file se il TTL e' scaduto.
  - Il server FastAPI fa `settings.set_many(cfg)` per cambio immediato
    in-process, senza aspettare il prossimo file refresh.
"""

from __future__ import annotations

import os
import json
import time
from typing import Any
from dotenv import load_dotenv

load_dotenv()

_HERE = os.path.dirname(__file__)
_RUNTIME_CONFIG_FILE = os.path.normpath(
    os.path.join(_HERE, "..", "dashboard", "runtime_config.json")
)


class DynamicSettings:
    """
    Singleton configurazione. I campi runtime vengono riletti da
    runtime_config.json ogni REFRESH_INTERVAL secondi.
    """

    REFRESH_INTERVAL = 5.0

    _DEFAULTS: dict[str, Any] = {
        "MAX_RISK_PCT":          3.5,
        "DEFAULT_LEVERAGE":      5,
        "MAX_DAILY_LOSS_PCT":    8.0,
        "MAX_DRAWDOWN_PCT":      15.0,
        "TAKE_PROFIT_RATIO":     2.5,
        "TRAILING_STOP_PCT":     1.2,
        "MIN_CONFIDENCE":        65.0,
        "MAX_POSITIONS_SPOT":    4,
        "MAX_POSITIONS_FUTURES": 3,
        "MARGIN_MODE":           "isolated",
        "ENABLE_RSI_MACD":       True,
        "ENABLE_BOLLINGER":      True,
        "ENABLE_BREAKOUT":       True,
        "ENABLE_SCALPING":       True,
        "ENABLE_EMERGING":       True,
    }

    _RUNTIME_FIELDS = set(_DEFAULTS.keys())

    def __init__(self):
        self._file_cache: dict[str, Any] = {}
        self._cache_ts: float = 0.0
        self._mem: dict[str, Any] = {}

    # ------------------------------------------------------------------ core

    def _refresh(self):
        now = time.monotonic()
        if now - self._cache_ts < self.REFRESH_INTERVAL:
            return
        try:
            if os.path.exists(_RUNTIME_CONFIG_FILE):
                with open(_RUNTIME_CONFIG_FILE) as f:
                    data = json.load(f)
                self._file_cache = {k: v for k, v in data.items()
                                    if not k.startswith("_")}
            else:
                self._file_cache = {}
        except Exception:
            self._file_cache = {}
        self._cache_ts = now

    def _raw(self, key: str) -> Any:
        if key in self._mem:
            return self._mem[key]
        self._refresh()
        if key in self._file_cache:
            return self._file_cache[key]
        env = os.getenv(key)
        if env is not None:
            return env
        return self._DEFAULTS.get(key)

    def _cast(self, key: str, raw: Any) -> Any:
        if raw is None:
            return self._DEFAULTS.get(key)
        default = self._DEFAULTS.get(key)
        if default is None:
            return raw
        try:
            if isinstance(default, bool):
                if isinstance(raw, bool):
                    return raw
                return str(raw).lower() in ("true", "1", "yes")
            if isinstance(default, int):
                return int(float(raw))
            if isinstance(default, float):
                return float(raw)
        except (ValueError, TypeError):
            pass
        return raw

    def __getattr__(self, key: str) -> Any:
        if key.startswith("_"):
            raise AttributeError(key)
        raw = self._raw(key)
        if raw is not None:
            return self._cast(key, raw)
        raise AttributeError(f"settings has no attribute '{key}'")

    # ----------------------------------------------------------------- public

    def set(self, key: str, value: Any):
        """Override in-memory immediato (stesso processo)."""
        self._mem[key] = self._cast(key, value)

    def set_many(self, updates: dict[str, Any]) -> list[str]:
        """Applica piu' override. Ritorna lista campi cambiati."""
        changed = []
        for key, value in updates.items():
            if key not in self._RUNTIME_FIELDS:
                continue
            new_val = self._cast(key, value)
            old_val = self.get_current(key)
            if old_val != new_val:
                self._mem[key] = new_val
                changed.append(f"{key}: {old_val} -> {new_val}")
        return changed

    def get_current(self, key: str) -> Any:
        try:
            return getattr(self, key)
        except AttributeError:
            return None

    def reset_runtime(self):
        """Svuota override in-memory e cache. Torna a env/default."""
        self._mem.clear()
        self._file_cache.clear()
        self._cache_ts = 0.0

    def as_dict(self) -> dict:
        self._refresh()
        return {k: self.get_current(k) for k in self._RUNTIME_FIELDS}

    # ---------------------------------------------------------- campi fissi

    @property
    def BITGET_API_KEY(self)        -> str:  return os.getenv("BITGET_API_KEY", "")
    @property
    def BITGET_API_SECRET(self)     -> str:  return os.getenv("BITGET_API_SECRET", "")
    @property
    def BITGET_API_PASSPHRASE(self) -> str:  return os.getenv("BITGET_API_PASSPHRASE", "")

    @property
    def TRADING_MODE(self)  -> str:  return os.getenv("TRADING_MODE", "paper")
    @property
    def IS_LIVE(self)       -> bool: return self.TRADING_MODE == "live"
    @property
    def MARKET_TYPES(self)  -> list:
        return [s.strip() for s in os.getenv("MARKET_TYPES", "spot,futures").split(",") if s.strip()]

    @property
    def SPOT_SYMBOLS(self) -> list:
        return [s.strip() for s in os.getenv("SPOT_SYMBOLS", "BTC/USDT,ETH/USDT,SOL/USDT").split(",") if s.strip()]

    @property
    def FUTURES_SYMBOLS(self) -> list:
        return [s.strip() for s in os.getenv("FUTURES_SYMBOLS", "BTC/USDT:USDT,ETH/USDT:USDT,SOL/USDT:USDT").split(",") if s.strip()]

    @property
    def SCALPING_SYMBOLS(self) -> list:
        return [s.strip() for s in os.getenv("SCALPING_SYMBOLS", "BTC/USDT").split(",") if s.strip()]

    @property
    def TF_SWING(self)    -> str: return os.getenv("TF_SWING",    "15m")
    @property
    def TF_SCALP(self)    -> str: return os.getenv("TF_SCALP",    "1m")
    @property
    def TF_BREAKOUT(self) -> str: return os.getenv("TF_BREAKOUT", "1h")

    @property
    def TELEGRAM_TOKEN(self)      -> str:  return os.getenv("TELEGRAM_TOKEN", "")
    @property
    def TELEGRAM_CHAT_ID(self)    -> str:  return os.getenv("TELEGRAM_CHAT_ID", "")
    @property
    def NOTIFY_TRADES(self)       -> bool: return os.getenv("NOTIFY_TRADES",       "true").lower() in ("true","1","yes")
    @property
    def NOTIFY_ERRORS(self)       -> bool: return os.getenv("NOTIFY_ERRORS",       "true").lower() in ("true","1","yes")
    @property
    def NOTIFY_DAILY_REPORT(self) -> bool: return os.getenv("NOTIFY_DAILY_REPORT", "true").lower() in ("true","1","yes")

    @property
    def DATABASE_URL(self)     -> str:  return os.getenv("DATABASE_URL", "")
    @property
    def LOG_LEVEL(self)        -> str:  return os.getenv("LOG_LEVEL", "INFO")
    @property
    def ENABLE_DASHBOARD(self) -> bool: return os.getenv("ENABLE_DASHBOARD", "true").lower() in ("true","1","yes")
    @property
    def DASHBOARD_PORT(self)   -> int:
        try: return int(os.getenv("DASHBOARD_PORT", "8080"))
        except: return 8080


# Singleton globale — importato ovunque come: from trading_bot.config import settings
settings = DynamicSettings()
