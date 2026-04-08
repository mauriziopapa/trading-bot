"""
settings.py — Tutti i parametri runtime vengono SOLO dal DB (tabella bot_config).
Zero valori hardcoded nel codice. Zero file su disco.

PATCH v4: Aggiunto MAX_NOTIONAL_PCT per full exposure mode.
"""

from __future__ import annotations

import os
import json
import time
import logging
from typing import Any
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger("settings")

# ── Tipo atteso per ogni campo (usato solo per il cast, non come default) ──────
_FIELD_TYPES: dict[str, type] = {
    "MAX_RISK_PCT":          float,
    "DEFAULT_LEVERAGE":      int,
    "MAX_DAILY_LOSS_PCT":    float,
    "MAX_DRAWDOWN_PCT":      float,
    "TAKE_PROFIT_RATIO":     float,
    "TRAILING_STOP_PCT":     float,
    "MIN_CONFIDENCE":        float,
    "MAX_POSITIONS_SPOT":    int,
    "MAX_POSITIONS_FUTURES": int,
    "MARGIN_MODE":           str,
    "ENABLE_RSI_MACD":       bool,
    "ENABLE_BOLLINGER":      bool,
    "ENABLE_BREAKOUT":       bool,
    "ENABLE_SCALPING":       bool,
    "ENABLE_EMERGING":       bool,
    # ── Emerging scanner ──────────────────────────────────────────────────
    "EM_MIN_VOLUME_USD":     float,
    "EM_MIN_CHANGE_24H":     float,
    "EM_MIN_VOLUME_SURGE":   float,
    "EM_MAX_MARKET_CAP":     float,
    "EM_MIN_MARKET_CAP":     float,
    "EM_MAX_RESULTS":        int,
    "EM_NEW_LISTING_DAYS":   int,
    "EM_EXCLUDE_SYMBOLS":    str,
    # ── Sentiment ─────────────────────────────────────────────────────────
    "SENTIMENT_BYPASS":       bool,
    "FEAR_GREED_LONG_MIN":    float,
    "FEAR_GREED_LONG_MAX":    float,
    "FEAR_GREED_SHORT_MIN":   float,
    "FEAR_GREED_SHORT_MAX":   float,
    # ── v4: Full exposure ─────────────────────────────────────────────────
    "MAX_NOTIONAL_PCT":       float,   # % max del balance per singolo trade (default 40)
    # ── v4: Emerging tuning (usati da regime_detector + dashboard) ────────
    "EMERGING_DIRECT_SCORE":  float,   # soglia score per BUY diretto
    "EMERGING_MOMENTUM_CHG":  float,   # soglia % change 24h per momentum
    "EMERGING_RISK_MULT":     float,   # moltiplicatore risk per emerging
    "EMERGING_MAX_SPREAD":    float,   # max spread % per entrare
    # ── Strategy governance ──────────────────────────────────────────────
    "ENABLE_MOMENTUM":              bool,   # enable MomentumStrategy
    "STRATEGIES_ENABLED":           str,    # comma-separated allowlist (e.g. "MOMENTUM")
    # ── Momentum strategy tuning ─────────────────────────────────────────
    "MOMENTUM_MIN_SCORE":           float,
    "MOMENTUM_MIN_VOLUME_USD":      float,
    "MOMENTUM_RISK_PCT":            float,
    "MOMENTUM_LEVERAGE":            int,
    "MOMENTUM_MAX_HOLD_SECONDS":    int,
    "MOMENTUM_MIN_HOLD_SECONDS":    int,
    "MOMENTUM_SL_ATR_MULT":         float,
    "MOMENTUM_TP_ATR_MULT":         float,
    "MOMENTUM_TRAILING_ENABLE":     bool,
    "MOMENTUM_TRAIL_ACTIVATION_R":  float,
    "MOMENTUM_TRAIL_DIST_ATR":      float,
    "MOMENTUM_COOLDOWN_LOSS_MIN":   int,
    "MOMENTUM_COOLDOWN_WIN_MIN":    int,
    "MOMENTUM_MAX_CONCURRENT":      int,
    # ── Risk manager extras ──────────────────────────────────────────────
    "MANUAL_UNLOCK_REQUIRED":           bool,
    "PER_SYMBOL_MAX_LOSS_USDT":         float,
    "PER_SYMBOL_MAX_CONSEC_LOSSES":     int,
    "PER_STRATEGY_MAX_DRAWDOWN_PCT":    float,
    "STALE_GLOBAL_STOP_ALERT_MIN":      int,
}
_RUNTIME_FIELDS = set(_FIELD_TYPES.keys())


# ── Cast ──────────────────────────────────────────────────────────────────────

def _cast(key: str, raw: Any) -> Any:
    t = _FIELD_TYPES.get(key)
    if t is None or raw is None:
        return raw
    try:
        if t is bool:
            if isinstance(raw, bool): return raw
            if isinstance(raw, str):  return raw.lower() in ("true", "1", "yes")
            return bool(raw)
        if t is int:
            if isinstance(raw, bool): return int(raw)
            return int(float(str(raw)))
        if t is float:
            return float(str(raw))
        if t is str:
            return str(raw)
    except (ValueError, TypeError):
        pass
    return raw


# ── DB ────────────────────────────────────────────────────────────────────────

_DDL = """
CREATE TABLE IF NOT EXISTS bot_config (
    key        VARCHAR(64) PRIMARY KEY,
    value      TEXT        NOT NULL,
    updated_at TIMESTAMP   NOT NULL DEFAULT NOW()
);
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'bot_config'
          AND column_name = 'value'
          AND data_type != 'text'
    ) THEN
        ALTER TABLE bot_config ALTER COLUMN value TYPE TEXT;
    END IF;
END $$;
"""


def _make_engine():
    url = os.getenv("DATABASE_URL", "")
    if not url:
        log.error("[settings] DATABASE_URL non impostata!")
        return None
    try:
        from sqlalchemy import create_engine
        ssl = "sslmode=require" not in url
        kwargs: dict = {"pool_pre_ping": True}
        if ssl and ("railway" in url or "amazonaws" in url or "supabase" in url):
            kwargs["connect_args"] = {"sslmode": "require", "connect_timeout": 5}
        return create_engine(url, **kwargs)
    except Exception as e:
        log.error(f"[settings] create_engine fallito: {e}")
        return None


def _db_load(engine) -> dict[str, Any] | None:
    if engine is None:
        return None
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            conn.execute(text(_DDL))
            conn.commit()
            rows = conn.execute(
                text("SELECT key, value FROM bot_config WHERE key = ANY(:keys)"),
                {"keys": list(_RUNTIME_FIELDS)}
            ).fetchall()
        result = {}
        for key, val_raw in rows:
            try:
                try:
                    parsed = json.loads(val_raw)
                except (json.JSONDecodeError, TypeError):
                    parsed = val_raw
                result[key] = _cast(key, parsed)
            except Exception as e:
                log.warning(f"[settings] cast error {key}={val_raw!r}: {e}")
        return result
    except Exception as e:
        log.error(f"[settings] db_load fallito: {e}")
        return None


def _db_save(engine, data: dict[str, Any]) -> bool:
    if engine is None or not data:
        return False
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            conn.execute(text(_DDL))
            conn.commit()
            for key, val in data.items():
                if key not in _RUNTIME_FIELDS:
                    continue
                conn.execute(text("""
                    INSERT INTO bot_config (key, value, updated_at)
                    VALUES (:k, :v, NOW())
                    ON CONFLICT (key) DO UPDATE
                        SET value      = EXCLUDED.value,
                            updated_at = NOW()
                """), {"k": key, "v": json.dumps(val)})
            conn.commit()
        return True
    except Exception as e:
        log.error(f"[settings] db_save fallito: {e}")
        return False


def _db_delete(engine) -> bool:
    if engine is None:
        return False
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            conn.execute(text("DELETE FROM bot_config"))
            conn.commit()
        return True
    except Exception as e:
        log.error(f"[settings] db_delete fallito: {e}")
        return False


# ── Singleton ─────────────────────────────────────────────────────────────────

class DynamicSettings:
    CACHE_TTL = 8.0

    def __init__(self):
        self._engine = None
        self._cache: dict[str, Any] = {}
        self._cache_ts: float = 0.0
        self._db_ok: bool | None = None

    def _eng(self):
        if self._engine is None:
            self._engine = _make_engine()
        return self._engine

    def _refresh(self, force: bool = False):
        now = time.monotonic()
        if not force and (now - self._cache_ts) < self.CACHE_TTL:
            return
        data = _db_load(self._eng())
        if data is not None:
            self._cache = data
            self._db_ok = True
        else:
            self._db_ok = False
        self._cache_ts = time.monotonic()

    def __getattr__(self, key: str) -> Any:
        if key.startswith("_"):
            raise AttributeError(key)
        if key in _RUNTIME_FIELDS:
            self._refresh()
            if key in self._cache:
                return self._cache[key]
            raise AttributeError(
                f"[settings] '{key}' non trovato in bot_config. "
                f"Esegui init_bot_config.sql sul DB PostgreSQL."
            )
        raise AttributeError(f"settings has no attribute '{key}'")

    def set_many(self, updates: dict[str, Any]) -> list[str]:
        self._refresh(force=True)
        to_save: dict[str, Any] = {}
        changed: list[str] = []
        for key, value in updates.items():
            if key not in _RUNTIME_FIELDS:
                continue
            new_val = _cast(key, value)
            old_val = self._cache.get(key)
            to_save[key] = new_val
            if old_val != new_val:
                changed.append(f"{key}: {old_val} -> {new_val}")
        if to_save:
            ok = _db_save(self._eng(), to_save)
            self._cache.update(to_save)
            self._cache_ts = time.monotonic()
            if ok:
                self._db_ok = True
            else:
                log.error("[settings] set_many: DB non raggiungibile")
        return changed

    def reset_runtime(self):
        _db_delete(self._eng())
        self._cache.clear()
        self._cache_ts = 0.0

    def get_current(self, key: str) -> Any:
        try:
            return getattr(self, key)
        except AttributeError:
            return None

    def as_dict(self, force: bool = False) -> dict:
        self._refresh(force=force)
        return dict(self._cache)

    def storage_backend(self) -> str:
        if self._db_ok is True:  return "postgresql"
        if self._db_ok is False: return "memory_only"
        self._refresh(force=True)
        return "postgresql" if self._db_ok else "memory_only"

    # ── campi fissi — sempre da env Railway ───────────────────────────────────
    @property
    def BITGET_API_KEY(self)        -> str:  return os.getenv("BITGET_API_KEY", "")
    @property
    def BITGET_API_SECRET(self)     -> str:  return os.getenv("BITGET_API_SECRET", "")
    @property
    def BITGET_API_PASSPHRASE(self) -> str:  return os.getenv("BITGET_API_PASSPHRASE", "")
    @property
    def TRADING_MODE(self)  -> str:  return os.getenv("TRADING_MODE", "live")
    @property
    def IS_LIVE(self)       -> bool: return self.TRADING_MODE == "live"
    @property
    def MARKET_TYPES(self)  -> list:
        return [s.strip() for s in os.getenv("MARKET_TYPES", "futures").split(",") if s.strip()]
    @property
    def SPOT_SYMBOLS(self) -> list:
        raw = os.getenv("SPOT_SYMBOLS", "BTC/USDT,ETH/USDT,SOL/USDT,DOT/USDT,FET/USDT,FET/USDT").strip()
        if raw.upper() == "AUTO":
            try:
                from trading_bot.utils.symbol_discovery import get_discovery
                return get_discovery().get_spot_symbols()
            except Exception:
                return ["BTC/USDT", "ETH/USDT", "SOL/USDT"]
        return [s.strip() for s in raw.split(",") if s.strip()]
    @property
    def FUTURES_SYMBOLS(self) -> list:
        raw = os.getenv("FUTURES_SYMBOLS", "BTC/USDT:USDT,ETH/USDT:USDT,SOL/USDT:USDT,BNB/USDT:USDT,XRP/USDT:USDT,ADA/USDT:USDT,AVAX/USDT:USDT,LINK/USDT:USDT,DOT/USDT:USDT,MATIC/USDT:USDT,DOGE/USDT:USDT,LTC/USDT:USDT,ATOM/USDT:USDT,NEAR/USDT:USDT,APT/USDT:USDT,OP/USDT:USDT,ARB/USDT:USDT,INJ/USDT:USDT,RUNE/USDT:USDT,SUI/USDT:USDT,SEI/USDT:USDT,PEPE/USDT:USDT,WIF/USDT:USDT,BONK/USDT:USDT,FET/USDT:USDT,TAO/USDT:USDT,ENS/USDT:USDT,FIL/USDT:USDT,ICP/USDT:USDT,ETC/USDT:USDT,AAVE/USDT:USDT,UNI/USDT:USDT,HBAR/USDT:USDT,THETA/USDT:USDT,ALGO/USDT:USDT,VET/USDT:USDT,GRT/USDT:USDT,IMX/USDT:USDT,STX/USDT:USDT,KAS/USDT:USDT,AR/USDT:USDT,ORDI/USDT:USDT,ZEC/USDT:USDT,XPL/USDT:USDT,FARTCOIN/USDT:USDT,HYPE/USDT:USDT,GRASS/USDT:USDT,B/USDT:USDT,WLD/USDT:USDT,EIGEN/USDT:USDT,PENGU/USDT:USDT,ZEN/USDT:USDT,M/USDT:USDT,QUBIC/USDT:USDT,RNDR/USDT:USDT,AGIX/USDT:USDT,OCEAN/USDT:USDT,AKT/USDT:USDT,AIOZ/USDT:USDT,IO/USDT:USDT,NMR/USDT:USDT,PHB/USDT:USDT,CQT/USDT:USDT,ORAI/USDT:USDT,PAAL/USDT:USDT,ARC/USDT:USDT").strip()
        if raw.upper() == "AUTO":
            try:
                from trading_bot.utils.symbol_discovery import get_discovery
                return get_discovery().get_futures_symbols()
            except Exception:
                return ["BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT"]
        return [s.strip() for s in raw.split(",") if s.strip()]
    @property
    def SCALPING_SYMBOLS(self) -> list:
        raw = os.getenv("SCALPING_SYMBOLS", "BTC/USDT").strip()
        if raw.upper() == "AUTO":
            try:
                from trading_bot.utils.symbol_discovery import get_discovery
                return get_discovery().get_top_by_volume("spot", 6)
            except Exception:
                return ["BTC/USDT", "ETH/USDT", "SOL/USDT"]
        return [s.strip() for s in raw.split(",") if s.strip()]
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
    def LOG_LEVEL(self)        -> str:  return os.getenv("LOG_LEVEL", "DEBUG")
    @property
    def ENABLE_DASHBOARD(self) -> bool: return os.getenv("ENABLE_DASHBOARD", "true").lower() in ("true","1","yes")
    @property
    def DASHBOARD_PORT(self)   -> int:
        try: return int(os.getenv("DASHBOARD_PORT", "8080"))
        except: return 8080


settings = DynamicSettings()
