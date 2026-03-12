"""
settings.py — Configurazione centralizzata.

Fonte di verita' per i campi runtime:
  PostgreSQL tabella bot_config  (SEMPRE, persistente tra restart)

Fallback solo se DB non raggiungibile:
  env vars Railway -> default hardcoded

Campi fissi (API keys, symbols, mode):
  sempre da env vars Railway.

DESIGN:
  - Nessun file su disco (Railway filesystem e' efimero)
  - Nessun _mem che puo' nascondere i valori DB
  - Al riavvio il bot legge dal DB e trova i valori salvati dalla dashboard
  - set_many() scrive su DB E aggiorna la cache locale
  - La cache locale dura DB_CACHE_TTL secondi, poi rilegge dal DB
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

# ── Schema tabella ────────────────────────────────────────────────────────────
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

# ── Campi configurabili dalla dashboard ──────────────────────────────────────
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


# ── DB helpers (modulo-level, usati dal singleton) ────────────────────────────

def _get_engine():
    """Crea un engine SQLAlchemy monouso. Ritorna None se DATABASE_URL mancante."""
    url = os.getenv("DATABASE_URL", "")
    if not url:
        return None
    try:
        from sqlalchemy import create_engine
        kwargs: dict = {"pool_pre_ping": True}
        if "railway" in url or "postgres" in url:
            kwargs["connect_args"] = {"sslmode": "require", "connect_timeout": 5}
        else:
            kwargs["connect_args"] = {"connect_timeout": 5}
        return create_engine(url, **kwargs)
    except Exception as e:
        log.warning(f"[settings] engine creation failed: {e}")
        return None


def _cast(key: str, raw: Any) -> Any:
    """Converte raw al tipo corretto basandosi sul default."""
    if raw is None:
        return _DEFAULTS.get(key)
    default = _DEFAULTS.get(key)
    if default is None:
        return raw
    try:
        if isinstance(default, bool):
            if isinstance(raw, bool):
                return raw
            if isinstance(raw, str):
                return raw.lower() in ("true", "1", "yes")
            return bool(raw)
        if isinstance(default, int):
            if isinstance(raw, bool):
                return int(raw)
            return int(float(str(raw)))
        if isinstance(default, float):
            return float(str(raw))
        if isinstance(default, str):
            return str(raw)
    except (ValueError, TypeError):
        pass
    return raw


def _db_load_all(engine) -> dict[str, Any]:
    """
    Legge TUTTA la tabella bot_config e ritorna dict castato.
    Crea la tabella se non esiste.
    Ritorna {} se fallisce.
    """
    if engine is None:
        return {}
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
        for key, val_json in rows:
            try:
                raw = json.loads(val_json)
                result[key] = _cast(key, raw)
            except Exception:
                pass
        return result
    except Exception as e:
        log.warning(f"[settings] db_load_all failed: {e}")
        return {}


def _db_save_all(engine, data: dict[str, Any]) -> bool:
    """
    Salva/aggiorna tutti i campi in bot_config con UPSERT.
    Ritorna True se ok.
    """
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
        log.warning(f"[settings] db_save_all failed: {e}")
        return False


def _db_delete_all(engine) -> bool:
    """Svuota bot_config (reset ai default)."""
    if engine is None:
        return False
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            conn.execute(text("DELETE FROM bot_config"))
            conn.commit()
        return True
    except Exception as e:
        log.warning(f"[settings] db_delete_all failed: {e}")
        return False


# ── Singleton ─────────────────────────────────────────────────────────────────

class DynamicSettings:
    """
    Tutti i campi runtime vengono SEMPRE dal DB (bot_config).
    La cache locale si rinnova ogni DB_CACHE_TTL secondi.
    Non esiste piu' _mem che puo' nascondere i valori DB.
    """

    DB_CACHE_TTL = 8.0   # secondi tra reload da DB

    def __init__(self):
        self._cache: dict[str, Any] = {}   # cache dei valori DB castati
        self._cache_ts: float = 0.0         # timestamp ultimo reload
        self._engine = None                 # engine SQLAlchemy (lazy init)
        self._db_ok: bool | None = None     # None=non testato

    # ── engine lazy init ──────────────────────────────────────────────────────

    def _get_engine(self):
        if self._engine is None:
            self._engine = _get_engine()
        return self._engine

    # ── cache refresh ─────────────────────────────────────────────────────────

    def _refresh(self, force: bool = False):
        """Ricarica dal DB se il TTL e' scaduto o force=True."""
        now = time.monotonic()
        if not force and (now - self._cache_ts) < self.DB_CACHE_TTL:
            return
        engine = self._get_engine()
        data = _db_load_all(engine)
        if engine is not None:
            self._db_ok = True if data is not None else False
        # Aggiorna la cache con i valori dal DB
        # Per le chiavi non nel DB, usa default/env
        self._cache = data   # solo le chiavi presenti nel DB
        self._cache_ts = now

    # ── lettura ───────────────────────────────────────────────────────────────

    def _get_runtime(self, key: str) -> Any:
        """
        Legge un campo runtime.
        Ordine: DB cache -> env var -> default hardcoded.
        """
        self._refresh()
        if key in self._cache:
            return self._cache[key]   # gia' castato da _db_load_all
        # env var (utile per override temporaneo senza dashboard)
        env = os.getenv(key)
        if env is not None:
            return _cast(key, env)
        return _defaults_get(key)

    def __getattr__(self, key: str) -> Any:
        if key.startswith("_"):
            raise AttributeError(key)
        if key in _RUNTIME_FIELDS:
            return self._get_runtime(key)
        raise AttributeError(f"settings has no attribute '{key}'")

    # ── scrittura ─────────────────────────────────────────────────────────────

    def set_many(self, updates: dict[str, Any]) -> list[str]:
        """
        Salva i nuovi valori sul DB e aggiorna la cache locale.
        Ritorna lista dei campi cambiati (per il log).
        Funziona sia stesso processo che processo separato:
          - stesso processo: cache aggiornata subito, __getattr__ vede i nuovi valori
          - processo separato: il bot rilegge dal DB entro DB_CACHE_TTL secondi
        """
        # Forza reload cache prima per avere i valori attuali
        self._refresh(force=True)

        to_save: dict[str, Any] = {}
        changed: list[str] = []

        for key, value in updates.items():
            if key not in _RUNTIME_FIELDS:
                continue
            new_val = _cast(key, value)
            old_val = self._cache.get(key, _DEFAULTS.get(key))
            to_save[key] = new_val
            if old_val != new_val:
                changed.append(f"{key}: {old_val} -> {new_val}")

        if to_save:
            engine = self._get_engine()
            ok = _db_save_all(engine, to_save)
            if ok:
                # Aggiorna cache locale subito (non aspettare TTL)
                self._cache.update(to_save)
                self._cache_ts = time.monotonic()
                self._db_ok = True
                log.info(f"[settings] salvato su DB: {list(to_save.keys())}")
            else:
                self._db_ok = False
                log.warning("[settings] DB non raggiungibile — valori solo in-cache locale")
                # Aggiorna cache locale comunque (dura fino al prossimo restart)
                self._cache.update(to_save)
                self._cache_ts = time.monotonic()

        return changed

    def reset_runtime(self):
        """
        Cancella tutti i valori da bot_config.
        Dopo il reset: DB vuoto -> si usano env vars / default.
        """
        engine = self._get_engine()
        _db_delete_all(engine)
        self._cache.clear()
        self._cache_ts = 0.0
        log.info("[settings] reset_runtime: bot_config svuotata")

    def get_current(self, key: str) -> Any:
        try:
            return getattr(self, key)
        except AttributeError:
            return None

    def as_dict(self) -> dict:
        """Snapshot di tutti i campi runtime (dal DB + default per i mancanti)."""
        self._refresh()
        result = {}
        for key in _RUNTIME_FIELDS:
            val = self._cache.get(key)
            if val is None:
                env = os.getenv(key)
                val = _cast(key, env) if env is not None else _DEFAULTS.get(key)
            result[key] = val
        return result

    def storage_backend(self) -> str:
        if self._db_ok is True:
            return "postgresql"
        if self._db_ok is False:
            return "memory_only"
        # Non ancora testato
        self._refresh(force=True)
        return "postgresql" if self._db_ok else "memory_only"

    # ── campi fissi (sempre da env Railway) ───────────────────────────────────

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


def _defaults_get(key: str) -> Any:
    return _DEFAULTS.get(key)


# ── Singleton globale ─────────────────────────────────────────────────────────
settings = DynamicSettings()
