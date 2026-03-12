"""
settings.py — Configurazione con persistenza PostgreSQL.

Priorità (dal basso all'alto):
  1. Default hardcoded
  2. Variabili d'ambiente Railway (env vars)
  3. Tabella bot_config su PostgreSQL  ← sopravvive ai riavvii Railway
  4. Override in-memory (set_many)     ← applicazione immediata

PERCHE' POSTGRESQL E NON FILE:
  Railway usa filesystem EFIMERO — ogni restart/deploy svuota /app.
  Il database PostgreSQL Railway e' PERMANENTE e sopravvive a tutti i restart.
"""

from __future__ import annotations

import os
import time
import json
from typing import Any
from dotenv import load_dotenv

load_dotenv()

# Fallback: file locale se DB non disponibile (sviluppo locale)
_HERE = os.path.dirname(__file__)
_FALLBACK_FILE = os.path.normpath(
    os.path.join(_HERE, "..", "dashboard", "runtime_config.json")
)


class DynamicSettings:
    """
    Singleton configurazione. Legge/scrive su PostgreSQL (tabella bot_config).
    Refresh automatico ogni REFRESH_INTERVAL secondi.
    Fallback su file locale se DATABASE_URL non e' impostato.
    """

    REFRESH_INTERVAL = 8.0   # secondi tra un DB read e l'altro

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
        self._db_cache: dict[str, Any] = {}
        self._cache_ts: float = 0.0
        self._mem: dict[str, Any] = {}
        self._db_ready: bool = False
        self._db_init_attempted: bool = False

    # ------------------------------------------------------------------ DB

    def _get_db_url(self) -> str:
        return os.getenv("DATABASE_URL", "")

    def _ensure_table(self, conn) -> bool:
        """Crea la tabella bot_config se non esiste. Ritorna True se ok."""
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS bot_config (
                    key   VARCHAR(64) PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)
            conn.execute("COMMIT")
            return True
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            return False

    def _db_read_all(self) -> dict:
        """Legge tutte le chiavi da bot_config. Ritorna {} se fallisce."""
        url = self._get_db_url()
        if not url:
            return self._file_read_all()
        try:
            import psycopg2
            conn = psycopg2.connect(url, connect_timeout=3)
            self._ensure_table(conn)
            cur = conn.cursor()
            cur.execute("SELECT key, value FROM bot_config")
            rows = cur.fetchall()
            conn.close()
            result = {}
            for key, val in rows:
                try:
                    result[key] = json.loads(val)
                except Exception:
                    result[key] = val
            self._db_ready = True
            return result
        except Exception as e:
            # DB non raggiungibile → fallback su file
            return self._file_read_all()

    def _db_write_many(self, updates: dict) -> bool:
        """Scrive/aggiorna piu' chiavi su bot_config. Ritorna True se ok."""
        url = self._get_db_url()
        if not url:
            return self._file_write_many(updates)
        try:
            import psycopg2
            conn = psycopg2.connect(url, connect_timeout=3)
            self._ensure_table(conn)
            cur = conn.cursor()
            for key, val in updates.items():
                cur.execute("""
                    INSERT INTO bot_config (key, value, updated_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (key) DO UPDATE
                        SET value = EXCLUDED.value,
                            updated_at = NOW()
                """, (key, json.dumps(val)))
            conn.execute("COMMIT") if hasattr(conn, 'execute') else conn.commit()
            conn.close()
            # Invalida cache in-memory del file
            self._cache_ts = 0.0
            return True
        except Exception as e:
            return self._file_write_many(updates)

    def _db_delete_all(self) -> bool:
        """Cancella tutta la config da bot_config (reset)."""
        url = self._get_db_url()
        if not url:
            return self._file_delete()
        try:
            import psycopg2
            conn = psycopg2.connect(url, connect_timeout=3)
            cur = conn.cursor()
            cur.execute("DELETE FROM bot_config")
            conn.commit()
            conn.close()
            self._db_cache = {}
            self._cache_ts = 0.0
            return True
        except Exception:
            return self._file_delete()

    # --------------------------------------------------------------- file fallback

    def _file_read_all(self) -> dict:
        try:
            if os.path.exists(_FALLBACK_FILE):
                with open(_FALLBACK_FILE) as f:
                    data = json.load(f)
                return {k: v for k, v in data.items() if not k.startswith("_")}
        except Exception:
            pass
        return {}

    def _file_write_many(self, updates: dict) -> bool:
        try:
            existing = self._file_read_all()
            existing.update(updates)
            os.makedirs(os.path.dirname(_FALLBACK_FILE), exist_ok=True)
            with open(_FALLBACK_FILE, "w") as f:
                json.dump(existing, f, indent=2)
            return True
        except Exception:
            return False

    def _file_delete(self) -> bool:
        try:
            if os.path.exists(_FALLBACK_FILE):
                os.remove(_FALLBACK_FILE)
            return True
        except Exception:
            return False

    # ------------------------------------------------------------------ core

    def _refresh(self):
        now = time.monotonic()
        if now - self._cache_ts < self.REFRESH_INTERVAL:
            return
        self._db_cache = self._db_read_all()
        self._cache_ts = now

    def _raw(self, key: str) -> Any:
        # 1. Override in-memory (set_many / set)
        if key in self._mem:
            return self._mem[key]
        # 2. DB / file (refresh ogni REFRESH_INTERVAL sec)
        self._refresh()
        if key in self._db_cache:
            return self._db_cache[key]
        # 3. Env var Railway
        env = os.getenv(key)
        if env is not None:
            return env
        # 4. Default
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

    # ----------------------------------------------------------------- public API

    def set(self, key: str, value: Any):
        """Override immediato in-memory + persiste su DB."""
        casted = self._cast(key, value)
        self._mem[key] = casted
        self._db_write_many({key: casted})

    def set_many(self, updates: dict[str, Any]) -> list[str]:
        """
        Applica aggiornamenti a runtime.
        1. Override immediato in-memory
        2. Persiste su PostgreSQL (sopravvive al restart)
        Ritorna lista dei campi cambiati.
        """
        changed = []
        to_write = {}
        for key, value in updates.items():
            if key not in self._RUNTIME_FIELDS:
                continue
            new_val = self._cast(key, value)
            old_val = self.get_current(key)
            if old_val != new_val:
                self._mem[key] = new_val
                to_write[key] = new_val
                changed.append(f"{key}: {old_val} -> {new_val}")

        if to_write:
            ok = self._db_write_many(to_write)
            if not ok:
                from loguru import logger
                logger.warning("[CONFIG] Persistenza DB fallita — cambio solo in-memory per questo ciclo")

        return changed

    def get_current(self, key: str) -> Any:
        try:
            return getattr(self, key)
        except AttributeError:
            return None

    def reset_runtime(self):
        """Cancella config dal DB e svuota override in-memory. Torna a env/default."""
        self._mem.clear()
        self._db_cache.clear()
        self._cache_ts = 0.0
        self._db_delete_all()

    def as_dict(self) -> dict:
        self._refresh()
        return {k: self.get_current(k) for k in self._RUNTIME_FIELDS}

    # ---------------------------------------------------------- campi fissi (solo env)

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


# Singleton globale
settings = DynamicSettings()
