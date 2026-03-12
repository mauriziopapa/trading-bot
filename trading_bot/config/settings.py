"""
settings.py - Configurazione centralizzata con DynamicSettings.

Priorita (dal basso all'alto):
  1. Default hardcoded
  2. Railway Variables (env vars) - lette ogni volta
  3. PostgreSQL tabella bot_config - PERSISTENTE tra restart
  4. Override in-memory (set_many) - cambio immediato stesso processo

PERCHE PostgreSQL e non un file:
  Railway ha filesystem EFIMERO - ogni restart azzera tutto.
  Il database PostgreSQL e' l'unico storage persistente garantito.
"""

from __future__ import annotations
import os
import time
import json
from typing import Any
from dotenv import load_dotenv

load_dotenv()

# ── Fallback file (usato solo se DB non disponibile) ──────────────────────────
_HERE = os.path.dirname(__file__)
_FALLBACK_FILE = os.path.normpath(
    os.path.join(_HERE, "..", "dashboard", "runtime_config.json")
)


class DynamicSettings:
    """
    Singleton configurazione. Priorita':
      in-memory > PostgreSQL (ogni 10s) > env vars > default hardcoded
    """

    DB_REFRESH_INTERVAL = 10.0   # secondi tra reload da DB
    FILE_REFRESH_INTERVAL = 5.0  # secondi tra reload da file (fallback)

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
        self._mem: dict[str, Any] = {}          # override in-process immediati
        self._db_cache: dict[str, Any] = {}     # cache dal DB
        self._db_cache_ts: float = 0.0
        self._file_cache: dict[str, Any] = {}   # cache dal file (fallback)
        self._file_cache_ts: float = 0.0
        self._db_available: bool | None = None  # None = non ancora testato

    # ------------------------------------------------------------------ DB

    def _get_db_url(self) -> str:
        return os.getenv("DATABASE_URL", "")

    def _try_load_from_db(self) -> dict:
        """Carica config da PostgreSQL. Ritorna {} se fallisce."""
        try:
            from sqlalchemy import create_engine, text
            url = self._get_db_url()
            if not url:
                return {}

            engine = create_engine(
                url,
                pool_size=1, max_overflow=0, pool_timeout=5,
                connect_args={"sslmode": "require", "connect_timeout": 5}
                    if "railway" in url or "postgres" in url else {"connect_timeout": 5}
            )
            with engine.connect() as conn:
                # Crea tabella se non esiste
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS bot_config (
                        key        VARCHAR(64) PRIMARY KEY,
                        value      TEXT        NOT NULL,
                        updated_at TIMESTAMP   NOT NULL DEFAULT NOW()
                    )
                """))
                # Sanity check: se value non e' TEXT (es. CHAR(1) da errore Railway)
                # la altera automaticamente
                conn.execute(text("""
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
                """))
                conn.commit()

                rows = conn.execute(text("SELECT key, value FROM bot_config")).fetchall()
                result = {}
                for key, val in rows:
                    if key in self._RUNTIME_FIELDS:
                        result[key] = json.loads(val)
            engine.dispose()
            self._db_available = True
            return result

        except Exception as e:
            self._db_available = False
            return {}

    def _save_to_db(self, updates: dict[str, Any]) -> bool:
        """Salva/aggiorna config nel DB. Ritorna True se ok."""
        try:
            from sqlalchemy import create_engine, text
            url = self._get_db_url()
            if not url:
                return False

            engine = create_engine(
                url,
                pool_size=1, max_overflow=0, pool_timeout=5,
                connect_args={"sslmode": "require", "connect_timeout": 5}
                    if "railway" in url or "postgres" in url else {"connect_timeout": 5}
            )
            with engine.connect() as conn:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS bot_config (
                        key        VARCHAR(64) PRIMARY KEY,
                        value      TEXT        NOT NULL,
                        updated_at TIMESTAMP   NOT NULL DEFAULT NOW()
                    )
                """))
                conn.execute(text("""
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
                """))
                conn.commit()
                for key, val in updates.items():
                    if key in self._RUNTIME_FIELDS:
                        conn.execute(text("""
                            INSERT INTO bot_config (key, value, updated_at)
                            VALUES (:k, :v, NOW())
                            ON CONFLICT (key) DO UPDATE
                              SET value = EXCLUDED.value,
                                  updated_at = NOW()
                        """), {"k": key, "v": json.dumps(val)})
                conn.commit()
            engine.dispose()
            self._db_available = True
            # Invalida la cache DB cosi viene riletta subito
            self._db_cache_ts = 0.0
            return True

        except Exception as e:
            self._db_available = False
            return False

    def _delete_from_db(self) -> bool:
        """Cancella tutta la config dal DB (reset)."""
        try:
            from sqlalchemy import create_engine, text
            url = self._get_db_url()
            if not url:
                return False
            engine = create_engine(url, pool_size=1, max_overflow=0, pool_timeout=5,
                connect_args={"sslmode": "require", "connect_timeout": 5}
                    if "railway" in url or "postgres" in url else {"connect_timeout": 5})
            with engine.connect() as conn:
                # DROP + RECREATE per sicurezza (evita schema corrotto)
                conn.execute(text("DROP TABLE IF EXISTS bot_config"))
                conn.execute(text("""
                    CREATE TABLE bot_config (
                        key        VARCHAR(64) PRIMARY KEY,
                        value      TEXT        NOT NULL,
                        updated_at TIMESTAMP   NOT NULL DEFAULT NOW()
                    )
                """))
                conn.commit()
            engine.dispose()
            self._db_cache = {}
            self._db_cache_ts = 0.0
            return True
        except Exception:
            return False

    # ------------------------------------------------------------------ file fallback

    def _try_load_from_file(self) -> dict:
        try:
            if os.path.exists(_FALLBACK_FILE):
                with open(_FALLBACK_FILE) as f:
                    data = json.load(f)
                return {k: v for k, v in data.items()
                        if not k.startswith("_") and k in self._RUNTIME_FIELDS}
        except Exception:
            pass
        return {}

    def _save_to_file(self, updates: dict[str, Any]):
        try:
            existing = self._try_load_from_file()
            existing.update(updates)
            existing["_updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            os.makedirs(os.path.dirname(_FALLBACK_FILE), exist_ok=True)
            with open(_FALLBACK_FILE, "w") as f:
                json.dump(existing, f, indent=2)
        except Exception:
            pass

    # ------------------------------------------------------------------ core refresh

    def _refresh_db(self):
        now = time.monotonic()
        if now - self._db_cache_ts < self.DB_REFRESH_INTERVAL:
            return
        data = self._try_load_from_db()
        if data is not None:
            self._db_cache = data
        self._db_cache_ts = now

    def _refresh_file(self):
        now = time.monotonic()
        if now - self._file_cache_ts < self.FILE_REFRESH_INTERVAL:
            return
        self._file_cache = self._try_load_from_file()
        self._file_cache_ts = now

    # ------------------------------------------------------------------ get

    def _raw(self, key: str) -> Any:
        # 1. Override in-memory (set/set_many, applicati subito)
        if key in self._mem:
            return self._mem[key]

        # 2. PostgreSQL (persistente tra restart) - tentativo ogni 10s
        self._refresh_db()
        if key in self._db_cache:
            return self._db_cache[key]

        # 3. File fallback (per sviluppo locale senza DB)
        self._refresh_file()
        if key in self._file_cache:
            return self._file_cache[key]

        # 4. Env var Railway
        env = os.getenv(key)
        if env is not None:
            return env

        # 5. Default hardcoded
        return self._DEFAULTS.get(key)

    def _cast(self, key: str, raw: Any) -> Any:
        """
        Cast al tipo corretto basandosi sul default hardcoded.
        Gestisce: bool da JSON (True/False), numeri, stringhe.
        """
        if raw is None:
            return self._DEFAULTS.get(key)
        default = self._DEFAULTS.get(key)
        if default is None:
            return raw
        try:
            if isinstance(default, bool):
                # json.loads ritorna gia' bool Python per true/false
                if isinstance(raw, bool):
                    return raw
                # stringa "true"/"false" da env var
                if isinstance(raw, str):
                    return raw.lower() in ("true", "1", "yes")
                # numero 0/1
                return bool(raw)
            if isinstance(default, int):
                # attenzione: bool e' sottoclasse di int in Python
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

    def __getattr__(self, key: str) -> Any:
        if key.startswith("_"):
            raise AttributeError(key)
        raw = self._raw(key)
        if raw is not None:
            return self._cast(key, raw)
        raise AttributeError(f"settings has no attribute '{key}'")

    # ------------------------------------------------------------------ public API

    def set(self, key: str, value: Any):
        """Override in-memory immediato."""
        self._mem[key] = self._cast(key, value)

    def set_many(self, updates: dict[str, Any]) -> list[str]:
        """
        Applica aggiornamenti dalla dashboard:
        1. Salva SEMPRE su PostgreSQL (persistente tra restart)
        2. Applica SEMPRE in-memory (stesso processo, immediato)
        3. Invalida cache DB cosi la prossima lettura vede i nuovi valori
        4. Fallback file se DB non disponibile

        NON confronta old==new per decidere se salvare:
        la dashboard manda sempre lo snapshot completo, dobbiamo
        applicarlo tutto, anche se i valori sembrano uguali
        (potrebbero essere diversi per tipo: float vs int, bool vs str).
        """
        changed = []
        to_save = {}

        for key, value in updates.items():
            if key not in self._RUNTIME_FIELDS:
                continue
            new_val = self._cast(key, value)
            # Confronta con il valore PRECEDENTE in memoria (non dal DB)
            # per costruire il log "changed", ma salva comunque tutto
            old_mem = self._mem.get(key)
            old_db  = self._db_cache.get(key)
            old_shown = old_mem if old_mem is not None else old_db
            if old_shown is None:
                old_shown = self._DEFAULTS.get(key)

            # Applica in-memory SEMPRE
            self._mem[key] = new_val
            to_save[key] = new_val

            # Log solo se effettivamente diverso
            if self._cast(key, old_shown) != new_val:
                changed.append(f"{key}: {old_shown} -> {new_val}")

        if to_save:
            # Invalida cache DB prima di salvare
            self._db_cache_ts = 0.0
            ok = self._save_to_db(to_save)
            if ok:
                # Ricarica subito dal DB per conferma
                self._db_cache = self._try_load_from_db()
                self._db_cache_ts = time.monotonic()
            else:
                # Fallback file
                self._save_to_file(to_save)

        return changed

    def get_current(self, key: str) -> Any:
        try:
            return getattr(self, key)
        except AttributeError:
            return None

    def reset_runtime(self):
        """
        Reset completo: cancella DB (bot_config), svuota memoria, cancella file.
        Dopo il reset i valori tornano a Railway Variables / default hardcoded.
        """
        # 1. Svuota memoria in-process
        self._mem.clear()
        self._db_cache.clear()
        self._db_cache_ts = 0.0
        self._file_cache.clear()
        self._file_cache_ts = 0.0

        # 2. Cancella dal DB (DROP + RECREATE tabella vuota)
        self._delete_from_db()

        # 3. Cancella file fallback
        try:
            if os.path.exists(_FALLBACK_FILE):
                os.remove(_FALLBACK_FILE)
        except Exception:
            pass

    def as_dict(self) -> dict:
        return {k: self.get_current(k) for k in self._RUNTIME_FIELDS}

    def storage_backend(self) -> str:
        """Ritorna quale backend sta usando: 'postgresql', 'file', 'memory_only'"""
        if self._db_available is True:
            return "postgresql"
        if self._db_available is False:
            return "file" if os.path.exists(_FALLBACK_FILE) else "memory_only"
        # Non ancora testato - prova
        self._refresh_db()
        return self.storage_backend()

    # ------------------------------------------------------------------ campi fissi

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
