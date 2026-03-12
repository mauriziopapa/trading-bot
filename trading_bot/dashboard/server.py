"""
Dashboard Server — FastAPI + WebSocket
Serve la UI e fa push dello stato ogni 3 secondi.
Include /api/config per aggiornare la configurazione del bot a runtime.
"""

import os
import json
import asyncio
from datetime import datetime, timezone

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, validator
from loguru import logger

app = FastAPI(title="Bitget Bot Dashboard")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

_DIR       = os.path.dirname(__file__)
STATE_FILE = os.path.join(_DIR, "dashboard_state.json")
HTML_FILE  = os.path.join(_DIR, "dashboard.html")


# ════════════════════════════════════════════════════════════════════════════
# CONFIG MODEL
# ════════════════════════════════════════════════════════════════════════════

class ConfigPayload(BaseModel):
    MAX_RISK_PCT:           float = Field(3.5,  ge=0.5,  le=10.0)
    DEFAULT_LEVERAGE:       int   = Field(5,    ge=1,    le=20)
    MAX_DAILY_LOSS_PCT:     float = Field(8.0,  ge=1.0,  le=30.0)
    MAX_DRAWDOWN_PCT:       float = Field(15.0, ge=5.0,  le=50.0)
    TAKE_PROFIT_RATIO:      float = Field(2.5,  ge=1.0,  le=5.0)
    TRAILING_STOP_PCT:      float = Field(1.2,  ge=0.1,  le=5.0)
    MIN_CONFIDENCE:         float = Field(65.0, ge=40.0, le=95.0)
    MAX_POSITIONS_SPOT:     int   = Field(4,    ge=1,    le=10)
    MAX_POSITIONS_FUTURES:  int   = Field(3,    ge=1,    le=10)
    MARGIN_MODE:            str   = Field("isolated")
    ENABLE_RSI_MACD:        bool  = True
    ENABLE_BOLLINGER:       bool  = True
    ENABLE_BREAKOUT:        bool  = True
    ENABLE_SCALPING:        bool  = True
    ENABLE_EMERGING:        bool  = True

    @validator("MARGIN_MODE")
    def validate_margin_mode(cls, v):
        if v not in ("isolated", "cross"):
            raise ValueError("Deve essere 'isolated' o 'cross'")
        return v

    class Config:
        extra = "ignore"


# ── Config I/O — SEMPRE DAL DB tramite settings ───────────────────────────────

def _read_config() -> dict:
    """
    Legge la config SEMPRE dal DB (force refresh).
    FIX: 'if data:' era falsy con {} — usava i default Pydantic anche col DB pieno.
    FIX: force=True garantisce lettura fresca dal DB, non dalla cache TTL.
    """
    try:
        from trading_bot.config import settings as S
        # force=True: salta la cache TTL e rilegge dal DB ora
        data = S.as_dict(force=True)   # forza lettura fresca dal DB
        # FIX: 'if data is not None' invece di 'if data'
        # {} (cache vuota) è falsy → prima tornava sempre i default Pydantic!
        if data is not None:
            data["_storage_backend"] = S.storage_backend()
            data["_source"] = "postgresql" if S._db_ok else "memory"
            return data
    except Exception as e:
        logger.warning(f"[CONFIG] _read_config fallito: {e}")
    # Fallback: solo se settings non importabile (primo avvio senza DB)
    fallback = ConfigPayload().dict()
    fallback["_storage_backend"] = "fallback_defaults"
    fallback["_source"] = "pydantic_defaults"
    return fallback


def _write_config(cfg: dict) -> None:
    pass   # no-op: salvataggio avviene in set_many() sul DB


def _apply_to_settings(cfg: dict) -> list[str]:
    try:
        from trading_bot.config import settings as S
        changed = S.set_many(cfg)
        if changed:
            backend = S.storage_backend()
            for ch in changed:
                logger.info(f"[CONFIG LIVE] {ch}")
            logger.info(f"[CONFIG] Salvato su: {backend}")
        else:
            logger.debug("[CONFIG] Nessun campo modificato")
        return changed
    except Exception as e:
        logger.warning(f"[CONFIG] Errore apply: {e}")
        return []


# ── State I/O ─────────────────────────────────────────────────────────────────

def _read_state() -> dict:
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE) as f:
                data = json.load(f)
            data["config"] = _read_config()
            return data
    except Exception:
        pass
    return _demo_state()


def _demo_state() -> dict:
    now = datetime.now(timezone.utc).isoformat()
    return {
        "mode": "paper", "status": "starting", "last_update": now,
        "balance":   {"spot": 0, "futures": 0, "total": 0, "pnl_today_pct": 0, "pnl_today_usdt": 0},
        "positions": [], "signals": [],
        "logs":      [{"ts": now, "level": "INFO", "msg": "Dashboard avviata — in attesa del bot..."}],
        "stats":     {"total_trades": 0, "wins": 0, "losses": 0, "win_rate": 0,
                      "avg_win_pct": 0, "avg_loss_pct": 0, "daily_pnl": 0, "daily_trades": 0},
        "sentiment": None, "emerging": [],
        "config":    _read_config(),
    }


# ════════════════════════════════════════════════════════════════════════════
# WEBSOCKET MANAGER
# ════════════════════════════════════════════════════════════════════════════

class ConnectionManager:
    def __init__(self):
        self.active: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)

    async def broadcast(self, msg: dict):
        dead = []
        for ws in self.active:
            try:
                await ws.send_json(msg)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


manager = ConnectionManager()


@app.on_event("startup")
async def start_pusher():
    asyncio.create_task(_push_loop())


async def _push_loop():
    while True:
        await asyncio.sleep(3)
        if manager.active:
            await manager.broadcast({"type": "state", "data": _read_state()})


# ════════════════════════════════════════════════════════════════════════════
# ENDPOINTS
# ════════════════════════════════════════════════════════════════════════════

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    try:
        with open(HTML_FILE) as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>dashboard.html non trovato</h1>"


@app.get("/api/state")
async def get_state():
    return _read_state()


@app.get("/api/config")
async def get_config():
    return _read_config()


@app.post("/api/config")
async def update_config(request: Request):
    """
    Aggiorna SOLO i campi presenti nel payload (merge con DB).
    FIX: prima usava ConfigPayload con default Pydantic — campi assenti
    venivano riempiti con i default e sovrascrivevano il DB (es.
    salvataggio filtri EM_ resettava leva, confidence, ecc.).
    Ora: legge il DB corrente, applica solo le chiavi presenti nel JSON.
    """
    try:
        raw = await request.json()
    except Exception:
        raw = {}

    # Legge config corrente dal DB come base (non usa i default Pydantic)
    current = _read_config()
    # Rimuove metadati interni
    current.pop("_storage_backend", None)
    current.pop("_source", None)

    # Merge: sovrascrive SOLO i campi ricevuti
    merged = {**current, **raw}

    # Valida i campi presenti (solo quelli nel payload) con ConfigPayload
    # per i vincoli ge/le — ma senza riempire i mancanti
    from pydantic import ValidationError
    try:
        ConfigPayload(**merged)   # validazione completa sul merged
    except ValidationError as ve:
        return {"ok": False, "error": str(ve)}

    changed = _apply_to_settings(merged)
    applied_live = True

    # Rilegge config aggiornata per il broadcast
    updated_cfg = _read_config()
    updated_cfg.pop("_storage_backend", None)
    updated_cfg.pop("_source", None)

    await manager.broadcast({
        "type": "config_updated",
        "data": {"config": updated_cfg, "changed": changed, "applied_live": applied_live}
    })

    msg = f"✅ Configurazione applicata live ({len(changed)} campi modificati). Nessun restart necessario."
    logger.info(f"[CONFIG] {msg}")
    return {"ok": True, "saved": True, "applied_live": applied_live,
            "changed": changed, "config": updated_cfg, "message": msg}


@app.delete("/api/config")
async def reset_config():
    try:
        from trading_bot.config import settings as S
        S.reset_runtime()
        logger.info("[CONFIG] reset_runtime() OK — DB + memory svuotati")
    except Exception as e:
        logger.warning(f"[CONFIG] reset_runtime parziale: {e}")

    defaults = ConfigPayload().dict()
    await manager.broadcast({
        "type": "config_updated",
        "data": {"config": defaults, "changed": ["RESET"], "applied_live": True}
    })
    return {"ok": True, "message": "Config resettata — Railway Variables / default attivi", "config": defaults}


@app.post("/api/restart")
async def restart_bot():
    import sys
    import threading

    logger.warning("[RESTART] Riavvio self-exec richiesto dalla dashboard")

    await manager.broadcast({
        "type": "restarting",
        "data": {"message": "Bot in riavvio — riconnessione automatica tra 15-30s"}
    })

    def _do_restart():
        import time, os, sys
        time.sleep(1.5)
        try:
            try:
                import importlib
                main_mod = importlib.import_module("trading_bot.main")
                if hasattr(main_mod, "_bot_ref") and main_mod._bot_ref:
                    main_mod._bot_ref._running = False
                    time.sleep(4)
            except Exception:
                pass
            logger.warning(f"[RESTART] os.execv({sys.executable}, {sys.argv})")
            os.execv(sys.executable, [sys.executable] + sys.argv)
        except Exception as e:
            logger.error(f"[RESTART] execv fallito: {e} — sys.exit(1)")
            sys.exit(1)

    threading.Thread(target=_do_restart, daemon=False).start()
    return {"ok": True, "message": "Riavvio avviato — riconnessione automatica entro 20s", "eta_sec": 20}


@app.get("/api/sentiment")
async def get_sentiment():
    return _read_state().get("sentiment") or {"score": None, "label": "N/A"}


@app.get("/api/emerging")
async def get_emerging():
    return _read_state().get("emerging") or []


@app.get("/api/health")
async def health():
    info = {"ok": True, "ts": datetime.now(timezone.utc).isoformat()}
    try:
        from trading_bot.config import settings as S
        info["storage"]     = S.storage_backend()
        # FIX #3: _db_cache e _file_cache non esistono — il dict si chiama _cache
        info["config_keys"] = list(S._cache.keys())
        info["db_ok"]       = S._db_ok
    except Exception:
        pass
    return info


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        await ws.send_json({"type": "state", "data": _read_state()})
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(ws)
