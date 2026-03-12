"""
Dashboard Server — FastAPI + WebSocket
Serve la UI, fa push dello stato ogni 3 secondi,
espone /api/config per aggiornare la configurazione a runtime.
"""

import os
import json
import asyncio
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
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

# ── File paths ───────────────────────────────────────────────────────────────

_HERE       = os.path.dirname(__file__)
STATE_FILE  = os.path.join(_HERE, "dashboard_state.json")
CONFIG_FILE = os.path.join(_HERE, "runtime_config.json")
HTML_FILE   = os.path.join(_HERE, "dashboard.html")


# ════════════════════════════════════════════════════════════════════════════
# CONFIG MODEL — validazione Pydantic con limiti di sicurezza
# ════════════════════════════════════════════════════════════════════════════

class ConfigUpdate(BaseModel):
    # Risk
    MAX_RISK_PCT:          Optional[float] = Field(None, ge=0.5,  le=10.0,  description="Rischio % per trade")
    DEFAULT_LEVERAGE:      Optional[int]   = Field(None, ge=1,    le=20,    description="Leva futures")
    MAX_DAILY_LOSS_PCT:    Optional[float] = Field(None, ge=1.0,  le=30.0,  description="Max loss giornaliero %")
    MAX_DRAWDOWN_PCT:      Optional[float] = Field(None, ge=5.0,  le=50.0,  description="Max drawdown %")
    TAKE_PROFIT_RATIO:     Optional[float] = Field(None, ge=1.0,  le=5.0,   description="TP = SL × ratio")
    TRAILING_STOP_PCT:     Optional[float] = Field(None, ge=0.1,  le=5.0,   description="Trailing stop %")

    # Posizioni
    MAX_POSITIONS_SPOT:    Optional[int]   = Field(None, ge=1,    le=10)
    MAX_POSITIONS_FUTURES: Optional[int]   = Field(None, ge=1,    le=10)
    MIN_CONFIDENCE:        Optional[float] = Field(None, ge=40.0, le=95.0,  description="Confidenza minima segnale")
    MARGIN_MODE:           Optional[str]   = Field(None, description="isolated | cross")

    # Strategie (on/off)
    ENABLE_RSI_MACD:    Optional[bool] = None
    ENABLE_BOLLINGER:   Optional[bool] = None
    ENABLE_BREAKOUT:    Optional[bool] = None
    ENABLE_SCALPING:    Optional[bool] = None
    ENABLE_EMERGING:    Optional[bool] = None

    @validator("MARGIN_MODE")
    def validate_margin_mode(cls, v):
        if v is not None and v not in ("isolated", "cross"):
            raise ValueError("MARGIN_MODE deve essere 'isolated' o 'cross'")
        return v

    class Config:
        extra = "forbid"   # rifiuta campi non dichiarati


# ════════════════════════════════════════════════════════════════════════════
# CONFIG STORAGE — legge/scrive runtime_config.json
# Questo file viene letto da settings.py ad ogni accesso se RUNTIME_CONFIG=1
# ════════════════════════════════════════════════════════════════════════════

def _read_config() -> dict:
    try:
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE) as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def _write_config(data: dict) -> dict:
    """
    Merge del nuovo config con quello esistente e salvataggio.
    Ritorna il config completo aggiornato.
    """
    current = _read_config()
    current.update({k: v for k, v in data.items() if v is not None})
    current["_updated_at"] = datetime.now(timezone.utc).isoformat()

    with open(CONFIG_FILE, "w") as f:
        json.dump(current, f, indent=2)

    logger.info(f"[CONFIG] Aggiornato: {list(data.keys())}")
    return current


def _apply_to_settings(cfg: dict):
    """
    Applica la config al modulo settings a runtime, senza riavvio.
    Funziona perché settings è importato come modulo singleton.
    """
    try:
        from trading_bot.config import settings
        for key, val in cfg.items():
            if key.startswith("_"):
                continue
            if hasattr(settings, key):
                old = getattr(settings, key)
                setattr(settings, key, val)
                logger.info(f"[CONFIG] settings.{key}: {old} → {val}")
            else:
                logger.debug(f"[CONFIG] Campo ignorato (non in settings): {key}")
    except ImportError:
        # Dashboard standalone senza il bot — solo persiste su file
        logger.debug("[CONFIG] settings non disponibile — solo persistenza su file")


# ════════════════════════════════════════════════════════════════════════════
# STATE
# ════════════════════════════════════════════════════════════════════════════

def _read_state() -> dict:
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE) as f:
                data = json.load(f)
                # Inietta la config corrente nello state per la dashboard
                data["runtime_config"] = _read_config()
                return data
    except Exception:
        pass
    return _demo_state()


def _demo_state() -> dict:
    now = datetime.now(timezone.utc).isoformat()
    return {
        "mode": "paper", "status": "starting",
        "last_update": now,
        "balance":  {"spot": 0, "futures": 0, "total": 0, "pnl_today_pct": 0, "pnl_today_usdt": 0},
        "positions": [], "signals": [],
        "logs":     [{"ts": now, "level": "INFO", "msg": "Dashboard avviata — in attesa del bot..."}],
        "stats":    {"total_trades": 0, "wins": 0, "losses": 0,
                     "win_rate": 0, "avg_win_pct": 0, "avg_loss_pct": 0,
                     "daily_pnl": 0, "daily_trades": 0},
        "sentiment":       None,
        "emerging":        [],
        "runtime_config":  _read_config(),
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

    async def broadcast(self, data: dict):
        dead = []
        for ws in self.active:
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


manager = ConnectionManager()


@app.on_event("startup")
async def start_pusher():
    asyncio.create_task(_push_loop())
    # Carica config salvata e applicala a settings al boot
    saved = _read_config()
    if saved:
        _apply_to_settings(saved)
        logger.info(f"[CONFIG] Caricato runtime_config.json ({len(saved)} parametri)")


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
        return "<h1>Dashboard HTML not found — dashboard.html mancante</h1>"


@app.get("/api/state")
async def get_state():
    return _read_state()


@app.get("/api/config")
async def get_config():
    """Ritorna la configurazione runtime corrente."""
    return {
        "config":     _read_config(),
        "last_saved": _read_config().get("_updated_at"),
    }


@app.post("/api/config")
async def update_config(body: ConfigUpdate):
    """
    Aggiorna la configurazione del bot a runtime.

    - Valida i valori con i limiti di sicurezza definiti nel modello
    - Persiste su runtime_config.json (sopravvive ai riavvii)
    - Applica immediatamente a settings se il bot è in esecuzione
    - Fa broadcast ai client WebSocket connessi con il nuovo stato

    Esempio body:
    {
        "MAX_RISK_PCT": 5.0,
        "DEFAULT_LEVERAGE": 10,
        "ENABLE_SCALPING": true
    }
    """
    # Filtra solo i campi effettivamente inviati (non None)
    updates = {k: v for k, v in body.dict().items() if v is not None}

    if not updates:
        raise HTTPException(status_code=400, detail="Nessun parametro valido ricevuto")

    # Persiste
    new_config = _write_config(updates)

    # Applica a runtime senza riavvio
    _apply_to_settings(updates)

    # Notifica tutti i client WebSocket
    await manager.broadcast({
        "type": "config_updated",
        "data": {
            "config":  new_config,
            "changes": updates,
            "ts":      datetime.now(timezone.utc).isoformat(),
        }
    })

    logger.info(f"[CONFIG] POST /api/config → {updates}")

    return {
        "ok":      True,
        "applied": updates,
        "config":  new_config,
        "message": f"{len(updates)} parametri aggiornati e applicati a runtime",
    }


@app.delete("/api/config")
async def reset_config():
    """Resetta tutta la configurazione runtime ai valori di default (da env vars)."""
    if os.path.exists(CONFIG_FILE):
        os.remove(CONFIG_FILE)
    logger.warning("[CONFIG] Runtime config resettata ai default")
    return {"ok": True, "message": "Config resettata — i valori tornano alle env vars di Railway"}


@app.get("/api/sentiment")
async def get_sentiment():
    state = _read_state()
    return state.get("sentiment") or {"score": None, "label": "N/A"}


@app.get("/api/emerging")
async def get_emerging():
    state = _read_state()
    return state.get("emerging") or []


@app.get("/api/health")
async def health():
    return {
        "ok":            True,
        "ts":            datetime.now(timezone.utc).isoformat(),
        "config_active": bool(_read_config()),
        "ws_clients":    len(manager.active),
    }


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        # Invia stato iniziale + config corrente
        await ws.send_json({"type": "state", "data": _read_state()})
        while True:
            msg = await ws.receive_text()
            # Gestisci ping dal client
            if msg == "ping":
                await ws.send_json({"type": "pong"})
    except WebSocketDisconnect:
        manager.disconnect(ws)
