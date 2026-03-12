"""
Dashboard Server — FastAPI + WebSocket
Serve la UI e fa push dello stato ogni 3 secondi.
Include /api/config per aggiornare la configurazione del bot a runtime.
"""

import os
import json
import asyncio
from datetime import datetime, timezone

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
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

_DIR        = os.path.dirname(__file__)
STATE_FILE  = os.path.join(_DIR, "dashboard_state.json")
CONFIG_FILE = os.path.join(_DIR, "runtime_config.json")
HTML_FILE   = os.path.join(_DIR, "dashboard.html")


# ════════════════════════════════════════════════════════════════════════════
# CONFIG MODEL — Pydantic valida ogni campo prima di salvarlo
# ════════════════════════════════════════════════════════════════════════════

class ConfigPayload(BaseModel):
    # Risk
    MAX_RISK_PCT:           float = Field(3.5,  ge=0.5,  le=10.0)
    DEFAULT_LEVERAGE:       int   = Field(5,    ge=1,    le=20)
    MAX_DAILY_LOSS_PCT:     float = Field(8.0,  ge=1.0,  le=30.0)
    MAX_DRAWDOWN_PCT:       float = Field(15.0, ge=5.0,  le=50.0)
    TAKE_PROFIT_RATIO:      float = Field(2.5,  ge=1.0,  le=5.0)
    TRAILING_STOP_PCT:      float = Field(1.2,  ge=0.1,  le=5.0)
    # Positions
    MIN_CONFIDENCE:         float = Field(65.0, ge=40.0, le=95.0)
    MAX_POSITIONS_SPOT:     int   = Field(4,    ge=1,    le=10)
    MAX_POSITIONS_FUTURES:  int   = Field(3,    ge=1,    le=10)
    MARGIN_MODE:            str   = Field("isolated")
    # Strategies on/off
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
        extra = "ignore"   # ignora campi extra dalla dashboard


# ── Config I/O ───────────────────────────────────────────────────────────────

def _read_config() -> dict:
    """Legge runtime_config.json. Fallback ai default Pydantic."""
    try:
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE) as f:
                return json.load(f)
    except Exception:
        pass
    return ConfigPayload().dict()


def _write_config(cfg: dict) -> None:
    cfg["_updated_at"] = datetime.now(timezone.utc).isoformat()
    with open(CONFIG_FILE, "w") as f:
        json.dump(cfg, f, indent=2)
    logger.info(f"[CONFIG] Salvato in {CONFIG_FILE}")


def _apply_to_settings(cfg: dict) -> list[str]:
    """
    Applica la nuova config direttamente al modulo settings a runtime.
    Funziona solo se bot e dashboard girano nello STESSO processo (start.sh).
    Ritorna la lista dei campi effettivamente modificati.
    """
    changed = []
    try:
        from trading_bot.config import settings as S

        fields = [
            "MAX_RISK_PCT", "DEFAULT_LEVERAGE", "MAX_DAILY_LOSS_PCT",
            "MAX_DRAWDOWN_PCT", "TAKE_PROFIT_RATIO", "TRAILING_STOP_PCT",
            "MAX_POSITIONS_SPOT", "MAX_POSITIONS_FUTURES", "MARGIN_MODE",
            "ENABLE_RSI_MACD", "ENABLE_BOLLINGER", "ENABLE_BREAKOUT",
            "ENABLE_SCALPING",
        ]
        for field in fields:
            if field in cfg and hasattr(S, field):
                old = getattr(S, field)
                new = cfg[field]
                if old != new:
                    setattr(S, field, new)
                    changed.append(f"{field}: {old} → {new}")
                    logger.info(f"[CONFIG LIVE] {field}: {old} → {new}")

    except ImportError:
        # Processo separato — nessun apply live, solo file
        logger.debug("[CONFIG] Processo separato — solo file aggiornato")

    return changed


# ── State I/O ─────────────────────────────────────────────────────────────────

def _read_state() -> dict:
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE) as f:
                data = json.load(f)
            data["config"] = _read_config()   # inietta config nello state
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


# ── State ─────────────────────────────────────────────────────────────────────

@app.get("/api/state")
async def get_state():
    return _read_state()


# ── Config ────────────────────────────────────────────────────────────────────

@app.get("/api/config")
async def get_config():
    """Ritorna la configurazione runtime corrente."""
    return _read_config()


@app.post("/api/config")
async def update_config(payload: ConfigPayload):
    """
    Aggiorna la configurazione del bot a runtime.

    Flusso:
      1. Pydantic valida tutti i campi (tipi, range, enum)
      2. Salva su runtime_config.json (persistente tra restart)
      3. Tenta apply diretto a settings (solo se stesso processo)
      4. Notifica tutti i client WS con tipo 'config_updated'
      5. Ritorna { ok, saved, applied_live, changed[], message }

    applied_live=True  → cambio immediato, nessun restart necessario
    applied_live=False → cambio al prossimo restart del bot
    """
    cfg = payload.dict()

    # 1. Salva
    _write_config(cfg)

    # 2. Apply live
    changed = _apply_to_settings(cfg)
    applied_live = len(changed) > 0

    # 3. Notifica WebSocket
    await manager.broadcast({
        "type": "config_updated",
        "data": {
            "config":       cfg,
            "changed":      changed,
            "applied_live": applied_live,
        }
    })

    # 4. Messaggio risposta
    if applied_live:
        msg = f"✅ Configurazione applicata live ({len(changed)} campi modificati). Nessun restart necessario."
    else:
        msg = "💾 Configurazione salvata su file. Verrà applicata al prossimo avvio del bot."

    logger.info(f"[CONFIG] {msg}")

    return {
        "ok":           True,
        "saved":        True,
        "applied_live": applied_live,
        "changed":      changed,
        "config":       cfg,
        "message":      msg,
    }


# ── Accessori ─────────────────────────────────────────────────────────────────

@app.post("/api/restart")
async def restart_bot():
    """
    Riavvia il processo del bot in modo graceful.
    Funziona su Railway perché il processo principale (python -m trading_bot.main)
    viene riavviato dallo scheduler Railway quando esce con codice 0.

    Strategia:
    - Segnala al bot di fermarsi (setta _bot_ref._running = False)
    - Il loop principale esce, Railway/Procfile rilancia automaticamente
    - Se il bot non è nello stesso processo, usa os.kill(os.getpid(), SIGTERM)
      che Railway intercetta e riavvia
    """
    import os
    import signal
    import threading

    logger.warning("[RESTART] Riavvio richiesto dalla dashboard")

    # Notifica i client WS prima di morire
    await manager.broadcast({
        "type": "restarting",
        "data": {"message": "Bot in riavvio — riconnessione automatica tra 15-30s"}
    })

    # Lascia 1s per la consegna del messaggio WS, poi triggera il riavvio
    def _do_restart():
        import time
        time.sleep(1)
        try:
            # Prova prima a fermare il bot via _running flag
            # (funziona se bot e dashboard sono stesso processo via start.sh)
            import importlib
            try:
                main_mod = importlib.import_module("trading_bot.main")
                if hasattr(main_mod, "_bot_ref") and main_mod._bot_ref:
                    main_mod._bot_ref._running = False
                    logger.info("[RESTART] _running = False — bot si fermerà al prossimo ciclo")
                    time.sleep(6)   # aspetta che il loop finisca
            except Exception:
                pass
            # SIGTERM → Railway rileva l'uscita e fa redeploy
            logger.warning("[RESTART] SIGTERM — Railway riavvierà il servizio")
            os.kill(os.getpid(), signal.SIGTERM)
        except Exception as e:
            logger.error(f"[RESTART] Errore: {e}")

    threading.Thread(target=_do_restart, daemon=True).start()

    return {
        "ok":      True,
        "message": "Riavvio avviato — il servizio Railway si riavvierà automaticamente",
        "eta_sec": 20,
    }


@app.get("/api/sentiment")
async def get_sentiment():
    return _read_state().get("sentiment") or {"score": None, "label": "N/A"}


@app.get("/api/emerging")
async def get_emerging():
    return _read_state().get("emerging") or []


@app.get("/api/health")
async def health():
    return {"ok": True, "ts": datetime.now(timezone.utc).isoformat()}


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        await ws.send_json({"type": "state", "data": _read_state()})
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(ws)
