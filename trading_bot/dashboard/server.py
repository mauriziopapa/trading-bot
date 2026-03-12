"""
Dashboard Server — FastAPI + WebSocket
Serve la UI e fa push dello stato ogni 3 secondi.
"""

import os
import json
import asyncio
from datetime import datetime, timezone
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from loguru import logger

app = FastAPI(title="Bitget Bot Dashboard")

STATE_FILE = os.path.join(os.path.dirname(__file__), "dashboard_state.json")
HTML_FILE  = os.path.join(os.path.dirname(__file__), "dashboard.html")


def _read_state() -> dict:
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE) as f:
                return json.load(f)
    except Exception:
        pass
    return _demo_state()


def _demo_state() -> dict:
    now = datetime.now(timezone.utc).isoformat()
    return {
        "mode": "paper", "status": "starting",
        "last_update": now,
        "balance": {"spot": 0, "futures": 0, "total": 0,
                    "pnl_today_pct": 0, "pnl_today_usdt": 0},
        "positions": [], "signals": [],
        "logs": [{"ts": now, "level": "INFO", "msg": "Dashboard avviata — in attesa del bot..."}],
        "stats": {"total_trades": 0, "wins": 0, "losses": 0,
                  "win_rate": 0, "avg_win_pct": 0, "avg_loss_pct": 0,
                  "daily_pnl": 0, "daily_trades": 0},
        "sentiment": None,
        "emerging": [],
    }


# ── WebSocket Manager ────────────────────────────────────────────────────────

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


async def _push_loop():
    while True:
        await asyncio.sleep(3)
        if manager.active:
            await manager.broadcast({"type": "state", "data": _read_state()})


# ── Endpoints ────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    try:
        with open(HTML_FILE) as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>Dashboard HTML not found</h1>"


@app.get("/api/state")
async def get_state():
    return _read_state()


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
