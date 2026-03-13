"""
Dashboard Server v4 — FastAPI + WebSocket + Regime Detection
═══════════════════════════════════════════════════════════════
Aggiunto:
  ✓ /api/regime — stato regime attuale
  ✓ Override manuale: quando l'utente applica una config,
    il regime detector blocca auto-switch per 2h
  ✓ Regime state incluso nel push WS
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
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

_DIR       = os.path.dirname(__file__)
STATE_FILE = os.path.join(_DIR, "dashboard_state.json")
HTML_FILE  = os.path.join(_DIR, "dashboard.html")


# ════════════════════════════════════════════════════════════════════════════
# CONFIG MODEL
# ════════════════════════════════════════════════════════════════════════════

class ConfigPayload(BaseModel):
    MAX_RISK_PCT:           float = Field(3.5,  ge=0.5,  le=15.0)
    DEFAULT_LEVERAGE:       int   = Field(5,    ge=1,    le=20)
    MAX_DAILY_LOSS_PCT:     float = Field(8.0,  ge=1.0,  le=30.0)
    MAX_DRAWDOWN_PCT:       float = Field(15.0, ge=5.0,  le=50.0)
    TAKE_PROFIT_RATIO:      float = Field(2.5,  ge=1.0,  le=5.0)
    TRAILING_STOP_PCT:      float = Field(1.2,  ge=0.1,  le=5.0)
    MIN_CONFIDENCE:         float = Field(65.0, ge=40.0, le=95.0)
    MAX_POSITIONS_SPOT:     int   = Field(4,    ge=1,    le=10)
    MAX_POSITIONS_FUTURES:  int   = Field(3,    ge=1,    le=10)
    MARGIN_MODE:            str   = Field("isolated")
    MAX_NOTIONAL_PCT:       float = Field(40.0, ge=10.0, le=95.0)
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


# ── Config I/O ────────────────────────────────────────────────────────────────

def _read_config() -> dict:
    try:
        from trading_bot.config import settings as S
        data = S.as_dict(force=True)
        if data is not None:
            data["_storage_backend"] = S.storage_backend()
            data["_source"] = "postgresql" if S._db_ok else "memory"
            return data
    except Exception as e:
        logger.warning(f"[CONFIG] _read_config fallito: {e}")
    fallback = ConfigPayload().dict()
    fallback["_storage_backend"] = "fallback_defaults"
    fallback["_source"] = "pydantic_defaults"
    return fallback


def _apply_to_settings(cfg: dict) -> list[str]:
    try:
        from trading_bot.config import settings as S
        changed = S.set_many(cfg)
        if changed:
            for ch in changed:
                logger.info(f"[CONFIG LIVE] {ch}")
        return changed
    except Exception as e:
        logger.warning(f"[CONFIG] Errore apply: {e}")
        return []


def _notify_regime_override():
    """Notifica il regime detector che l'utente ha applicato manualmente."""
    try:
        from trading_bot.main import _bot_ref
        if _bot_ref and hasattr(_bot_ref, '_regime'):
            _bot_ref._regime.set_manual_override()
            logger.info("[REGIME] Override manuale impostato — auto-switch bloccato 2h")
    except Exception:
        pass


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
        "sentiment": None, "emerging": [], "regime": None,
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
        if ws in self.active: self.active.remove(ws)
    async def broadcast(self, msg: dict):
        dead = []
        for ws in self.active:
            try: await ws.send_json(msg)
            except: dead.append(ws)
        for ws in dead: self.disconnect(ws)

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
        with open(HTML_FILE) as f: return f.read()
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
    try:
        raw = await request.json()
    except Exception:
        raw = {}

    current = _read_config()
    current.pop("_storage_backend", None)
    current.pop("_source", None)
    merged = {**current, **raw}

    from pydantic import ValidationError
    try:
        ConfigPayload(**merged)
    except ValidationError as ve:
        return {"ok": False, "error": str(ve)}

    changed = _apply_to_settings(merged)

    # ── NUOVO: Notifica regime detector dell'override manuale ────────
    _notify_regime_override()

    updated_cfg = _read_config()
    updated_cfg.pop("_storage_backend", None)
    updated_cfg.pop("_source", None)

    await manager.broadcast({
        "type": "config_updated",
        "data": {"config": updated_cfg, "changed": changed, "applied_live": True}
    })

    return {"ok": True, "saved": True, "applied_live": True,
            "changed": changed, "config": updated_cfg}


@app.delete("/api/config")
async def reset_config():
    try:
        from trading_bot.config import settings as S
        S.reset_runtime()
    except Exception as e:
        logger.warning(f"[CONFIG] reset: {e}")
    defaults = ConfigPayload().dict()
    await manager.broadcast({
        "type": "config_updated",
        "data": {"config": defaults, "changed": ["RESET"], "applied_live": True}
    })
    return {"ok": True, "config": defaults}


@app.get("/api/regime")
async def get_regime():
    """Stato corrente del regime detector."""
    try:
        from trading_bot.main import _bot_ref
        if _bot_ref and hasattr(_bot_ref, '_regime'):
            return _bot_ref._regime.get_state()
    except Exception:
        pass
    return {"current_regime": "normal", "current_label": "🚀 Raddoppio ×2", "auto_enabled": True}


@app.post("/api/sync")
async def sync_bitget():
    """
    Sincronizza le posizioni del bot con quelle REALI su Bitget.
    1. Legge balance reale spot + futures
    2. Legge posizioni futures aperte da Bitget API
    3. Confronta con risk_manager e segnala discrepanze
    4. Opzionale: allinea il risk_manager
    """
    try:
        from trading_bot.main import _bot_ref
        if not _bot_ref:
            return {"ok": False, "error": "Bot non avviato"}

        bot = _bot_ref
        result = {"ok": True, "ts": datetime.now(timezone.utc).isoformat()}

        # ── 1. Balance reale ─────────────────────────────────────────────
        try:
            from trading_bot.config import settings as S
            spot_bal = bot.exchange.get_usdt_balance("spot") if "spot" in S.MARKET_TYPES else 0
            fut_bal  = bot.exchange.get_usdt_balance("futures") if "futures" in S.MARKET_TYPES else 0
            result["balance"] = {"spot": round(spot_bal, 4), "futures": round(fut_bal, 4), "total": round(spot_bal + fut_bal, 4)}
            # Update peak
            total = spot_bal + fut_bal
            if total > bot.risk.peak_balance:
                bot.risk.peak_balance = total
            bot.risk.session_start_balance = total
        except Exception as e:
            result["balance_error"] = str(e)

        # ── 2. Posizioni futures reali da Bitget ─────────────────────────
        real_futures = []
        try:
            positions = bot.exchange.fetch_positions()
            for p in positions:
                contracts = float(p.get("contracts", 0) or 0)
                if contracts == 0:
                    continue
                real_futures.append({
                    "symbol":    p.get("symbol", ""),
                    "side":      p.get("side", ""),
                    "contracts": contracts,
                    "notional":  float(p.get("notional", 0) or 0),
                    "entry":     float(p.get("entryPrice", 0) or 0),
                    "mark":      float(p.get("markPrice", 0) or 0),
                    "pnl":       float(p.get("unrealizedPnl", 0) or 0),
                    "margin":    float(p.get("initialMargin", 0) or 0),
                    "lev":       int(float(p.get("leverage", 1) or 1)),
                    "liq":       float(p.get("liquidationPrice", 0) or 0),
                })
            result["bitget_futures"] = real_futures
        except Exception as e:
            result["futures_error"] = str(e)

        # ── 3. Posizioni spot reali (solo i token con saldo > 0) ─────────
        real_spot = []
        try:
            balance = bot.exchange.fetch_balance("spot")
            for asset, info in balance.items():
                if asset in ("USDT", "USD", "info", "free", "used", "total", "timestamp", "datetime"):
                    continue
                total_amt = float(info.get("total", 0) or 0)
                if total_amt > 0:
                    # Stima valore
                    try:
                        ticker = bot.exchange.fetch_ticker(f"{asset}/USDT", "spot")
                        price = float(ticker.get("last", 0) or 0)
                        value = total_amt * price
                    except Exception:
                        price = 0; value = 0
                    if value > 1:  # ignora dust < 1 USDT
                        real_spot.append({
                            "asset": asset, "amount": round(total_amt, 6),
                            "price": round(price, 6), "value_usdt": round(value, 2),
                        })
            result["bitget_spot"] = real_spot
        except Exception as e:
            result["spot_error"] = str(e)

        # ── 4. Confronta con risk_manager ────────────────────────────────
        bot_trades = bot.risk.all_open_trades()
        bot_syms = {t["symbol"] for t in bot_trades}
        real_syms = {p["symbol"] for p in real_futures}
        # Spot check
        spot_syms_bot = {t["symbol"] for t in bot_trades if t["market"] == "spot"}
        spot_syms_real = {f"{s['asset']}/USDT" for s in real_spot}

        discrepancies = []
        # Futures su Bitget ma non nel bot
        for p in real_futures:
            if p["symbol"] not in bot_syms:
                discrepancies.append({
                    "type": "PHANTOM_FUTURES",
                    "symbol": p["symbol"],
                    "side": p["side"],
                    "contracts": p["contracts"],
                    "pnl": p["pnl"],
                    "msg": f"{p['symbol']} {p['side']} aperta su Bitget ma NON tracciata dal bot"
                })
        # Nel bot ma non su Bitget
        for t in bot_trades:
            if t["market"] == "futures" and t["symbol"] not in real_syms:
                discrepancies.append({
                    "type": "GHOST_TRADE",
                    "symbol": t["symbol"],
                    "msg": f"{t['symbol']} tracciata dal bot ma NON presente su Bitget"
                })

        result["bot_positions"] = len(bot_trades)
        result["bitget_positions"] = len(real_futures) + len(real_spot)
        result["discrepancies"] = discrepancies
        result["synced"] = len(discrepancies) == 0

        # ── 5. Log ───────────────────────────────────────────────────────
        logger.info(
            f"[SYNC] Bot={len(bot_trades)} Bitget={len(real_futures)}fut+{len(real_spot)}spot "
            f"Discrepanze={len(discrepancies)}"
        )
        for d in discrepancies:
            logger.warning(f"[SYNC] {d['type']}: {d['msg']}")

        return result

    except Exception as e:
        logger.error(f"[SYNC] Errore: {e}")
        return {"ok": False, "error": str(e)}


@app.post("/api/restart")
async def restart_bot():
    import threading
    logger.warning("[RESTART] Richiesto dalla dashboard")
    await manager.broadcast({"type": "restarting", "data": {"message": "Bot in riavvio..."}})
    def _do_restart():
        import time, os, sys
        time.sleep(1.5)
        try:
            os.execv(sys.executable, [sys.executable] + sys.argv)
        except Exception as e:
            logger.error(f"[RESTART] fallito: {e}")
            sys.exit(1)
    threading.Thread(target=_do_restart, daemon=False).start()
    return {"ok": True, "eta_sec": 20}


@app.get("/api/sentiment")
async def get_sentiment():
    return _read_state().get("sentiment") or {"score": None}

@app.get("/api/emerging")
async def get_emerging():
    return _read_state().get("emerging") or []

@app.get("/api/health")
async def health():
    info = {"ok": True, "ts": datetime.now(timezone.utc).isoformat()}
    try:
        from trading_bot.config import settings as S
        info["storage"] = S.storage_backend()
        info["db_ok"]   = S._db_ok
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
