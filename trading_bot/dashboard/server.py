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
    Sincronizza il bot con Bitget:
    1. Legge balance reale spot + futures
    2. Legge posizioni reali da Bitget API
    3. Confronta con risk_manager + DB
    4. ALLINEA: chiude trade fantasma nel DB, aggiunge trade mancanti
    5. Aggiorna risk_manager in memoria
    """
    try:
        from trading_bot.main import _bot_ref
        if not _bot_ref:
            return {"ok": False, "error": "Bot non avviato"}

        bot = _bot_ref
        from trading_bot.config import settings as S
        result = {"ok": True, "ts": datetime.now(timezone.utc).isoformat(), "actions": []}

        # ── 1. Balance reale ─────────────────────────────────────────────
        try:
            spot_bal = bot.exchange.get_usdt_balance("spot") if "spot" in S.MARKET_TYPES else 0
            fut_bal  = bot.exchange.get_usdt_balance("futures") if "futures" in S.MARKET_TYPES else 0
            total = spot_bal + fut_bal
            result["balance"] = {"spot": round(spot_bal, 4), "futures": round(fut_bal, 4), "total": round(total, 4)}
            if total > bot.risk.peak_balance:
                bot.risk.peak_balance = total
            bot.risk.session_start_balance = total
        except Exception as e:
            result["balance_error"] = str(e)

        # ── 2. Posizioni futures reali ───────────────────────────────────
        real_futures = []
        try:
            for p in bot.exchange.fetch_positions():
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

        # ── 3. Posizioni spot reali (token con saldo > 1 USDT) ───────────
        real_spot = []
        try:
            balance = bot.exchange.fetch_balance("spot")
            for asset, info in balance.items():
                if asset in ("USDT", "USD", "info", "free", "used", "total", "timestamp", "datetime"):
                    continue
                total_amt = float(info.get("total", 0) or 0)
                if total_amt > 0:
                    try:
                        ticker = bot.exchange.fetch_ticker(f"{asset}/USDT", "spot")
                        price = float(ticker.get("last", 0) or 0)
                        value = total_amt * price
                    except Exception:
                        price = 0; value = 0
                    if value > 1:
                        real_spot.append({
                            "asset": asset, "amount": round(total_amt, 6),
                            "price": round(price, 6), "value_usdt": round(value, 2),
                        })
            result["bitget_spot"] = real_spot
        except Exception as e:
            result["spot_error"] = str(e)

        # ── 4. Confronta e ALLINEA ───────────────────────────────────────
        bot_trades = bot.risk.all_open_trades()
        discrepancies = []
        actions = []

        # --- 4a. Trade nel bot ma NON su Bitget → chiudi nel DB + risk_manager ---
        real_fut_syms = {p["symbol"] for p in real_futures}
        real_spot_syms = {f"{s['asset']}/USDT" for s in real_spot}

        for t in bot_trades:
            sym = t["symbol"]
            mkt = t["market"]

            if mkt == "futures" and sym not in real_fut_syms:
                # Posizione fantasma: il bot pensa sia aperta ma Bitget dice no
                discrepancies.append({
                    "type": "GHOST_CLOSED",
                    "symbol": sym, "market": mkt,
                    "msg": f"CHIUSA {sym} futures — non più su Bitget"
                })
                # Chiudi nel risk_manager
                bot.risk.register_close(sym, 0, mkt, reason="sync_ghost")
                # Chiudi nel DB
                _sync_close_db_trade(sym, "sync_phantom_futures")
                actions.append(f"Rimossa {sym} futures (fantasma)")

            elif mkt == "spot" and sym not in real_spot_syms:
                discrepancies.append({
                    "type": "GHOST_CLOSED",
                    "symbol": sym, "market": mkt,
                    "msg": f"CHIUSA {sym} spot — non più su Bitget"
                })
                bot.risk.register_close(sym, 0, mkt, reason="sync_ghost")
                _sync_close_db_trade(sym, "sync_phantom_spot")
                actions.append(f"Rimossa {sym} spot (fantasma)")

        # --- 4b. Trade su Bitget ma NON nel bot → aggiungi al risk_manager + DB ---
        bot_fut_syms = {t["symbol"] for t in bot_trades if t["market"] == "futures"}
        bot_spot_syms = {t["symbol"] for t in bot_trades if t["market"] == "spot"}

        for p in real_futures:
            if p["symbol"] not in bot_fut_syms:
                discrepancies.append({
                    "type": "MISSING_FUTURES",
                    "symbol": p["symbol"], "side": p["side"],
                    "contracts": p["contracts"], "pnl": p["pnl"],
                    "msg": f"AGGIUNTA {p['symbol']} {p['side']} futures — trovata su Bitget"
                })
                # Aggiungi al risk_manager
                trade_data = {
                    "order_id":    f"sync_{int(time.time())}_{p['symbol'].replace('/','_')}",
                    "side":        "buy" if p["side"] == "long" else "sell",
                    "entry":       p["entry"],
                    "size":        p["contracts"],
                    "stop_loss":   0,
                    "take_profit": 0,
                    "strategy":    "SYNC_BITGET",
                    "confidence":  0,
                    "atr":         0,
                }
                bot.risk.register_open(p["symbol"], trade_data, "futures")
                # Salva nel DB
                _sync_open_db_trade(p, "futures")
                actions.append(f"Aggiunta {p['symbol']} {p['side']} futures (da Bitget)")

        for s in real_spot:
            spot_sym = f"{s['asset']}/USDT"
            if spot_sym not in bot_spot_syms:
                discrepancies.append({
                    "type": "MISSING_SPOT",
                    "symbol": spot_sym,
                    "amount": s["amount"], "value": s["value_usdt"],
                    "msg": f"AGGIUNTA {spot_sym} spot — trovata su Bitget"
                })
                trade_data = {
                    "order_id":    f"sync_{int(time.time())}_{s['asset']}",
                    "side":        "buy",
                    "entry":       s["price"],
                    "size":        s["amount"],
                    "stop_loss":   0,
                    "take_profit": 0,
                    "strategy":    "SYNC_BITGET",
                    "confidence":  0,
                    "atr":         0,
                }
                bot.risk.register_open(spot_sym, trade_data, "spot")
                _sync_open_db_trade_spot(s)
                actions.append(f"Aggiunta {spot_sym} spot (da Bitget)")

        # --- 4c. Trade presenti in entrambi: aggiorna size/entry nel DB ---
        for p in real_futures:
            if p["symbol"] in bot_fut_syms:
                _sync_update_db_trade(p["symbol"], p["entry"], p["contracts"], "futures")

        for s in real_spot:
            spot_sym = f"{s['asset']}/USDT"
            if spot_sym in bot_spot_syms:
                _sync_update_db_trade(spot_sym, s["price"], s["amount"], "spot")

        result["bot_positions"] = len(bot.risk.all_open_trades())
        result["bitget_positions"] = len(real_futures) + len(real_spot)
        result["discrepancies"] = discrepancies
        result["actions"] = actions
        result["synced"] = True

        # ── 5. Force state write ─────────────────────────────────────────
        try:
            from trading_bot.dashboard.state_writer import write_state
            write_state(bot)
        except Exception:
            pass

        await manager.broadcast({"type": "state", "data": _read_state()})

        logger.info(
            f"[SYNC] Bot={result['bot_positions']} Bitget={result['bitget_positions']} "
            f"Azioni={len(actions)} Disc={len(discrepancies)}"
        )
        for a in actions:
            logger.warning(f"[SYNC] {a}")

        return result

    except Exception as e:
        logger.error(f"[SYNC] Errore: {e}")
        return {"ok": False, "error": str(e)}


# ── Helpers DB per sync ──────────────────────────────────────────────────────

def _sync_close_db_trade(symbol: str, reason: str):
    """Chiude un trade nel DB (tabella trades) marcandolo come closed."""
    try:
        from trading_bot.config import settings as S
        if not S.DATABASE_URL:
            return
        from sqlalchemy import create_engine, text
        engine = create_engine(S.DATABASE_URL, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("""
                UPDATE trades SET status = 'closed', close_reason = :reason,
                    exit_price = entry_price, pnl_pct = 0, pnl_usdt = 0,
                    closed_at = NOW()
                WHERE symbol = :sym AND status = 'open'
            """), {"sym": symbol, "reason": reason})
            conn.commit()
        logger.info(f"[SYNC DB] Chiuso {symbol} → {reason}")
    except Exception as e:
        logger.warning(f"[SYNC DB] Close {symbol}: {e}")


def _sync_open_db_trade(p: dict, market: str):
    """Inserisce un trade nel DB dalla posizione Bitget futures."""
    try:
        from trading_bot.config import settings as S
        if not S.DATABASE_URL:
            return
        from sqlalchemy import create_engine, text
        engine = create_engine(S.DATABASE_URL, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO trades (order_id, symbol, market, strategy, side, status,
                    entry_price, stop_loss, take_profit, size, leverage, confidence,
                    atr, notes, timeframe, is_paper, opened_at)
                VALUES (:oid, :sym, :mkt, 'SYNC_BITGET', :side, 'open',
                    :entry, 0, 0, :size, :lev, 0,
                    0, 'Sincronizzato da Bitget', '15m', false, NOW())
                ON CONFLICT (order_id) DO NOTHING
            """), {
                "oid": f"sync_{int(time.time())}_{p['symbol'].replace('/','_').replace(':','')}",
                "sym": p["symbol"], "mkt": market,
                "side": "buy" if p.get("side") == "long" else "sell",
                "entry": p.get("entry", 0), "size": p.get("contracts", 0),
                "lev": p.get("lev", 10),
            })
            conn.commit()
        logger.info(f"[SYNC DB] Aperto {p['symbol']} futures")
    except Exception as e:
        logger.warning(f"[SYNC DB] Open futures {p.get('symbol','?')}: {e}")


def _sync_open_db_trade_spot(s: dict):
    """Inserisce un trade spot nel DB dal balance Bitget."""
    try:
        from trading_bot.config import settings as S
        if not S.DATABASE_URL:
            return
        from sqlalchemy import create_engine, text
        engine = create_engine(S.DATABASE_URL, pool_pre_ping=True)
        sym = f"{s['asset']}/USDT"
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO trades (order_id, symbol, market, strategy, side, status,
                    entry_price, stop_loss, take_profit, size, leverage, confidence,
                    atr, notes, timeframe, is_paper, opened_at)
                VALUES (:oid, :sym, 'spot', 'SYNC_BITGET', 'buy', 'open',
                    :entry, 0, 0, :size, 1, 0,
                    0, 'Sincronizzato da Bitget', '15m', false, NOW())
                ON CONFLICT (order_id) DO NOTHING
            """), {
                "oid": f"sync_{int(time.time())}_{s['asset']}",
                "sym": sym, "entry": s.get("price", 0), "size": s.get("amount", 0),
            })
            conn.commit()
        logger.info(f"[SYNC DB] Aperto {sym} spot")
    except Exception as e:
        logger.warning(f"[SYNC DB] Open spot {s.get('asset','?')}: {e}")


def _sync_update_db_trade(symbol: str, entry: float, size: float, market: str):
    """Aggiorna entry_price e size nel DB con i valori reali di Bitget."""
    try:
        from trading_bot.config import settings as S
        if not S.DATABASE_URL:
            return
        from sqlalchemy import create_engine, text
        engine = create_engine(S.DATABASE_URL, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("""
                UPDATE trades SET entry_price = :entry, size = :size
                WHERE symbol = :sym AND market = :mkt AND status = 'open'
            """), {"entry": entry, "size": size, "sym": symbol, "mkt": market})
            conn.commit()
    except Exception as e:
        logger.debug(f"[SYNC DB] Update {symbol}: {e}")


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

@app.post("/api/force-refresh")
async def force_refresh():
    """
    Forza refresh immediato di: sentiment, emerging, regime, state.
    Chiamato dal tasto ⚡ Force Refresh sulla dashboard.
    """
    result = {"ok": True, "ts": datetime.now(timezone.utc).isoformat()}
    try:
        from trading_bot.main import _bot_ref
        if not _bot_ref:
            return {"ok": False, "error": "Bot non avviato"}

        bot = _bot_ref

        # 1. Force sentiment refresh
        try:
            s = bot._sentiment.get_sentiment(force=True)
            result["sentiment"] = {"score": s.get("score"), "label": s.get("label")}
        except Exception as e:
            result["sentiment_error"] = str(e)

        # 2. Force emerging scan
        try:
            coins = bot._emerging.scan(force=True)
            result["emerging_count"] = len(coins)
            result["emerging_top"] = [c["symbol"] for c in coins[:5]]
        except Exception as e:
            result["emerging_error"] = str(e)

        # 3. Force regime check
        try:
            r = bot._regime.evaluate(bot)
            result["regime"] = r.get("current_label")
        except Exception as e:
            result["regime_error"] = str(e)

        # 4. Force state write
        try:
            from trading_bot.dashboard.state_writer import write_state
            write_state(bot)
            result["state_written"] = True
        except Exception as e:
            result["state_error"] = str(e)

        # 5. Broadcast updated state via WS
        await manager.broadcast({"type": "state", "data": _read_state()})

        logger.info(f"[FORCE REFRESH] Emerging={result.get('emerging_count',0)} Sentiment={result.get('sentiment',{}).get('score','?')}")
    except Exception as e:
        result["error"] = str(e)
    return result

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
