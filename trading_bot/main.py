"""
Bitget Trading Bot — Main Orchestrator v4
═══════════════════════════════════════════════════════════════
v4 FIX:
  ✓ RegimeDetector importato, inizializzato, schedulato
  ✓ _check_regime() definito + _check_regime_urgent() dopo close
  ✓ Tutti i fix v3 mantenuti
"""

import os, time, schedule
from datetime import datetime, timezone
from loguru import logger
from trading_bot.config import settings

def _cfg_float(key, default):
    try:
        val = settings.as_dict().get(key)
        return float(val) if val is not None else default
    except: return default

from trading_bot.utils.exchange import BitgetExchange
from trading_bot.utils.risk_manager import RiskManager
from trading_bot.utils.notifier import TelegramNotifier
from trading_bot.utils.indicators import ohlcv_to_df
from trading_bot.models.database import DB
from trading_bot.strategies.base import Signal
from trading_bot.strategies.rsi_macd import RSIMACDStrategy
from trading_bot.strategies.bollinger import BollingerStrategy
from trading_bot.strategies.breakout import BreakoutStrategy
from trading_bot.strategies.scalping import ScalpingStrategy
from trading_bot.utils.sentiment_analyzer import SentimentAnalyzer
from trading_bot.utils.emerging_scanner import EmergingScanner
from trading_bot.utils.regime_detector import RegimeDetector

if settings.ENABLE_DASHBOARD:
    try:
        from trading_bot.dashboard.state_writer import write_state
        DASHBOARD_ENABLED = True
    except:
        DASHBOARD_ENABLED = False; write_state = None
else:
    DASHBOARD_ENABLED = False; write_state = None

_bot_ref = None

def _setup_logger():
    os.makedirs("logs", exist_ok=True); logger.remove()
    logger.add("logs/bot.log", rotation="50 MB", retention="14 days", level=settings.LOG_LEVEL, format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}")
    logger.add(lambda msg: print(msg, end=""), level=settings.LOG_LEVEL, colorize=True, format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}")
    def _to_buf(msg):
        if _bot_ref is None: return
        _bot_ref._recent_logs.append({"ts": msg.record["time"].isoformat(), "level": msg.record["level"].name, "msg": msg.record["message"]})
        _bot_ref._recent_logs = _bot_ref._recent_logs[-100:]
    logger.add(_to_buf, level="DEBUG")

class TradingBot:
    def __init__(self):
        self.exchange = BitgetExchange(); self.risk = RiskManager()
        self.notifier = TelegramNotifier(); self.db = DB()
        self._sentiment = SentimentAnalyzer(); self._emerging = EmergingScanner()
        self._regime = RegimeDetector()
        self._recent_signals = []; self._recent_logs = []
        self.strategies = []
        if settings.ENABLE_RSI_MACD: self.strategies.append(RSIMACDStrategy())
        if settings.ENABLE_BOLLINGER: self.strategies.append(BollingerStrategy())
        if settings.ENABLE_BREAKOUT: self.strategies.append(BreakoutStrategy())
        if settings.ENABLE_SCALPING: self.strategies.append(ScalpingStrategy())
        self._running = True

    def start(self):
        global _bot_ref; _bot_ref = self; _setup_logger()
        # Condividi il bot con il server (funziona se stesso processo)
        try:
            from trading_bot.utils.shared import set_bot
            set_bot(self)
        except Exception:
            pass
        logger.info("=" * 60); logger.info("BITGET TRADING BOT v4"); logger.info(f"Mode: {settings.TRADING_MODE} | Regime: AUTO"); logger.info("=" * 60)
        # ── Dashboard: avvia uvicorn NELLO STESSO PROCESSO ────────────────
        if DASHBOARD_ENABLED:
            self._start_dashboard_server()
        self.exchange.initialize(); self.db.connect(); self.risk.recover_from_db(); self._sync_balance()
        if settings.IS_LIVE: logger.warning("*** LIVE — ORDINI REALI ***")
        try:
            s = self._sentiment.get_sentiment(); logger.info(f"Sentiment: {s['label']} ({s['score']})")
        except Exception as e: logger.warning(f"Sentiment init: {e}")
        try:
            em = self._emerging.scan()
            if em: logger.info(f"Emerging: {[c['symbol'] for c in em]}")
        except Exception as e: logger.warning(f"Emerging init: {e}")
        _bs = _bf = 0.0
        try:
            if "spot" in settings.MARKET_TYPES: _bs = self.exchange.get_usdt_balance("spot")
            if "futures" in settings.MARKET_TYPES: _bf = self.exchange.get_usdt_balance("futures")
            self.risk.session_start_balance = _bs + _bf; self.risk.peak_balance = _bs + _bf
            logger.info(f"Balance: Spot={_bs:.2f} Fut={_bf:.2f}")
        except Exception as e: logger.warning(f"Balance: {e}")
        self.notifier.startup(settings.TRADING_MODE, settings.SPOT_SYMBOLS if "spot" in settings.MARKET_TYPES else [], settings.FUTURES_SYMBOLS if "futures" in settings.MARKET_TYPES else [], _bs, _bf)
        self._check_regime()
        if settings.ENABLE_RSI_MACD or settings.ENABLE_BOLLINGER: schedule.every(2).minutes.do(self._scan_swing)
        if settings.ENABLE_BREAKOUT: schedule.every(5).minutes.do(self._scan_breakout)
        if settings.ENABLE_SCALPING: schedule.every(45).seconds.do(self._scan_scalping)
        schedule.every(3).minutes.do(self._scan_emerging)
        schedule.every(30).seconds.do(self._monitor_positions)
        schedule.every(1).hours.do(self._health_check)
        schedule.every(8).minutes.do(lambda: self._sentiment.get_sentiment(force=True))
        schedule.every(5).minutes.do(lambda: self._emerging.scan(force=True))
        schedule.every(5).minutes.do(self._check_regime)
        schedule.every(10).minutes.do(self._auto_rebalance)
        schedule.every().day.at("00:05").do(self._daily_report)
        if DASHBOARD_ENABLED: schedule.every(20).seconds.do(lambda: write_state(self))
        self._auto_rebalance()
        self._scan_swing(); self._scan_breakout(); self._scan_emerging()
        if DASHBOARD_ENABLED: write_state(self)
        logger.info("Bot operativo")
        while self._running: schedule.run_pending(); time.sleep(3)

    def _start_dashboard_server(self):
        """Avvia uvicorn in un thread daemon — stesso processo del bot."""
        import threading, uvicorn
        port = int(os.environ.get("PORT", settings.DASHBOARD_PORT))
        config = uvicorn.Config(
            "trading_bot.dashboard.server:app",
            host="0.0.0.0", port=port,
            log_level="warning", access_log=False,
        )
        server = uvicorn.Server(config)
        thread = threading.Thread(target=server.run, daemon=True, name="dashboard")
        thread.start()
        logger.info(f"[DASHBOARD] Avviato su :{port} (stesso processo)")

    def _check_regime(self):
        try:
            r = self._regime.evaluate(self)
            if r.get("switched"): logger.warning(f"[REGIME] ⚡ {r['current_label']}")
        except Exception as e: logger.error(f"[REGIME] {e}")

    def _check_regime_urgent(self):
        try:
            sig = self._regime._collect_signals(self)
            if sig["drawdown_pct"] > 15 or sig["daily_pnl"] < -10:
                if self._regime.current_regime != "safe":
                    self._regime._last_switch_ts = 0
                    self._regime.evaluate(self)
                    logger.warning("[REGIME] ⚠️ URGENT SAFE")
        except: pass

    def _scan_swing(self):
        strats = [s for s in self.strategies if s.NAME in ("RSI_MACD","BOLLINGER")]
        if not strats: return
        for mkt, syms in self._market_symbol_pairs():
            for sym in syms:
                try:
                    df = ohlcv_to_df(self.exchange.fetch_ohlcv(sym, settings.TF_SWING, 300, mkt))
                    for st in strats:
                        sig = st.analyze(df, sym, mkt)
                        if sig: self._process_signal(sig)
                except Exception as e: logger.error(f"[swing] {sym}: {e}")

    def _scan_breakout(self):
        st = next((s for s in self.strategies if s.NAME=="BREAKOUT"), None)
        if not st: return
        for mkt, syms in self._market_symbol_pairs():
            for sym in syms:
                try:
                    df = ohlcv_to_df(self.exchange.fetch_ohlcv(sym, settings.TF_BREAKOUT, 150, mkt))
                    sig = st.analyze(df, sym, mkt)
                    if sig: self._process_signal(sig)
                except Exception as e: logger.error(f"[breakout] {sym}: {e}")

    def _scan_scalping(self):
        st = next((s for s in self.strategies if s.NAME=="SCALPING"), None)
        if not st: return
        for mkt, syms in self._market_symbol_pairs():
            for sym in syms:
                if sym.split(":")[0] not in settings.SCALPING_SYMBOLS: continue
                try:
                    df = ohlcv_to_df(self.exchange.fetch_ohlcv(sym, settings.TF_SCALP, 100, mkt))
                    sig = st.analyze(df, sym, mkt)
                    if sig: self._process_signal(sig)
                except Exception as e: logger.error(f"[scalp] {sym}: {e}")

    def _scan_emerging(self):
        coins = self._emerging.scan()
        if not coins: return
        erm = float(_cfg_float("EMERGING_RISK_MULT",1.5)); eds = float(_cfg_float("EMERGING_DIRECT_SCORE",55))
        emc = float(_cfg_float("EMERGING_MOMENTUM_CHG",8)); ems = float(_cfg_float("EMERGING_MAX_SPREAD",0.8))
        swst = [s for s in self.strategies if s.NAME in ("RSI_MACD","BOLLINGER")]
        for c in coins:
            sym = f"{c['symbol']}/USDT"; sc = c.get("score",0); chg = c.get("price_change_24h",0); srg = c.get("volume_surge",1)
            try:
                # FIX BUG 1: Verifica che il simbolo esista su Bitget PRIMA di tutto
                if not self.exchange.is_valid_symbol(sym, "spot"):
                    logger.debug(f"[EMERGING SKIP] {sym} non esiste su Bitget spot")
                    continue
                sp = self._check_spread(sym, "spot")
                if sp > ems: continue
                df = ohlcv_to_df(self.exchange.fetch_ohlcv(sym, settings.TF_SWING, 100, "spot"))
                if len(df) < 20: continue
                cl = float(df["close"].iloc[-1]); at = float(df["close"].diff().abs().rolling(14).mean().iloc[-1] or cl*0.02)
                if chg >= emc:
                    slp = 0.05 if chg > 15 else 0.04; sld = cl*slp+sp/100*cl
                    self._process_signal(Signal(strategy="EMERGING_MOMENTUM",symbol=sym,market="spot",side="buy",confidence=min(50+chg*0.8+srg*2,92),entry=cl,stop_loss=round(cl-sld,6),take_profit=round(cl+sld*settings.TAKE_PROFIT_RATIO,6),atr=at,timeframe=settings.TF_SWING,notes=f"MOM chg={chg:+.1f}%"), risk_multiplier=erm)
                elif sc >= eds:
                    sld = at*2+sp/100*cl
                    self._process_signal(Signal(strategy="EMERGING_DIRECT",symbol=sym,market="spot",side="buy",confidence=min(sc,92),entry=cl,stop_loss=round(cl-sld,6),take_profit=round(cl+sld*settings.TAKE_PROFIT_RATIO,6),atr=at,timeframe=settings.TF_SWING,notes=f"DIR sc={sc:.0f}"), risk_multiplier=erm)
                else:
                    for st in swst:
                        sig = st.analyze(df, sym, "spot")
                        if sig and sig.side == "buy":
                            sig.confidence = min(100, sig.confidence*1.2); sig.notes += f" em={sc:.0f}"
                            self._process_signal(sig, risk_multiplier=erm*0.8)
            except Exception as e: logger.debug(f"[em] {sym}: {e}")

    def _check_spread(self, sym, mkt):
        try:
            ob = self.exchange.fetch_order_book(sym, 5, mkt)
            if ob.get("bids") and ob.get("asks"):
                b,a = float(ob["bids"][0][0]), float(ob["asks"][0][0]); m=(b+a)/2
                return (a-b)/m*100 if m>0 else 0
        except: pass
        return 0.0

    def _track_signal(self, signal, executed=False, skip_reason=""):
        """Traccia TUTTI i segnali generati — eseguiti e skippati — per la dashboard."""
        self._recent_signals.append({
            "ts": datetime.now(timezone.utc).isoformat(),
            "symbol": signal.symbol, "market": signal.market,
            "side": signal.side, "strategy": signal.strategy,
            "confidence": round(signal.confidence, 1),
            "entry": signal.entry, "stop_loss": signal.stop_loss,
            "take_profit": signal.take_profit, "executed": executed,
            "skip_reason": skip_reason,
        })
        self._recent_signals = self._recent_signals[-50:]

    def _process_signal(self, signal, risk_multiplier=1.0):
        if signal.market == "spot" and signal.side == "sell": return
        ok, reason = self.risk.can_trade_symbol(signal.symbol, signal.market)
        if not ok:
            self._track_signal(signal, False, reason)
            return
        if not self.risk.reserve_symbol(signal.symbol):
            self._track_signal(signal, False, "pending")
            return
        try: self._execute_signal(signal, risk_multiplier)
        except Exception as e: logger.error(f"[SIG] {signal.symbol}: {e}")
        finally: self.risk.release_symbol(signal.symbol)

    def _execute_signal(self, signal, risk_multiplier=1.0):
        try:
            ok_s, reason_s = (self._sentiment.should_trade_long if signal.side=="buy" else self._sentiment.should_trade_short)(signal.symbol)
            if not ok_s:
                self._track_signal(signal, False, f"sentiment: {reason_s[:30]}")
                return
            signal.confidence = min(100, signal.confidence * self._sentiment.confidence_modifier(signal.side))
        except: pass
        if signal.confidence < settings.MIN_CONFIDENCE:
            self._track_signal(signal, False, f"conf {signal.confidence:.0f}%<{settings.MIN_CONFIDENCE:.0f}%")
            return
        bal = self.exchange.get_usdt_balance(signal.market)
        if bal < 10:
            self._track_signal(signal, False, f"bal {bal:.0f}<10")
            return
        size = self.risk.position_size(balance=bal, entry=signal.entry, stop_loss=signal.stop_loss, atr=signal.atr, market=signal.market, risk_multiplier=risk_multiplier, symbol=signal.symbol)
        if size <= 0:
            self._track_signal(signal, False, "size=0")
            return
        if size < self.exchange.get_min_order_size(signal.symbol, signal.market):
            self._track_signal(signal, False, "min_size")
            return
        if size * signal.entry < self.exchange.get_min_notional(signal.symbol, signal.market):
            self._track_signal(signal, False, "min_notional")
            return
        logger.info(f"SEGNALE {signal.strategy} | {signal.symbol} {signal.market} {signal.side.upper()} conf={signal.confidence:.0f}%")
        params = {"reduceOnly": False, "marginMode": settings.MARGIN_MODE} if signal.market == "futures" else {}
        order = self.exchange.create_market_order(symbol=signal.symbol, side=signal.side, amount=size, market=signal.market, params=params)
        if not order:
            self._track_signal(signal, False, "order_failed")
            return
        oid = order.get("id", f"unk_{int(time.time())}")
        self._track_signal(signal, True)
        td = {"order_id":oid,"side":signal.side,"entry":signal.entry,"size":size,"stop_loss":signal.stop_loss,"take_profit":signal.take_profit,"strategy":signal.strategy,"confidence":signal.confidence,"atr":signal.atr}
        self.risk.register_open(signal.symbol, td, signal.market)
        self.db.save_trade_open(order_id=oid,symbol=signal.symbol,market=signal.market,strategy=signal.strategy,side=signal.side,entry=signal.entry,size=size,stop_loss=signal.stop_loss,take_profit=signal.take_profit,confidence=signal.confidence,atr=signal.atr,notes=signal.notes,timeframe=signal.timeframe,leverage=settings.DEFAULT_LEVERAGE if signal.market=="futures" else 1)
        self.notifier.trade_opened(symbol=signal.symbol,side=signal.side,size=size,entry=signal.entry,stop_loss=signal.stop_loss,take_profit=signal.take_profit,market=signal.market,strategy=signal.strategy,confidence=signal.confidence)

    def _monitor_positions(self):
        for t in self.risk.all_open_trades():
            sym, mkt = t["symbol"], t["market"]
            try:
                cp = float(self.exchange.fetch_ticker(sym, mkt)["last"])
                ns = self.risk.trailing_stop(t, cp)
                if ns != t.get("stop_loss"): self.risk.update_trade_sl(sym, mkt, ns); t["stop_loss"] = ns
                atr, entry = t.get("atr",0) or 0, t.get("entry", cp)
                if atr > 0:
                    if t["side"]=="buy" and cp >= entry+atr:
                        be = entry+atr*0.1
                        if t["stop_loss"] < be: self.risk.update_trade_sl(sym, mkt, be); t["stop_loss"] = be
                    elif t["side"]=="sell" and cp <= entry-atr:
                        be = entry-atr*0.1
                        if t["stop_loss"] > be: self.risk.update_trade_sl(sym, mkt, be); t["stop_loss"] = be
                ok, reason = self.risk.should_close(t, cp)
                if ok: self._close_position(sym, mkt, t, cp, reason)
            except Exception as e: logger.error(f"[mon] {sym}: {e}")

    def _close_position(self, sym, mkt, trade, exit_p, reason):
        cs = "sell" if trade["side"]=="buy" else "buy"
        order = self.exchange.create_market_order(symbol=sym, side=cs, amount=trade["size"], market=mkt, params={"reduceOnly":True} if mkt=="futures" else {})
        if not order and settings.IS_LIVE: return
        entry = trade["entry"]; mult = settings.DEFAULT_LEVERAGE if mkt=="futures" else 1
        pnl_pct = ((exit_p-entry)/entry*100*mult) * (1 if trade["side"]=="buy" else -1)
        pnl_usdt = trade["size"]*entry*(pnl_pct/100)
        self.risk.register_close(sym, pnl_pct, mkt, reason=reason)
        self.db.save_trade_close(order_id=trade["order_id"], exit_price=exit_p, pnl_pct=pnl_pct, pnl_usdt=pnl_usdt, reason=reason)
        self.notifier.trade_closed(symbol=sym, side=trade["side"], entry=entry, exit_price=exit_p, pnl_pct=pnl_pct, pnl_usdt=pnl_usdt, reason=reason, market=mkt)
        logger.info(f"{'✅' if pnl_pct>0 else '❌'} CHIUSO {sym} {mkt} | {reason} | {pnl_pct:+.2f}%")
        self._check_regime_urgent()
        # Auto-trasferisci USDT liberi spot → futures dopo chiusura spot
        if mkt == "spot":
            try:
                self.exchange.auto_rebalance(keep_spot_usdt=5.0)
            except Exception:
                pass

    def _sync_balance(self):
        try:
            s = self.exchange.get_usdt_balance("spot") if "spot" in settings.MARKET_TYPES else 0
            f = self.exchange.get_usdt_balance("futures") if "futures" in settings.MARKET_TYPES else 0
            self.risk.session_start_balance = s+f; self.risk.peak_balance = s+f
        except Exception as e: logger.warning(f"balance: {e}")

    def _auto_rebalance(self):
        """
        Ogni 10 min: trasferisce USDT liberi spot → futures.
        Mantiene 5 USDT sullo spot per fee e piccoli trade.
        Anche dopo ogni chiusura spot, trasferisce i proventi.
        """
        try:
            if "futures" not in settings.MARKET_TYPES:
                return
            result = self.exchange.auto_rebalance(keep_spot_usdt=5.0)
            if result.get("transferred", 0) > 0:
                # Aggiorna il peak balance
                total = result.get("spot_after", 0) + result.get("futures_after", 0)
                if total > self.risk.peak_balance:
                    self.risk.peak_balance = total
        except Exception as e:
            logger.debug(f"[REBALANCE] {e}")

    def _health_check(self):
        try:
            s = self.exchange.get_usdt_balance("spot") if "spot" in settings.MARKET_TYPES else 0
            f = self.exchange.get_usdt_balance("futures") if "futures" in settings.MARKET_TYPES else 0
            if "futures" in settings.MARKET_TYPES: self.exchange._setup_leverage()
            st = self.risk.stats(); total = s+f
            if total > self.risk.peak_balance: self.risk.peak_balance = total
            logger.info(f"[HEALTH] S={s:.0f} F={f:.0f} PnL={st.get('daily_pnl',0):+.1f}% Regime={self._regime.current_regime}")
            ok, reason = self.risk.can_trade()
            if not ok: self.notifier.circuit_breaker(reason)
        except Exception as e: logger.error(f"health: {e}")

    def _daily_report(self):
        try:
            st = self.risk.stats()
            s = self.exchange.get_usdt_balance("spot") if "spot" in settings.MARKET_TYPES else 0
            f = self.exchange.get_usdt_balance("futures") if "futures" in settings.MARKET_TYPES else 0
            self.notifier.daily_report(st, s, f)
        except Exception as e: logger.error(f"report: {e}")

    def _market_symbol_pairs(self):
        p = []
        if "spot" in settings.MARKET_TYPES: p.append(("spot", settings.SPOT_SYMBOLS))
        if "futures" in settings.MARKET_TYPES: p.append(("futures", settings.FUTURES_SYMBOLS))
        return p

if __name__ == "__main__":
    TradingBot().start()
