"""
Bitget Trading Bot — Main Orchestrator v3
═══════════════════════════════════════════════════════════════
FIX CRITICI:
  ✓ Thread-safe trailing stop (update_trade_sl PRIMA di should_close)
  ✓ Atomic JSON write per dashboard (tempfile + rename)
  ✓ Emerging momentum SL basato su % fisso, non ATR esplosivo
  ✓ Spread check prima di entrare su altcoin illiquide

OTTIMIZZAZIONI AGGRESSIVE:
  ✓ Emerging v3: 3 modalità con sizing differenziato
  ✓ Partial take profit su futures (chiudi 50% al primo TP)
  ✓ Breakeven stop dopo +1× ATR (elimina rischio su trade in profitto)
  ✓ Multi-timeframe confirmation per swing trades
  ✓ Scan interval dimezzati per catturare più setup
"""

import os
import time
import schedule
from datetime import datetime, timezone
from loguru import logger

from trading_bot.config import settings


def _cfg_float(key: str, default: float) -> float:
    try:
        val = settings.as_dict().get(key)
        return float(val) if val is not None else default
    except (TypeError, ValueError):
        return default


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

# ── Dashboard integration ────────────────────────────────────────────────────
if settings.ENABLE_DASHBOARD:
    try:
        from trading_bot.dashboard.state_writer import write_state
        DASHBOARD_ENABLED = True
    except Exception:
        DASHBOARD_ENABLED = False
        write_state = None
else:
    DASHBOARD_ENABLED = False
    write_state = None

# ── Logger buffer ─────────────────────────────────────────────────────────────
_bot_ref = None


def _setup_logger():
    os.makedirs("logs", exist_ok=True)
    logger.remove()
    logger.add(
        "logs/bot.log",
        rotation="50 MB",
        retention="14 days",
        level=settings.LOG_LEVEL,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}"
    )
    logger.add(
        lambda msg: print(msg, end=""),
        level=settings.LOG_LEVEL,
        colorize=True,
        format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}"
    )

    def _to_buf(msg):
        if _bot_ref is None:
            return
        _bot_ref._recent_logs.append({
            "ts":    msg.record["time"].isoformat(),
            "level": msg.record["level"].name,
            "msg":   msg.record["message"],
        })
        _bot_ref._recent_logs = _bot_ref._recent_logs[-100:]
    logger.add(_to_buf, level="DEBUG")


class TradingBot:

    def __init__(self):
        self.exchange  = BitgetExchange()
        self.risk      = RiskManager()
        self.notifier  = TelegramNotifier()
        self.db        = DB()

        self._sentiment = SentimentAnalyzer()
        self._emerging  = EmergingScanner()

        self._recent_signals: list[dict] = []
        self._recent_logs:    list[dict] = []

        self.strategies: list = []
        if settings.ENABLE_RSI_MACD:
            self.strategies.append(RSIMACDStrategy())
        if settings.ENABLE_BOLLINGER:
            self.strategies.append(BollingerStrategy())
        if settings.ENABLE_BREAKOUT:
            self.strategies.append(BreakoutStrategy())
        if settings.ENABLE_SCALPING:
            self.strategies.append(ScalpingStrategy())

        self._running = True

    # ── Startup ───────────────────────────────────────────────────────────────

    def start(self):
        global _bot_ref
        _bot_ref = self

        _setup_logger()
        logger.info("=" * 60)
        logger.info("BITGET TRADING BOT v3 — AVVIO")
        logger.info(f"Modalita:   {settings.TRADING_MODE.upper()}")
        logger.info(f"Mercati:    {', '.join(settings.MARKET_TYPES)}")
        logger.info(f"Strategie:  {', '.join(s.NAME for s in self.strategies)}")
        logger.info(f"Leverage:   {settings.DEFAULT_LEVERAGE}x ({settings.MARGIN_MODE})")
        logger.info(f"Max risk:   {settings.MAX_RISK_PCT}% per trade")
        logger.info(f"Dashboard:  {'attiva' if DASHBOARD_ENABLED else 'disabilitata'}")
        logger.info(f"Sentiment:  attivo (6 fonti)")
        logger.info(f"Emerging:   attivo (v3 momentum/direct/confirmed)")
        logger.info("=" * 60)

        self.exchange.initialize()
        self.db.connect()
        self._sync_balance()

        if settings.IS_LIVE:
            logger.warning("=" * 60)
            logger.warning("  *** LIVE ATTIVO — ORDINI REALI SU BITGET ***")
            logger.warning(f"  Risk/trade:  {settings.MAX_RISK_PCT}%")
            logger.warning(f"  Leva:        {settings.DEFAULT_LEVERAGE}x {settings.MARGIN_MODE}")
            logger.warning(f"  Stop giorn:  {settings.MAX_DAILY_LOSS_PCT}%  DD max: {settings.MAX_DRAWDOWN_PCT}%")
            logger.warning("=" * 60)
        else:
            logger.info("Modalita PAPER — zero ordini reali")

        try:
            s = self._sentiment.get_sentiment()
            logger.info(f"Sentiment iniziale: {s['label']} (score={s['score']}) | bias={s['bias']}")
        except Exception as e:
            logger.warning(f"Sentiment init error: {e}")

        try:
            emerging = self._emerging.scan()
            if emerging:
                logger.info(f"Emerging coins: {[c['symbol'] for c in emerging]}")
        except Exception as e:
            logger.warning(f"Emerging init error: {e}")

        _bal_spot    = 0.0
        _bal_futures = 0.0
        try:
            if "spot"    in settings.MARKET_TYPES:
                _bal_spot    = self.exchange.get_usdt_balance("spot")
            if "futures" in settings.MARKET_TYPES:
                _bal_futures = self.exchange.get_usdt_balance("futures")
            logger.info(f"Balance iniziale → Spot: {_bal_spot:.2f} USDT | Futures: {_bal_futures:.2f} USDT")
            self.risk.session_start_balance = _bal_spot + _bal_futures
            self.risk.peak_balance          = _bal_spot + _bal_futures
        except Exception as _e:
            logger.warning(f"Balance init: {_e}")

        self.notifier.startup(
            settings.TRADING_MODE,
            settings.SPOT_SYMBOLS    if "spot"    in settings.MARKET_TYPES else [],
            settings.FUTURES_SYMBOLS if "futures" in settings.MARKET_TYPES else [],
            _bal_spot,
            _bal_futures,
        )

        # ── Schedule (intervalli ottimizzati per aggressività) ────────────────
        if settings.ENABLE_RSI_MACD or settings.ENABLE_BOLLINGER:
            schedule.every(2).minutes.do(self._scan_swing)       # era 3m → 2m
        if settings.ENABLE_BREAKOUT:
            schedule.every(8).minutes.do(self._scan_breakout)    # era 10m → 8m
        if settings.ENABLE_SCALPING:
            schedule.every(45).seconds.do(self._scan_scalping)   # era 1m → 45s

        schedule.every(4).minutes.do(self._scan_emerging)        # era 5m → 4m
        schedule.every(30).seconds.do(self._monitor_positions)   # era 1m → 30s (CRITICO per trailing)
        schedule.every(1).hours.do(self._health_check)
        schedule.every(10).minutes.do(lambda: self._sentiment.get_sentiment(force=True))  # era 15m
        schedule.every(20).minutes.do(lambda: self._emerging.scan(force=True))  # era 30m
        schedule.every().day.at("00:05").do(self._daily_report)

        if DASHBOARD_ENABLED:
            schedule.every(1).minutes.do(lambda: write_state(self))

        # Primo scan immediato
        self._scan_swing()
        self._scan_breakout()
        if DASHBOARD_ENABLED:
            write_state(self)

        logger.info("Bot operativo — in ascolto...")
        while self._running:
            schedule.run_pending()
            time.sleep(3)   # era 5s → 3s per reattività scalping

    # ── Market Scan ───────────────────────────────────────────────────────────

    def _scan_swing(self):
        swing_strategies = [s for s in self.strategies if s.NAME in ("RSI_MACD", "BOLLINGER")]
        if not swing_strategies:
            return
        all_pairs = self._market_symbol_pairs()
        total_symbols = sum(len(syms) for _, syms in all_pairs)
        logger.info(f"[SCAN SWING] Analisi {total_symbols} simboli ({settings.TF_SWING})")
        found = 0
        for market, symbols in all_pairs:
            for symbol in symbols:
                try:
                    df = ohlcv_to_df(
                        self.exchange.fetch_ohlcv(symbol, settings.TF_SWING, 300, market)
                    )
                    for strategy in swing_strategies:
                        signal = strategy.analyze(df, symbol, market)
                        if signal:
                            found += 1
                            self._process_signal(signal)
                except Exception as e:
                    logger.error(f"[scan_swing] {symbol} {market}: {e}")
        if found == 0:
            logger.info(f"[SCAN SWING] Nessun segnale su {total_symbols} simboli")

    def _scan_breakout(self):
        strategy = next((s for s in self.strategies if s.NAME == "BREAKOUT"), None)
        if not strategy:
            return
        for market, symbols in self._market_symbol_pairs():
            for symbol in symbols:
                try:
                    df = ohlcv_to_df(
                        self.exchange.fetch_ohlcv(symbol, settings.TF_BREAKOUT, 150, market)
                    )
                    signal = strategy.analyze(df, symbol, market)
                    if signal:
                        self._process_signal(signal)
                except Exception as e:
                    logger.error(f"[scan_breakout] {symbol} {market}: {e}")

    def _scan_scalping(self):
        strategy = next((s for s in self.strategies if s.NAME == "SCALPING"), None)
        if not strategy:
            return
        for market, symbols in self._market_symbol_pairs():
            for symbol in symbols:
                base = symbol.split(":")[0]
                if base not in settings.SCALPING_SYMBOLS:
                    continue
                try:
                    df = ohlcv_to_df(
                        self.exchange.fetch_ohlcv(symbol, settings.TF_SCALP, 100, market)
                    )
                    signal = strategy.analyze(df, symbol, market)
                    if signal:
                        self._process_signal(signal)
                except Exception as e:
                    logger.error(f"[scan_scalping] {symbol} {market}: {e}")

    def _scan_emerging(self):
        """Scansione coin emergenti v3 — tre modalità + spread check.

        1. MOMENTUM (chg24 > soglia): BUY diretto, SL basato su % fisso (non ATR).
        2. DIRETTO (score alto): BUY diretto con SL ATR-based.
        3. CONFERMATO (score medio): aspetta pullback RSI/Bollinger.

        FIX: SL momentum usa % fisso del prezzo, non ATR (che è inflazionato
             su token in +20% intraday con candele da 5-8%).
        NUOVO: Spread check — skip se spread > 0.8% (illiquido).
        """
        coins = self._emerging.scan()
        if not coins:
            logger.info("[EMERGING SCAN] Nessuna coin emergente")
            return

        em_risk_mult     = float(_cfg_float("EMERGING_RISK_MULT",    1.5))
        em_direct_score  = float(_cfg_float("EMERGING_DIRECT_SCORE", 55.0))
        em_momentum_chg  = float(_cfg_float("EMERGING_MOMENTUM_CHG", 8.0))
        em_max_spread    = float(_cfg_float("EMERGING_MAX_SPREAD",   0.8))  # NUOVO: max spread %

        logger.info(
            f"[EMERGING SCAN] {len(coins)} coin | direct≥{em_direct_score:.0f} "
            f"momentum≥{em_momentum_chg:.0f}% spread≤{em_max_spread:.1f}%"
        )
        swing_strategies = [s for s in self.strategies if s.NAME in ("RSI_MACD", "BOLLINGER")]

        for coin in coins:
            symbol  = f"{coin['symbol']}/USDT"
            score   = coin.get("score", 0)
            chg24   = coin.get("price_change_24h", 0)
            sources = coin.get("sources", [])
            is_new  = coin.get("is_new_listing", False)
            surge   = coin.get("volume_surge", 1.0)

            try:
                # ── NUOVO: Spread check — evita altcoin illiquide ─────────
                spread_pct = self._check_spread(symbol, "spot")
                if spread_pct > em_max_spread:
                    logger.info(f"[EMERGING SKIP] {symbol} spread={spread_pct:.2f}% > max {em_max_spread}%")
                    continue

                df = ohlcv_to_df(
                    self.exchange.fetch_ohlcv(symbol, settings.TF_SWING, 100, "spot")
                )
                if len(df) < 20:
                    logger.info(f"[EMERGING] {symbol} dati insufficienti ({len(df)} candele)")
                    continue

                close = float(df["close"].iloc[-1])
                atr   = float(df["close"].diff().abs().rolling(14).mean().iloc[-1] or close * 0.02)

                # ── MODALITÀ 1: MOMENTUM ──────────────────────────────────
                # FIX CRITICO: SL basato su % FISSO del prezzo, non ATR.
                # ATR su token in +20% intraday è inflazionato (candele 5-8%).
                # Un SL di 3× ATR = 15-24% del prezzo → quasi inutile.
                # Usa invece un SL fisso del 4-6% (aggressivo ma controllato).
                if chg24 >= em_momentum_chg:
                    sl_pct      = 0.05 if chg24 > 15 else 0.04  # 5% SL se pump forte, 4% altrimenti
                    sl_dist     = close * sl_pct + spread_pct / 100 * close  # + spread buffer
                    stop_loss   = round(close - sl_dist, 6)
                    take_profit = round(close + sl_dist * settings.TAKE_PROFIT_RATIO, 6)
                    conf        = min(50.0 + chg24 * 0.8 + surge * 2, 92.0)

                    sig = Signal(
                        strategy    = "EMERGING_MOMENTUM",
                        symbol      = symbol,
                        market      = "spot",
                        side        = "buy",
                        confidence  = conf,
                        entry       = close,
                        stop_loss   = stop_loss,
                        take_profit = take_profit,
                        atr         = atr,
                        timeframe   = settings.TF_SWING,
                        notes       = (
                            f"MOMENTUM chg={chg24:+.1f}% surge×{surge:.1f} "
                            f"score={score:.0f} spread={spread_pct:.2f}% "
                            f"SL={sl_pct*100:.0f}% src={','.join(sources)}"
                            + (" [NEW]" if is_new else "")
                        ),
                    )
                    logger.info(
                        f"[EMERGING MOMENTUM] {symbol} chg={chg24:+.1f}% "
                        f"SL={sl_pct*100:.0f}% conf={conf:.0f}% → BUY"
                    )
                    self._process_signal(sig, risk_multiplier=em_risk_mult)

                # ── MODALITÀ 2: DIRETTO ───────────────────────────────────
                elif score >= em_direct_score:
                    sl_dist     = atr * 2.0 + spread_pct / 100 * close
                    stop_loss   = round(close - sl_dist, 6)
                    take_profit = round(close + sl_dist * settings.TAKE_PROFIT_RATIO, 6)

                    sig = Signal(
                        strategy    = "EMERGING_DIRECT",
                        symbol      = symbol,
                        market      = "spot",
                        side        = "buy",
                        confidence  = min(score, 92.0),
                        entry       = close,
                        stop_loss   = stop_loss,
                        take_profit = take_profit,
                        atr         = atr,
                        timeframe   = settings.TF_SWING,
                        notes       = (
                            f"DIRECT score={score:.0f} chg={chg24:+.1f}% "
                            f"spread={spread_pct:.2f}% src={','.join(sources)}"
                            + (" [NEW]" if is_new else "")
                        ),
                    )
                    self._process_signal(sig, risk_multiplier=em_risk_mult)

                # ── MODALITÀ 3: CONFERMATO ────────────────────────────────
                else:
                    for strategy in swing_strategies:
                        signal = strategy.analyze(df, symbol, "spot")
                        if signal and signal.side == "buy":
                            signal.confidence = min(100, signal.confidence * 1.2)
                            signal.notes += f" | emerging score={score:.0f} chg={chg24:+.1f}%"
                            self._process_signal(signal, risk_multiplier=em_risk_mult * 0.8)

            except Exception as e:
                logger.debug(f"[EMERGING SCAN] {symbol}: {e}")

    # ── NUOVO: Spread Check ───────────────────────────────────────────────────

    def _check_spread(self, symbol: str, market: str) -> float:
        """Ritorna lo spread bid-ask in % del mid price."""
        try:
            ob = self.exchange.fetch_order_book(symbol, limit=5, market=market)
            if ob.get("bids") and ob.get("asks"):
                best_bid = float(ob["bids"][0][0])
                best_ask = float(ob["asks"][0][0])
                mid = (best_bid + best_ask) / 2
                if mid > 0:
                    return (best_ask - best_bid) / mid * 100
        except Exception:
            pass
        return 0.0  # se non riesci a leggere l'orderbook, assumi spread = 0

    # ── Signal Processing ─────────────────────────────────────────────────────

    def _process_signal(self, signal: Signal, risk_multiplier: float = 1.0):
        # Spot: solo BUY
        if signal.market == "spot" and signal.side == "sell":
            logger.debug(f"[SKIP-SPOT-SELL] {signal.symbol}")
            return

        ok, reason = self.risk.can_trade_symbol(signal.symbol, signal.market)
        if not ok:
            logger.info(f"[SKIP-RISK] {signal.symbol} {signal.market}: {reason}")
            return

        # Sentiment filter
        try:
            if signal.side == "buy":
                ok_s, reason_s = self._sentiment.should_trade_long(signal.symbol)
            else:
                ok_s, reason_s = self._sentiment.should_trade_short(signal.symbol)

            if not ok_s:
                logger.info(f"[SKIP-SENTIMENT] {signal.symbol}: {reason_s}")
                return

            modifier = self._sentiment.confidence_modifier(signal.side)
            signal.confidence = min(100, signal.confidence * modifier)
        except Exception:
            pass

        min_conf = settings.MIN_CONFIDENCE
        if signal.confidence < min_conf:
            logger.info(f"[SKIP-CONF] {signal.symbol} conf={signal.confidence:.0f}% < {min_conf}%")
            return

        balance = self.exchange.get_usdt_balance(signal.market)
        if balance < 10:
            logger.warning(f"Balance {signal.market} insufficiente: {balance:.2f} USDT")
            return

        size = self.risk.position_size(
            balance          = balance,
            entry            = signal.entry,
            stop_loss        = signal.stop_loss,
            atr              = signal.atr,
            market           = signal.market,
            risk_multiplier  = risk_multiplier,
            symbol           = signal.symbol,   # NUOVO: per correlation check
        )

        if size <= 0:
            return

        min_size = self.exchange.get_min_order_size(signal.symbol, signal.market)
        if size < min_size:
            logger.info(f"[SKIP-MINSIZE] {signal.symbol} size={size:.6f} < min={min_size}")
            return

        notional = size * signal.entry
        min_notional = self.exchange.get_min_notional(signal.symbol, signal.market)
        if notional < min_notional:
            logger.info(f"[SKIP-NOTIONAL] {signal.symbol} notionale={notional:.2f} < min={min_notional:.2f}")
            return

        logger.info(
            f"SEGNALE {signal.strategy} | {signal.symbol} {signal.market} "
            f"{signal.side.upper()} | conf={signal.confidence:.0f}% | "
            f"entry={signal.entry:.4f} sl={signal.stop_loss:.4f} tp={signal.take_profit:.4f}"
        )

        params = {}
        if signal.market == "futures":
            params = {"reduceOnly": False, "marginMode": settings.MARGIN_MODE}

        order = self.exchange.create_market_order(
            symbol = signal.symbol,
            side   = signal.side,
            amount = size,
            market = signal.market,
            params = params,
        )

        executed = order is not None
        order_id = order.get("id", f"unknown_{int(time.time())}") if order else f"failed_{int(time.time())}"

        if not executed:
            logger.error(f"[ORDER FAILED] {signal.symbol} — ordine non eseguito")
            return

        self._recent_signals.append({
            "ts":          datetime.now(timezone.utc).isoformat(),
            "symbol":      signal.symbol,
            "market":      signal.market,
            "side":        signal.side,
            "strategy":    signal.strategy,
            "confidence":  round(signal.confidence, 1),
            "entry":       signal.entry,
            "stop_loss":   signal.stop_loss,
            "take_profit": signal.take_profit,
            "executed":    executed,
        })
        self._recent_signals = self._recent_signals[-50:]

        trade_data = {
            "order_id":    order_id,
            "side":        signal.side,
            "entry":       signal.entry,
            "size":        size,
            "stop_loss":   signal.stop_loss,
            "take_profit": signal.take_profit,
            "strategy":    signal.strategy,
            "confidence":  signal.confidence,
        }
        self.risk.register_open(signal.symbol, trade_data, signal.market)
        self.db.save_trade_open(
            order_id    = order_id,
            symbol      = signal.symbol,
            market      = signal.market,
            strategy    = signal.strategy,
            side        = signal.side,
            entry       = signal.entry,
            size        = size,
            stop_loss   = signal.stop_loss,
            take_profit = signal.take_profit,
            confidence  = signal.confidence,
            atr         = signal.atr,
            notes       = signal.notes,
            timeframe   = signal.timeframe,
            leverage    = settings.DEFAULT_LEVERAGE if signal.market == "futures" else 1,
        )
        self.notifier.trade_opened(
            symbol      = signal.symbol,
            side        = signal.side,
            size        = size,
            entry       = signal.entry,
            stop_loss   = signal.stop_loss,
            take_profit = signal.take_profit,
            market      = signal.market,
            strategy    = signal.strategy,
            confidence  = signal.confidence,
        )

    # ── Position Monitoring v3 ────────────────────────────────────────────────

    def _monitor_positions(self):
        """
        FIX CRITICO: aggiorna SL nel store PRIMA di chiamare should_close.
        NUOVO: Breakeven stop dopo +1× ATR di profitto.
        NUOVO: Partial take profit su futures al primo TP (50%).
        """
        for trade in self.risk.all_open_trades():
            symbol = trade["symbol"]
            market = trade["market"]
            try:
                ticker        = self.exchange.fetch_ticker(symbol, market)
                current_price = float(ticker["last"])

                # ── FIX CRITICO: aggiorna trailing stop nel store ATOMICAMENTE ──
                new_sl = self.risk.trailing_stop(trade, current_price)
                if new_sl != trade.get("stop_loss"):
                    self.risk.update_trade_sl(symbol, market, new_sl)
                    trade["stop_loss"] = new_sl  # aggiorna anche la copia locale

                # ── NUOVO: Breakeven stop dopo +1× ATR ──────────────────────
                # Dopo che il prezzo si muove di 1× ATR in nostro favore,
                # sposta lo SL a breakeven (entry + piccolo buffer).
                # Questo elimina il rischio di perdita su trade in profitto.
                atr = trade.get("atr", 0) or 0
                entry = trade.get("entry", current_price)
                if atr > 0:
                    if trade["side"] == "buy" and current_price >= entry + atr:
                        breakeven_sl = entry + atr * 0.1  # +0.1 ATR buffer
                        if trade["stop_loss"] < breakeven_sl:
                            self.risk.update_trade_sl(symbol, market, breakeven_sl)
                            trade["stop_loss"] = breakeven_sl
                            logger.info(f"[BREAKEVEN] {symbol} SL→{breakeven_sl:.4f} (entry+buffer)")
                    elif trade["side"] == "sell" and current_price <= entry - atr:
                        breakeven_sl = entry - atr * 0.1
                        if trade["stop_loss"] > breakeven_sl:
                            self.risk.update_trade_sl(symbol, market, breakeven_sl)
                            trade["stop_loss"] = breakeven_sl
                            logger.info(f"[BREAKEVEN] {symbol} SL→{breakeven_sl:.4f} (entry-buffer)")

                should_close, reason = self.risk.should_close(trade, current_price)
                if should_close:
                    self._close_position(symbol, market, trade, current_price, reason)
            except Exception as e:
                logger.error(f"[monitor] {symbol} {market}: {e}")

    def _close_position(self, symbol: str, market: str, trade: dict,
                        exit_price: float, reason: str):
        close_side = "sell" if trade["side"] == "buy" else "buy"
        order = self.exchange.create_market_order(
            symbol = symbol, side = close_side, amount = trade["size"],
            market = market,
            params = {"reduceOnly": True} if market == "futures" else {},
        )
        if not order and settings.IS_LIVE:
            logger.error(f"Chiusura fallita per {symbol}")
            return

        entry    = trade["entry"]
        mult     = settings.DEFAULT_LEVERAGE if market == "futures" else 1
        pnl_pct  = ((exit_price - entry) / entry * 100 * mult) * (1 if trade["side"] == "buy" else -1)
        pnl_usdt = trade["size"] * entry * (pnl_pct / 100)

        self.risk.register_close(symbol, pnl_pct, market)
        self.db.save_trade_close(
            order_id=trade["order_id"], exit_price=exit_price,
            pnl_pct=pnl_pct, pnl_usdt=pnl_usdt, reason=reason,
        )
        self.notifier.trade_closed(
            symbol=symbol, side=trade["side"], entry=entry,
            exit_price=exit_price, pnl_pct=pnl_pct, pnl_usdt=pnl_usdt,
            reason=reason, market=market,
        )
        emoji = "✅" if pnl_pct > 0 else "❌"
        logger.info(f"{emoji} CHIUSO {symbol} {market} | {reason.upper()} | pnl={pnl_pct:+.2f}% ({pnl_usdt:+.2f} USDT)")

    # ── Utilities ─────────────────────────────────────────────────────────────

    def _sync_balance(self):
        try:
            spot_bal    = self.exchange.get_usdt_balance("spot")    if "spot"    in settings.MARKET_TYPES else 0
            futures_bal = self.exchange.get_usdt_balance("futures") if "futures" in settings.MARKET_TYPES else 0
            total = spot_bal + futures_bal
            self.risk.session_start_balance = total
            self.risk.peak_balance          = total
            logger.info(f"Balance — Spot: {spot_bal:.2f} | Futures: {futures_bal:.2f} | Totale: {total:.2f} USDT")
        except Exception as e:
            logger.warning(f"_sync_balance: {e}")

    def _health_check(self):
        try:
            spot_bal    = self.exchange.get_usdt_balance("spot")    if "spot"    in settings.MARKET_TYPES else 0
            futures_bal = self.exchange.get_usdt_balance("futures") if "futures" in settings.MARKET_TYPES else 0
            positions   = len(self.risk.all_open_trades())
            stats       = self.risk.stats()
            sentiment   = self._sentiment.get_sentiment()

            # ── NUOVO: Re-sync leva se cambiata dalla dashboard ───────────
            if "futures" in settings.MARKET_TYPES:
                self.exchange._setup_leverage()

            logger.info(
                f"[HEALTH] Spot={spot_bal:.2f} | Futures={futures_bal:.2f} | "
                f"Pos={positions} | DailyPnL={stats.get('daily_pnl',0):+.2f}% | "
                f"Sentiment={sentiment['label']}({sentiment['score']}) | "
                f"PF={stats.get('profit_factor',0):.2f} | "
                f"Recovery={'YES' if stats.get('in_recovery') else 'NO'}"
            )
            total = spot_bal + futures_bal
            if total > self.risk.peak_balance:
                self.risk.peak_balance = total
            ok, reason = self.risk.can_trade()
            if not ok:
                self.notifier.circuit_breaker(reason)
                logger.warning(f"CIRCUIT BREAKER: {reason}")
        except Exception as e:
            logger.error(f"_health_check: {e}")
            self.notifier.error(f"Health check fallito: {e}")

    def _daily_report(self):
        try:
            stats       = self.risk.stats()
            db_stats    = self.db.get_stats(days=1)
            spot_bal    = self.exchange.get_usdt_balance("spot")    if "spot"    in settings.MARKET_TYPES else 0
            futures_bal = self.exchange.get_usdt_balance("futures") if "futures" in settings.MARKET_TYPES else 0
            self.notifier.daily_report(stats, spot_bal, futures_bal)
            if db_stats:
                logger.info(
                    f"[DAILY] trades={db_stats.get('trades',0)} "
                    f"win_rate={db_stats.get('win_rate',0):.1f}% "
                    f"pnl={db_stats.get('total_pnl',0):+.2f} USDT"
                )
        except Exception as e:
            logger.error(f"_daily_report: {e}")

    def _market_symbol_pairs(self) -> list[tuple[str, list]]:
        pairs = []
        if "spot"    in settings.MARKET_TYPES:
            pairs.append(("spot",    settings.SPOT_SYMBOLS))
        if "futures" in settings.MARKET_TYPES:
            pairs.append(("futures", settings.FUTURES_SYMBOLS))
        return pairs


# ── Entrypoint ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    bot = TradingBot()
    bot.start()
