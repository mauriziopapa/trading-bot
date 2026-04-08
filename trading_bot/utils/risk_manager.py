"""
Risk Manager v8.0 SNIPER HARDENED
Production ready + trailing + session protection
"""

import time
import threading
from collections import deque
from loguru import logger
from trading_bot.config import settings


# ==========================================================
# SAFE FLOAT
# ==========================================================

def safe_float(x, default=0.0):
    try:
        return float(x) if x is not None else default
    except:
        return default


class RiskManager:

    def __init__(self):

        self._lock = threading.Lock()

        self.open_spot = {}
        self.open_futures = {}

        self._pending_symbols = set()

        self.session_start_balance = 0
        self.peak_balance = 0
        self._estimated_balance = 0

        self.wins = 0
        self.losses = 0

        self._recent_pnls = deque(maxlen=100)

        self.db = None

        self.MAX_POSITIONS = 2             # HARD LIMIT — absolute, no override
        self.global_stop = False
        self._global_stop_reason: str = ""
        self._global_stop_since: float = 0.0   # epoch seconds, set when global_stop triggers

        # Capital & exposure limits
        self.MAX_CAPITAL_USAGE = 0.6      # max 60% of capital in margin
        self.MAX_DAILY_LOSS_PCT = 3.0     # max 3% daily loss -> stop trading

        # Daily loss tracking
        self._daily_pnl = 0.0             # accumulated PnL % for the day
        self._daily_reset_date = None     # date of last reset


# ==========================================================
# BALANCE
# ==========================================================

    @property
    def estimated_balance(self):

        if self._estimated_balance > 0:
            return self._estimated_balance

        return self.session_start_balance


    def update_balance(self, balance):

        balance = safe_float(balance)

        self._estimated_balance = balance

        if self.session_start_balance == 0:
            self.session_start_balance = balance

        if balance > self.peak_balance:
            self.peak_balance = balance

# ==========================================================
# RECOVERY FROM DB (COMPATIBILITY FIX)
# ==========================================================

    def recover_from_db(self):
        """
        DEPRECATED: Exchange is the sole source of truth.
        Kept for backward compatibility but does nothing.
        """
        logger.info("[RISK] recover_from_db skipped — exchange is sole source of truth")

# ==========================================================
# EXCHANGE SYNC (NEW)
# ==========================================================

    def sync_from_exchange(self, exchange_positions: list):
        """
        Rebuild open_futures from real exchange positions.
        Exchange is the single source of truth.

        Returns dict with sync results: {"added": [...], "removed": [...], "updated": [...]}
        """
        result = {"added": [], "removed": [], "updated": []}

        try:
            # Build map of exchange positions: symbol -> position data
            # Handles hedge mode: Bitget can return separate long+short per symbol
            exchange_map = {}
            for p in exchange_positions:
                size = safe_float(p.get("contracts"))
                if size <= 0:
                    continue
                symbol = p.get("symbol", "")
                side_raw = p.get("side", "")  # "long" or "short"
                entry = safe_float(p.get("entryPrice"))

                if symbol in exchange_map:
                    existing = exchange_map[symbol]
                    if existing["side"] != side_raw:
                        # Opposite sides (hedge) — net them
                        if size > existing["size"]:
                            exchange_map[symbol] = {
                                "side": side_raw,
                                "size": size - existing["size"],
                                "entry": entry,
                            }
                        elif size < existing["size"]:
                            existing["size"] -= size
                        else:
                            # Fully hedged (net zero) — not a real position
                            del exchange_map[symbol]
                    else:
                        # Same side — sum sizes
                        existing["size"] += size
                    continue

                exchange_map[symbol] = {
                    "side": side_raw,
                    "size": size,
                    "entry": entry,
                }

            with self._lock:
                # 1. Remove ghosts: in runtime but not on exchange
                ghosts = [s for s in self.open_futures if s not in exchange_map]
                for symbol in ghosts:
                    self.open_futures.pop(symbol, None)
                    self._pending_symbols.discard(symbol)
                    result["removed"].append(symbol)
                    logger.warning(f"[SYNC] ghost removed: {symbol}")

                # 2. Add missing: on exchange but not in runtime
                for symbol, edata in exchange_map.items():
                    if symbol not in self.open_futures:
                        side = "buy" if edata["side"] == "long" else "sell"
                        entry = edata["entry"]
                        trade = {
                            "symbol": symbol,
                            "side": side,
                            "entry": entry,
                            "size": edata["size"],
                            "stop_loss": entry * (0.995 if side == "buy" else 1.005),
                            "take_profit": entry * (1.005 if side == "buy" else 0.995),
                            "created_at": time.time(),
                            "market": "futures",
                        }
                        self.open_futures[symbol] = trade
                        result["added"].append(symbol)
                        logger.info(f"[SYNC] added missing: {symbol} {edata['side']}")
                    else:
                        # 3. Update size if exchange differs (exchange authoritative)
                        current = self.open_futures[symbol]
                        if abs(current.get("size", 0) - edata["size"]) > 1e-8:
                            current["size"] = edata["size"]
                            result["updated"].append(symbol)

        except Exception as e:
            logger.error(f"[SYNC] sync_from_exchange error: {e}")

        return result

    def rebuild(self, normalized_positions):
        """
        Full state rebuild from exchange positions list.
        Replaces all internal state with exchange truth.
        """
        with self._lock:
            self.open_futures.clear()
            self._pending_symbols.clear()
            for p in normalized_positions:
                symbol = p.get("symbol", "")
                side = p.get("side", "buy")
                entry = safe_float(p.get("entry"))
                size = safe_float(p.get("size"))
                if size <= 0:
                    continue
                self.open_futures[symbol] = {
                    "symbol": symbol,
                    "side": side,
                    "entry": entry,
                    "size": size,
                    "stop_loss": entry * (0.995 if side == "buy" else 1.005),
                    "take_profit": entry * (1.005 if side == "buy" else 0.995),
                    "created_at": time.time(),
                    "market": "futures",
                }
        logger.info(f"[RISK] rebuild: {len(self.open_futures)} positions loaded")

# ==========================================================
# POSITION SIDE QUERIES (NEW)
# ==========================================================

    def get_position_side(self, symbol):
        """Return current position side ('long'/'short') for a symbol, or None."""
        with self._lock:
            trade = self.open_futures.get(symbol)
            if trade:
                side = trade.get("side", "")
                return "long" if side == "buy" else "short"
            trade = self.open_spot.get(symbol)
            if trade:
                return "long"  # spot is always long
            return None

    def has_opposite_position(self, symbol, new_side):
        """Check if an existing position conflicts with new_side ('buy'/'sell')."""
        existing = self.get_position_side(symbol)
        if existing is None:
            return False
        new_direction = "long" if new_side == "buy" else "short"
        return existing != new_direction



# ==========================================================
# GLOBAL RISK CONTROL (NEW)
# ==========================================================

    def can_trade(self, symbol=None, available_balance=None):
        """
        Global risk gate — HARD position limit, no override.
        """

        if self.global_stop:
            logger.info("[RISK] can_trade=False reason=global_stop")
            return False

        with self._lock:
            active_count = len(self.open_futures)
            pending_count = len(self._pending_symbols)

            logger.info(
                f"[RISK] can_trade check: active={active_count} "
                f"pending={pending_count} max={self.MAX_POSITIONS} "
                f"open={list(self.open_futures.keys())} "
                f"balance={available_balance}"
            )

            # HARD LIMIT — absolute, includes pending (in-flight) trades
            if active_count + pending_count >= self.MAX_POSITIONS:
                logger.info(
                    f"[RISK] can_trade=False reason=hard_limit "
                    f"active={active_count} pending={pending_count} "
                    f"max={self.MAX_POSITIONS}"
                )
                return False

            if symbol:
                if symbol in self.open_futures or symbol in self.open_spot:
                    logger.info(f"[RISK] can_trade=False reason=symbol_open({symbol})")
                    return False
                if symbol in self._pending_symbols:
                    logger.info(f"[RISK] can_trade=False reason=symbol_pending({symbol})")
                    return False

        return True

    def check_exposure(self, equity, exchange_positions=None):
        """Block new trades if exposure (notional/equity) exceeds MAX_CAPITAL_USAGE (60%)."""
        exposure = self.calculate_exposure(exchange_positions, equity)
        if exposure > self.MAX_CAPITAL_USAGE:
            logger.info(
                f"[RISK] exposure={exposure:.1%} > max={self.MAX_CAPITAL_USAGE:.0%} — blocked"
            )
            return False
        return True

    def calculate_exposure(self, exchange_positions, equity):
        """Correct exposure = total_notional / equity. NOT margin-based."""
        if not exchange_positions or equity <= 0:
            return 0.0
        total_notional = 0.0
        for p in exchange_positions:
            notional = safe_float(p.get("notional", 0))
            if notional > 0:
                total_notional += abs(notional)
            else:
                # Fallback: contracts * entry
                contracts = safe_float(p.get("contracts"))
                entry = safe_float(p.get("entryPrice"))
                if contracts > 0 and entry > 0:
                    total_notional += contracts * entry
        return total_notional / equity


    def is_symbol_open(self, symbol):
        """Check if a symbol already has an open position."""
        with self._lock:
            return symbol in self.open_futures or symbol in self.open_spot


    def check_global_risk(self, balance):
        balance = safe_float(balance)
        if self.peak_balance <= 0 or balance <= 0:
            return True

        drawdown = (self.peak_balance - balance) / self.peak_balance * 100
        max_dd = safe_float(getattr(settings, "MAX_DRAWDOWN_PCT", 20))

        if drawdown > max_dd:
            if not self.global_stop:
                self._global_stop_reason = f"drawdown {drawdown:.1f}% > max {max_dd}%"
                self._global_stop_since = time.time()
            self.global_stop = True
            logger.error(f"[RISK] GLOBAL STOP — drawdown {drawdown:.1f}% > max {max_dd}%")
            return False

        return True

    def get_block_reason(self) -> str:
        """Return a human-readable reason why trading is currently blocked, or 'ok'."""
        if self.global_stop:
            reason = self._global_stop_reason or "unknown"
            age_min = (time.time() - self._global_stop_since) / 60 if self._global_stop_since else 0
            return f"global_stop({reason}, active {age_min:.1f}min)"

        with self._lock:
            open_count = len(self.open_futures)
        if open_count >= self.MAX_POSITIONS:
            return f"hard_limit({open_count}/{self.MAX_POSITIONS} positions)"

        return "ok"

    def global_stop_age_minutes(self) -> float:
        """Return how many minutes global_stop has been active (0 if not active)."""
        if not self.global_stop or not self._global_stop_since:
            return 0.0
        return (time.time() - self._global_stop_since) / 60

# ==========================================================
# CAPITAL & EXPOSURE CONTROL
# ==========================================================

    def get_used_margin(self, exchange_positions=None):
        """Calculate total used margin from exchange positions."""
        if not exchange_positions:
            return 0.0
        total = 0.0
        for p in exchange_positions:
            margin = safe_float(p.get("initialMargin") or p.get("margin") or 0)
            if margin > 0:
                total += margin
            else:
                # Fallback: notional / leverage
                contracts = safe_float(p.get("contracts"))
                entry = safe_float(p.get("entryPrice"))
                leverage = safe_float(p.get("leverage", 10))
                if contracts > 0 and entry > 0 and leverage > 0:
                    total += (contracts * entry) / leverage
        return total

    def check_capital_usage(self, available_balance, exchange_positions=None):
        """Block new trades if capital usage exceeds MAX_CAPITAL_USAGE."""
        used_margin = self.get_used_margin(exchange_positions)
        total = available_balance + used_margin
        if total <= 0:
            return True  # can't determine, allow
        usage = used_margin / total
        if usage > self.MAX_CAPITAL_USAGE:
            logger.info(
                f"[RISK] capital_usage={usage:.1%} > max={self.MAX_CAPITAL_USAGE:.0%} — blocking new trades"
            )
            return False
        return True

    def record_daily_pnl(self, pnl_pct):
        """Accumulate daily PnL. Resets at midnight."""
        import datetime as dt
        today = dt.date.today()
        if self._daily_reset_date != today:
            self._daily_pnl = 0.0
            self._daily_reset_date = today
        self._daily_pnl += safe_float(pnl_pct)

    def check_daily_loss(self):
        """Return False if daily loss limit exceeded."""
        import datetime as dt
        today = dt.date.today()
        if self._daily_reset_date != today:
            self._daily_pnl = 0.0
            self._daily_reset_date = today
            return True
        if self._daily_pnl <= -self.MAX_DAILY_LOSS_PCT:
            logger.warning(
                f"[RISK] daily_pnl={self._daily_pnl:.2f}% <= "
                f"-{self.MAX_DAILY_LOSS_PCT}% — DAILY STOP"
            )
            return False
        return True

# ==========================================================
# POSITION SIZE
# ==========================================================

    def compute_position_size(self, balance, price, leverage, risk_pct=None,
                              exchange_positions=None):
        """
        Correct position sizing with dynamic risk and capital usage cap.

        Dynamic risk_pct: min(configured_risk, available_balance / total_balance * 0.5)
        This automatically reduces size when capital is already deployed.

        Returns dict: {size, notional, required_margin} or None if invalid.
        """
        try:
            balance = safe_float(balance)
            price = safe_float(price)
            leverage = max(1, int(leverage))

            if balance <= 0 or price <= 0:
                return None

            # Capital usage check — block if over limit
            if not self.check_capital_usage(balance, exchange_positions):
                return None

            # Dynamic risk: scale down as capital usage increases
            if risk_pct is None:
                base_risk = safe_float(settings.MAX_RISK_PCT) / 100
                used_margin = self.get_used_margin(exchange_positions)
                total_capital = balance + used_margin
                if total_capital > 0:
                    available_ratio = balance / total_capital
                    risk_pct = min(base_risk, available_ratio * 0.5)
                    risk_pct = max(risk_pct, 0.005)  # floor at 0.5%
                else:
                    risk_pct = base_risk

            # Core formula: risk capital * leverage = notional
            risk_capital = balance * risk_pct
            notional = risk_capital * leverage

            # HARD SAFETY CAP — never exceed 80% of max leveraged exposure
            max_notional = balance * leverage * 0.8
            notional = min(notional, max_notional)

            # Minimum notional (exchange rejects below ~5 USDT)
            min_notional = 10
            if notional < min_notional:
                notional = min_notional

            # Margin validation
            required_margin = notional / leverage
            if required_margin > balance:
                logger.warning(
                    f"[RISK] margin check FAILED: required={required_margin:.2f} "
                    f"available={balance:.2f}"
                )
                return None

            size = notional / price

            if size <= 0:
                return None

            return {
                "size": round(size, 6),
                "notional": round(notional, 2),
                "required_margin": round(required_margin, 2),
                "risk_pct": round(risk_pct, 4),
            }

        except Exception as e:
            logger.error(f"[RISK] compute_position_size error {e}")
            return None

    def position_size(self, balance, entry, stop_loss, atr=None, market="spot", symbol=""):
        """Legacy method — kept for backward compatibility."""
        leverage = safe_float(getattr(settings, "DEFAULT_LEVERAGE", 10))
        result = self.compute_position_size(balance, entry, leverage)
        if result is None:
            return 0
        return result["size"]


# ==========================================================
# TRAILING STOP (NEW)
# ==========================================================

    def apply_trailing(self, trade, price):

        try:

            entry = safe_float(trade.get("entry"))
            stop = safe_float(trade.get("stop_loss"))
            side = trade.get("side")

            if entry <= 0 or price <= 0:
                return trade

            profit_pct = (price - entry) / entry * 100

            if side == "sell":
                profit_pct = -profit_pct

            # trailing attivo sopra 1.5%
            if profit_pct > 1.5:

                new_stop = price * 0.99

                if side == "buy":
                    trade["stop_loss"] = max(stop, new_stop)
                else:
                    trade["stop_loss"] = min(stop, new_stop)

            return trade

        except Exception as e:

            logger.error(f"[RISK] trailing error {e}")
            return trade


# ==========================================================
# SYMBOL LOCK
# ==========================================================

    def reserve_symbol(self, symbol):

        with self._lock:

            if symbol in self._pending_symbols:
                return False

            self._pending_symbols.add(symbol)
            return True


    def release_symbol(self, symbol):

        with self._lock:
            self._pending_symbols.discard(symbol)


# ==========================================================
# TRADE REGISTRY
# ==========================================================

    def register_open(self, symbol, trade, market):

        try:

            with self._lock:

                # Thread-safe duplicate guard
                if market == "spot":
                    if symbol in self.open_spot:
                        logger.warning(f"[RISK] duplicate blocked: {symbol} spot already open")
                        return False
                    self.open_spot[symbol] = trade
                else:
                    if symbol in self.open_futures:
                        logger.warning(f"[RISK] duplicate blocked: {symbol} futures already open")
                        return False
                    self.open_futures[symbol] = trade

                # Release pending lock now that position is registered
                self._pending_symbols.discard(symbol)

            return True

        except Exception as e:

            logger.error(f"[RISK] register_open error {e}")
            return False


    def register_close(self, symbol, pnl_pct, market, reason):

        try:

            pnl_pct = safe_float(pnl_pct)

            with self._lock:

                if pnl_pct > 0:
                    self.wins += 1
                else:
                    self.losses += 1

                self._recent_pnls.append(pnl_pct)

                if market == "spot":
                    self.open_spot.pop(symbol, None)
                else:
                    self.open_futures.pop(symbol, None)

                # Ensure pending lock is also released
                self._pending_symbols.discard(symbol)

            # Track daily PnL (outside lock — no deadlock risk)
            self.record_daily_pnl(pnl_pct)

            logger.info(
                f"[RISK] closed {symbol} pnl={pnl_pct:.2f}% reason={reason} "
                f"daily_pnl={self._daily_pnl:.2f}%"
            )

        except Exception as e:

            logger.error(f"[RISK] register_close error {e}")


# ==========================================================
# OPEN TRADES
# ==========================================================

    def open_symbols(self):
        """Return set of all symbols with open positions."""
        with self._lock:
            return set(self.open_spot.keys()) | set(self.open_futures.keys())

    def all_open_trades(self):

        trades = []

        for sym, t in self.open_spot.items():

            trade = t.copy()
            trade["symbol"] = sym
            trade["market"] = "spot"

            trades.append(trade)

        for sym, t in self.open_futures.items():

            trade = t.copy()
            trade["symbol"] = sym
            trade["market"] = "futures"

            trades.append(trade)

        return trades


# ==========================================================
# CLOSE CONDITIONS (ENHANCED)
# ==========================================================

    def should_close(self, trade, price):

        try:

            trade = self.apply_trailing(trade, price)

            entry = safe_float(trade.get("entry"))
            stop = safe_float(trade.get("stop_loss"))
            tp = safe_float(trade.get("take_profit"))
            price = safe_float(price)

            side = trade.get("side")

            if entry <= 0 or price <= 0:
                return False, None

            if side == "buy":

                if stop > 0 and price <= stop:
                    return True, "stop_loss"

                if tp > 0 and price >= tp:
                    return True, "take_profit"

            else:

                if stop > 0 and price >= stop:
                    return True, "stop_loss"

                if tp > 0 and price <= tp:
                    return True, "take_profit"

            return False, None

        except Exception as e:

            logger.error(f"[RISK] should_close error {e}")
            return False, None


# ==========================================================
# STATS
# ==========================================================

    def stats(self):

        return {
            "open_trades": len(self.open_spot) + len(self.open_futures),
            "wins": self.wins,
            "losses": self.losses
        }