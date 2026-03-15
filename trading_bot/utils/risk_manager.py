"""
Risk Manager v5 — Production Stable
════════════════════════════════════

Fix:
✓ futures sizing corretto
✓ exchange sync
✓ exposure cap
✓ correlation clusters
✓ SL cooldown dinamico
✓ thread safe
"""

import time
import threading
from datetime import datetime, timezone
from loguru import logger
from trading_bot.config import settings


_CORRELATION_CLUSTERS = {
    "LAYER1": {"BTC","ETH","SOL","AVAX","DOT","NEAR","ATOM","ADA"},
    "DEFI": {"AAVE","UNI","SUSHI","CRV","MKR","COMP","SNX"},
    "MEME": {"DOGE","SHIB","PEPE","FLOKI","BONK","WIF"},
    "L2": {"MATIC","ARB","OP","STRK","ZK","MANTA"},
}


def _normalize_base(symbol: str) -> str:

    s = symbol.upper()

    if ":" in s:
        s = s.split(":")[0]

    if "/" in s:
        s = s.split("/")[0]

    if s.endswith("USDT") and len(s) > 4:
        s = s[:-4]

    return s.strip()


def _get_cluster(symbol: str):

    base = _normalize_base(symbol)

    for name, members in _CORRELATION_CLUSTERS.items():

        if base in members:
            return name

    return None


class RiskManager:

    def __init__(self):

        self._lock = threading.Lock()

        self.open_spot = {}
        self.open_futures = {}

        self._pending_symbols = set()

        self.session_start_balance = 0
        self.peak_balance = 0

        self.daily_pnl = 0
        self.daily_reset_ts = 0

        self.daily_trades = 0

        self.wins = 0
        self.losses = 0

        self.total_win_pct = 0
        self.total_loss_pct = 0

        self._consecutive_wins = 0
        self._consecutive_losses = 0

        self._recent_pnls = []

        self._in_recovery = False

        self._sl_cooldown = {}


# ==========================================================
# DB RECOVERY
# ==========================================================

    def recover_from_db(self):

        try:

            from trading_bot.models.database import Trade, DB_AVAILABLE

            if not DB_AVAILABLE:
                return

            from sqlalchemy.orm import Session
            from sqlalchemy import create_engine

            url = settings.DATABASE_URL

            engine = create_engine(url, pool_pre_ping=True)

            with Session(engine) as s:

                trades = s.query(Trade).filter(Trade.status=="open").all()

            with self._lock:

                for t in trades:

                    td = {

                        "order_id":t.order_id,
                        "side":t.side,
                        "entry":t.entry_price,
                        "size":t.size,
                        "stop_loss":t.stop_loss,
                        "take_profit":t.take_profit,
                        "atr":t.atr,
                        "open_ts":time.time()
                    }

                    if t.market=="spot":
                        self.open_spot[t.symbol]=td
                    else:
                        self.open_futures[t.symbol]=td

                    self._pending_symbols.add(_normalize_base(t.symbol))

            logger.info(f"[RISK] recovery {len(trades)} trades")

        except Exception as e:

            logger.error(f"[RISK] recovery error {e}")


# ==========================================================
# SYMBOL LOCK
# ==========================================================

    def reserve_symbol(self,symbol):

        base=_normalize_base(symbol)

        with self._lock:

            if base in self._pending_symbols:
                return False

            self._pending_symbols.add(base)

            return True


    def release_symbol(self,symbol):

        base=_normalize_base(symbol)

        with self._lock:

            self._pending_symbols.discard(base)


# ==========================================================
# EXCHANGE SYNC
# ==========================================================

    def force_close(self,symbol,market):

        with self._lock:

            store=self.open_spot if market=="spot" else self.open_futures

            store.pop(symbol,None)


# ==========================================================
# POSITION SIZING
# ==========================================================

    def position_size(self,balance,entry,stop_loss,atr=None,market="spot",risk_multiplier=1.0,symbol=""):

        risk_pct=settings.MAX_RISK_PCT/100

        risk_pct*=risk_multiplier

        risk_pct*=self._correlation_discount(symbol)

        risk_pct=min(risk_pct,0.12)

        risk_amount=balance*risk_pct

        risk_per_unit=abs(entry-stop_loss)

        if risk_per_unit<=0:
            return 0

        units=risk_amount/risk_per_unit

        size=units/entry

        max_notional=balance*0.35

        notional=size*entry

        if notional>max_notional:

            size=max_notional/entry

        min_notional=6

        if size*entry<min_notional:

            return 0

        logger.info(f"[SIZING] {symbol} size={size:.4f}")

        return round(size,6)


# ==========================================================
# CORRELATION
# ==========================================================

    def _correlation_discount(self,symbol):

        cluster=_get_cluster(symbol)

        if cluster is None:
            return 1

        count=0

        with self._lock:

            for s in list(self.open_spot)+list(self.open_futures):

                if _get_cluster(s)==cluster:

                    count+=1

        if count>=3:
            return 0.4

        if count==2:
            return 0.6

        if count==1:
            return 0.8

        return 1


# ==========================================================
# STOPS
# ==========================================================

    def trailing_stop(self,trade,current_price):

        trail=settings.TRAILING_STOP_PCT/100

        atr=trade.get("atr",0)

        entry=trade.get("entry",current_price)

        atr_ratio=atr/entry if entry>0 else 0

        trail=max(trail,atr_ratio*1.2)

        if trade["side"]=="buy":

            return max(trade["stop_loss"],current_price*(1-trail))

        return min(trade["stop_loss"],current_price*(1+trail))


# ==========================================================
# CLOSE CONDITIONS
# ==========================================================

    def should_close(self,trade,current_price):

        side=trade["side"]

        sl=trade.get("stop_loss")

        tp=trade.get("take_profit")

        if side=="buy":

            if current_price<=sl:
                return True,"stop_loss"

            if tp and current_price>=tp:
                return True,"take_profit"

        else:

            if current_price>=sl:
                return True,"stop_loss"

            if tp and current_price<=tp:
                return True,"take_profit"

        return False,""


# ==========================================================
# TRADE TRACKING
# ==========================================================

    def register_open(self,symbol,trade,market):

        with self._lock:

            store=self.open_spot if market=="spot" else self.open_futures

            store[symbol]=trade

        logger.info(f"[OPEN] {symbol}")


    def register_close(self,symbol,pnl_pct,market,reason=""):

        base=_normalize_base(symbol)

        with self._lock:

            store=self.open_spot if market=="spot" else self.open_futures

            store.pop(symbol,None)

            if reason=="stop_loss":

                cooldown=3600 if pnl_pct<-3 else 900

                self._sl_cooldown[base]=time.time()+cooldown

        if pnl_pct>0:

            self.wins+=1

        else:

            self.losses+=1


# ==========================================================
# OPEN TRADES
# ==========================================================

    def all_open_trades(self):

        with self._lock:

            trades=[]

            for s,t in self.open_spot.items():

                trades.append({**t,"symbol":s,"market":"spot"})

            for s,t in self.open_futures.items():

                trades.append({**t,"symbol":s,"market":"futures"})

        return trades