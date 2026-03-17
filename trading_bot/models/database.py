"""
Database Layer — SQLAlchemy + PostgreSQL
Persiste tutti i trade, i segnali e le performance.
"""

from __future__ import annotations
import json
from datetime import datetime, timezone
from loguru import logger

try:
    from sqlalchemy import (
        create_engine, Column, String, Float, Integer,
        Boolean, DateTime, Text, Index
    )
    from sqlalchemy.orm import declarative_base, Session
    from sqlalchemy.pool import NullPool
    DB_AVAILABLE = True
except ImportError:
    DB_AVAILABLE = False

from trading_bot.config import settings

Base = declarative_base() if DB_AVAILABLE else object


class Trade(Base):
    __tablename__ = "trades"

    id          = Column(Integer, primary_key=True, autoincrement=True)
    order_id    = Column(String(64), unique=True, nullable=False)
    symbol      = Column(String(32), nullable=False)
    market      = Column(String(16), nullable=False)   # spot | futures
    strategy    = Column(String(32), nullable=False)
    side        = Column(String(8),  nullable=False)   # buy | sell
    status      = Column(String(16), default="open")   # open | closed | cancelled

    # Pricing
    entry_price  = Column(Float, nullable=False)
    exit_price   = Column(Float, nullable=True)
    stop_loss    = Column(Float, nullable=False)
    take_profit  = Column(Float, nullable=False)
    size         = Column(Float, nullable=False)
    leverage     = Column(Integer, default=1)

    # PnL
    pnl_pct      = Column(Float, nullable=True)
    pnl_usdt     = Column(Float, nullable=True)
    close_reason = Column(String(32), nullable=True)   # stop_loss | take_profit | manual | trailing

    # Meta
    confidence  = Column(Float, nullable=False)
    atr         = Column(Float, nullable=True)
    notes       = Column(Text, nullable=True)
    timeframe   = Column(String(8), nullable=True)
    is_paper    = Column(Boolean, default=True)

    opened_at   = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    closed_at   = Column(DateTime, nullable=True)

    __table_args__ = (
        Index("ix_trades_symbol",   "symbol"),
        Index("ix_trades_strategy", "strategy"),
        Index("ix_trades_status",   "status"),
    )


class DB:
    def __init__(self):
        self.engine  = None
        self.session = None
        self.enabled = False

    def connect(self):
        if not DB_AVAILABLE or not settings.DATABASE_URL:
            logger.warning("Database non disponibile — trades non persistiti")
            return

        try:
            self.engine = create_engine(
                settings.DATABASE_URL,
                poolclass=NullPool,
                connect_args={"sslmode": "require"} if "railway" in settings.DATABASE_URL else {}
            )
            Base.metadata.create_all(self.engine)
            self.enabled = True
            logger.info("Database PostgreSQL connesso")
        except Exception as e:
            logger.error(f"DB connect error: {e}")

    def save_trade_open(self, order_id: str, symbol: str, market: str,
                        strategy: str, side: str, entry: float, size: float,
                        stop_loss: float, take_profit: float, confidence: float,
                        atr: float, notes: str, timeframe: str, leverage: int = 1):
        if not self.enabled:
            return
        try:
            with Session(self.engine) as s:
                trade = Trade(
                    order_id    = order_id,
                    symbol      = symbol,
                    market      = market,
                    strategy    = strategy,
                    side        = side,
                    entry_price = entry,
                    size        = size,
                    stop_loss   = stop_loss,
                    take_profit = take_profit,
                    confidence  = confidence,
                    atr         = atr,
                    notes       = notes,
                    timeframe   = timeframe,
                    leverage    = leverage,
                    is_paper    = not settings.IS_LIVE,
                )
                s.add(trade)
                s.commit()
        except Exception as e:
            logger.error(f"DB save_trade_open: {e}")

    def save_trade_close(self, order_id: str, exit_price: float,
                         pnl_pct: float, pnl_usdt: float, reason: str):
        if not self.enabled:
            return
        try:
            with Session(self.engine) as s:
                trade = s.query(Trade).filter_by(order_id=order_id).first()
                if trade:
                    trade.exit_price  = exit_price
                    trade.pnl_pct     = pnl_pct
                    trade.pnl_usdt    = pnl_usdt
                    trade.close_reason = reason
                    trade.status      = "closed"
                    trade.closed_at   = datetime.now(timezone.utc)
                    s.commit()
        except Exception as e:
            logger.error(f"DB save_trade_close: {e}")

    def get_stats(self, days: int = 7) -> dict:
        if not self.enabled:
            return {}
        try:
            with Session(self.engine) as s:
                from sqlalchemy import func, text
                cutoff = f"NOW() - INTERVAL '{days} days'"
                closed = s.query(Trade).filter(
                    Trade.status == "closed",
                    Trade.closed_at >= text(cutoff)
                ).all()

                if not closed:
                    return {"trades": 0}

                wins   = [t for t in closed if (t.pnl_pct or 0) > 0]
                losses = [t for t in closed if (t.pnl_pct or 0) <= 0]
                return {
                    "trades":       len(closed),
                    "wins":         len(wins),
                    "losses":       len(losses),
                    "win_rate":     round(len(wins) / len(closed) * 100, 1),
                    "total_pnl":    round(sum(t.pnl_usdt or 0 for t in closed), 2),
                    "avg_win":      round(sum(t.pnl_pct or 0 for t in wins)   / len(wins)   if wins   else 0, 2),
                    "avg_loss":     round(sum(t.pnl_pct or 0 for t in losses) / len(losses) if losses else 0, 2),
                    "best_trade":   round(max((t.pnl_pct or 0 for t in closed), default=0), 2),
                    "worst_trade":  round(min((t.pnl_pct or 0 for t in closed), default=0), 2),
                }
        except Exception as e:
            logger.error(f"DB get_stats: {e}")
            return {}
# ==========================================================
# RECOVER OPEN TRADES
# ==========================================================

    def update_trade_status(self, symbol, status):

        try:

            query = """
            UPDATE trades
            SET status = %s,
                closed_at = %s
            WHERE symbol = %s
            AND status = 'open'
            """

            self.conn.execute(
                query,
                (status, int(time.time()), symbol)
            )

        except Exception as e:
            logger.error(f"[DB] update_trade_status error {e}")



    def get_open_trades(self):

        """
        Restituisce i trade ancora aperti nel formato atteso dal RiskManager.
        """

        if not self.enabled:
            return []

        try:

            with Session(self.engine) as s:

                trades = (
                    s.query(Trade)
                    .filter(Trade.status == "open")
                    .all()
                )

                out = []

                for t in trades:

                    out.append({

                        "order_id": t.order_id,
                        "symbol": t.symbol,
                        "market": t.market,
                        "side": t.side,

                        "entry": t.entry_price,
                        "size": t.size,

                        "stop_loss": t.stop_loss,
                        "take_profit": t.take_profit,

                        "atr": t.atr or 0

                    })

                logger.info(f"[DB] recovered {len(out)} open trades")

                return out

        except Exception as e:

            logger.error(f"DB get_open_trades: {e}")

            return []