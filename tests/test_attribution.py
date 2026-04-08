"""
FASE-1 Attribution Tests
========================
Uses SQLite in-memory — no PostgreSQL required for CI.
Tests:
  1. save_trade_open inserts correctly with strategy + new fields
  2. close_position_by_symbol updates correctly with fees_paid
  3. daily_strategy_report groups correctly by strategy
  4. save_trade_open REJECTS trades with strategy='sniper' or empty strategy
  5. signal_snapshot round-trips as JSON
"""

import json
import sys
import os
import time
import pytest

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db_in_memory():
    """
    Create an in-memory SQLite DB instance bypassing DATABASE_URL.
    We patch just enough to make DB work with SQLite.
    """
    from sqlalchemy import create_engine
    from sqlalchemy.pool import StaticPool
    from trading_bot.models.database import Base, DB

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)

    # Add attribution columns (SQLite doesn't support IF NOT EXISTS for columns,
    # so we do it via SQLAlchemy text, ignoring errors for existing columns)
    from sqlalchemy import text
    with engine.connect() as conn:
        for stmt in [
            "ALTER TABLE trades ADD COLUMN fees_paid REAL;",
            "ALTER TABLE trades ADD COLUMN signal_snapshot TEXT;",
        ]:
            try:
                conn.execute(text(stmt))
                conn.commit()
            except Exception:
                pass  # column already exists

    db = DB()
    db.engine = engine
    db.enabled = True
    return db


def _order_id():
    return f"test_{int(time.time() * 1000)}"


# ---------------------------------------------------------------------------
# 1. Basic insert
# ---------------------------------------------------------------------------

def test_save_trade_open_basic(db_in_memory):
    db = db_in_memory
    oid = _order_id()
    db.save_trade_open(
        order_id=oid, symbol="BTC/USDT:USDT", market="futures",
        strategy="MOMENTUM", side="buy",
        entry=65000.0, size=0.001, stop_loss=64675.0, take_profit=65325.0,
        confidence=75.0, atr=320.0, notes="test", timeframe="1m", leverage=2,
        fees_paid=0.039, signal_snapshot={"score": 82.1, "rsi": 61.3},
    )

    from sqlalchemy.orm import Session
    from trading_bot.models.database import Trade
    with Session(db.engine) as s:
        t = s.query(Trade).filter_by(order_id=oid).first()

    assert t is not None
    assert t.strategy == "MOMENTUM"
    assert t.fees_paid == pytest.approx(0.039)
    assert t.signal_snapshot is not None
    snap = json.loads(t.signal_snapshot)
    assert snap["score"] == pytest.approx(82.1)
    assert t.status == "open"


# ---------------------------------------------------------------------------
# 2. Close updates fees correctly
# ---------------------------------------------------------------------------

def test_close_accumulates_fees(db_in_memory):
    db = db_in_memory
    oid = _order_id()
    db.save_trade_open(
        order_id=oid, symbol="ETH/USDT:USDT", market="futures",
        strategy="MOMENTUM", side="sell",
        entry=3200.0, size=0.01, stop_loss=3216.0, take_profit=3168.0,
        confidence=70.0, atr=18.0, notes="", timeframe="1m", leverage=2,
        fees_paid=0.019,
    )
    db.close_position_by_symbol(
        "ETH/USDT:USDT", exit_price=3175.0,
        pnl_pct=0.78, pnl_usdt=0.25,
        reason="take_profit", fees_paid=0.019,
    )

    from sqlalchemy.orm import Session
    from trading_bot.models.database import Trade
    with Session(db.engine) as s:
        t = s.query(Trade).filter_by(order_id=oid).first()

    assert t.status == "closed"
    assert t.fees_paid == pytest.approx(0.038)   # entry + exit fee
    assert t.close_reason == "take_profit"
    assert t.pnl_pct == pytest.approx(0.78)


# ---------------------------------------------------------------------------
# 3. Reporting groups by strategy correctly
# ---------------------------------------------------------------------------

def test_daily_strategy_report(db_in_memory):
    db = db_in_memory
    from trading_bot.reporting.strategy_report import daily_strategy_report

    # Insert 3 MOMENTUM trades (2 wins, 1 loss) + 1 old strategy
    trades = [
        ("MOMENTUM", "BTC/USDT:USDT", 0.5,  0.5,  "take_profit", 0.04),
        ("MOMENTUM", "ETH/USDT:USDT", 0.4,  0.4,  "take_profit", 0.02),
        ("MOMENTUM", "SOL/USDT:USDT", -0.3, -0.3, "stop_loss",   0.02),
        ("RSI_MACD", "BTC/USDT:USDT", 0.1,  0.1,  "take_profit", 0.01),
    ]

    for strat, sym, pnl_pct, pnl_usdt, reason, fees in trades:
        oid = _order_id()
        db.save_trade_open(
            order_id=oid, symbol=sym, market="futures",
            strategy=strat, side="buy",
            entry=100.0, size=1.0, stop_loss=99.0, take_profit=101.5,
            confidence=70.0, atr=1.0, notes="", timeframe="1m", leverage=2,
            fees_paid=fees,
        )
        db.close_position_by_symbol(
            sym, exit_price=101.0,
            pnl_pct=pnl_pct, pnl_usdt=pnl_usdt,
            reason=reason, fees_paid=fees,
        )

    # Use a long lookback to catch all test rows
    report = daily_strategy_report(db, days=365)

    assert "MOMENTUM" in report
    assert "RSI_MACD" in report
    assert "_totals" in report

    m = report["MOMENTUM"]
    assert m["trades"] == 3
    assert m["wins"] == 2
    assert m["losses"] == 1
    assert m["win_rate"] == pytest.approx(66.7, abs=0.2)
    # entry fee + exit fee accumulated: (0.04+0.04) + (0.02+0.02) + (0.02+0.02) = 0.16
    assert m["total_fees_usdt"] == pytest.approx(0.16, abs=0.01)

    r = report["RSI_MACD"]
    assert r["trades"] == 1
    assert r["wins"] == 1

    totals = report["_totals"]
    assert totals["trades"] == 4


# ---------------------------------------------------------------------------
# 4. strategy enforcement — rejects 'sniper' and empty string
# ---------------------------------------------------------------------------

def test_rejects_sniper_strategy(db_in_memory):
    db = db_in_memory
    with pytest.raises(ValueError, match="strategy is required"):
        db.save_trade_open(
            order_id=_order_id(), symbol="BTC/USDT:USDT", market="futures",
            strategy="sniper", side="buy",
            entry=65000.0, size=0.001, stop_loss=64675.0, take_profit=65325.0,
            confidence=70.0, atr=320.0, notes="", timeframe="1m",
        )


def test_rejects_empty_strategy(db_in_memory):
    db = db_in_memory
    with pytest.raises(ValueError, match="strategy is required"):
        db.save_trade_open(
            order_id=_order_id(), symbol="BTC/USDT:USDT", market="futures",
            strategy="", side="buy",
            entry=65000.0, size=0.001, stop_loss=64675.0, take_profit=65325.0,
            confidence=70.0, atr=320.0, notes="", timeframe="1m",
        )


# ---------------------------------------------------------------------------
# 5. signal_snapshot round-trip
# ---------------------------------------------------------------------------

def test_signal_snapshot_roundtrip(db_in_memory):
    db = db_in_memory
    snap = {"rsi": 68.2, "macd_hist": 0.0034, "atr": 320.5, "score": 79.0, "direction": "long"}
    oid = _order_id()
    db.save_trade_open(
        order_id=oid, symbol="SOL/USDT:USDT", market="futures",
        strategy="MOMENTUM", side="buy",
        entry=140.0, size=0.5, stop_loss=139.3, take_profit=140.7,
        confidence=79.0, atr=0.7, notes="", timeframe="1m",
        signal_snapshot=snap,
    )

    from sqlalchemy.orm import Session
    from trading_bot.models.database import Trade
    with Session(db.engine) as s:
        t = s.query(Trade).filter_by(order_id=oid).first()

    recovered = json.loads(t.signal_snapshot)
    assert recovered == snap
