"""
Smoke test for attribution layer.
=================================
Creates a fake MOMENTUM paper trade in-memory, verifies:
  1. save_trade_open persists fees_paid + signal_snapshot
  2. close_position_by_symbol accumulates exit fees and sets status
  3. daily_strategy_report returns correct grouping
  4. MomentumStrategy.analyze() returns a Signal (no crash)
  5. risk_manager.get_block_reason() returns a string

Usage:
    python scripts/smoke_test_attribution.py
"""

import sys
import os
import json
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _check(label, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    print(f"  [{status}] {label}" + (f" — {detail}" if detail else ""))
    if not condition:
        sys.exit(1)


def run():
    print("\n── Smoke: attribution layer ────────────────────────────────────")

    # ── 1. In-memory DB ─────────────────────────────────────────────────────
    from sqlalchemy import create_engine, text
    from sqlalchemy.pool import StaticPool
    from trading_bot.models.database import Base, DB

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    with engine.connect() as conn:
        for stmt in [
            "ALTER TABLE trades ADD COLUMN fees_paid REAL;",
            "ALTER TABLE trades ADD COLUMN signal_snapshot TEXT;",
        ]:
            try:
                conn.execute(text(stmt))
                conn.commit()
            except Exception:
                pass

    db = DB()
    db.engine = engine
    db.enabled = True

    # ── 2. save_trade_open ──────────────────────────────────────────────────
    oid = f"smoke_{int(time.time())}"
    snap = {"ema8": 65100.0, "ema21": 64900.0, "macd_hist": 0.0042, "atr": 320.0, "scanner_score": 45.0}
    db.save_trade_open(
        order_id=oid, symbol="BTC/USDT:USDT", market="futures",
        strategy="MOMENTUM", side="buy",
        entry=65000.0, size=0.001, stop_loss=64680.0, take_profit=65800.0,
        confidence=82.0, atr=320.0, notes="smoke", timeframe="1m", leverage=2,
        fees_paid=0.039, signal_snapshot=snap,
    )

    from sqlalchemy.orm import Session
    from trading_bot.models.database import Trade
    with Session(db.engine) as s:
        t = s.query(Trade).filter_by(order_id=oid).first()

    _check("trade inserted", t is not None)
    _check("strategy=MOMENTUM", t.strategy == "MOMENTUM")
    _check("fees_paid persisted", abs((t.fees_paid or 0) - 0.039) < 1e-5, f"got {t.fees_paid}")
    _check("signal_snapshot not null", t.signal_snapshot is not None)
    recovered = json.loads(t.signal_snapshot)
    _check("snapshot round-trip", recovered["atr"] == 320.0, f"got {recovered}")

    # ── 3. close_position_by_symbol accumulates fees ────────────────────────
    db.close_position_by_symbol(
        "BTC/USDT:USDT", exit_price=65750.0,
        pnl_pct=1.15, pnl_usdt=0.74,
        reason="take_profit", fees_paid=0.039,
    )
    with Session(db.engine) as s:
        t = s.query(Trade).filter_by(order_id=oid).first()

    _check("status=closed", t.status == "closed")
    _check("fees accumulated (entry+exit)", abs((t.fees_paid or 0) - 0.078) < 1e-4, f"got {t.fees_paid}")
    _check("close_reason=take_profit", t.close_reason == "take_profit")

    # ── 4. daily_strategy_report ────────────────────────────────────────────
    from trading_bot.reporting.strategy_report import daily_strategy_report, format_daily_report
    report = daily_strategy_report(db, days=365)
    _check("MOMENTUM in report", "MOMENTUM" in report)
    _check("_totals in report", "_totals" in report)
    m = report["MOMENTUM"]
    _check("1 trade in report", m["trades"] == 1, f"got {m['trades']}")
    _check("1 win", m["wins"] == 1, f"got {m['wins']}")

    html = format_daily_report(report, balance=100.0, mode="paper")
    _check("format_daily_report returns str", isinstance(html, str))
    _check("MOMENTUM in formatted report", "MOMENTUM" in html)

    # ── 5. MomentumStrategy.analyze() smoke ─────────────────────────────────
    import pandas as pd
    import numpy as np
    from trading_bot.strategies.momentum import MomentumStrategy

    n = 50
    rng = np.random.default_rng(42)
    closes = 100 + np.cumsum(rng.normal(0, 0.5, n))
    df = pd.DataFrame({
        "open":  closes - 0.1,
        "high":  closes + 0.3,
        "low":   closes - 0.3,
        "close": closes,
        "volume": rng.uniform(1e6, 2e6, n),
    })

    strat = MomentumStrategy()
    sig = strat.analyze(
        df, "BTC/USDT:USDT", "futures",
        scanner_score=50.0,
        scanner_direction="long",
        scanner_volume=10_000_000,
    )
    _check("MomentumStrategy.analyze() does not crash", True)
    if sig:
        _check("Signal has _snapshot", hasattr(sig, "_snapshot") and sig._snapshot is not None)
        _check("Signal strategy=MOMENTUM", sig.strategy == "MOMENTUM")
        print(f"         → signal: side={sig.side} conf={sig.confidence:.0f} entry={sig.entry:.4f}")
    else:
        print("         → no signal (EMA/MACD not aligned — OK for this random series)")

    # ── 6. RiskManager.get_block_reason() ──────────────────────────────────
    from trading_bot.utils.risk_manager import RiskManager
    risk = RiskManager()
    reason = risk.get_block_reason()
    _check("get_block_reason() returns str", isinstance(reason, str), f"got {reason!r}")
    _check("no global_stop → reason=ok", reason == "ok", f"got {reason!r}")

    risk.global_stop = True
    risk._global_stop_reason = "drawdown 25.0% > max 20%"
    risk._global_stop_since = time.time() - 600
    reason2 = risk.get_block_reason()
    _check("global_stop → reason contains 'global_stop'", "global_stop" in reason2, f"got {reason2!r}")

    print("\n── ALL SMOKE TESTS PASSED ──────────────────────────────────────\n")


if __name__ == "__main__":
    run()
