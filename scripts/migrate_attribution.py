"""
Migration: Attribution columns on trades table
==============================================
Idempotent. Safe to run multiple times.
Adds: fees_paid (REAL), signal_snapshot (TEXT/JSON)

Usage:
    python scripts/migrate_attribution.py
or called automatically from DB.run_migrations() at startup.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ---------------------------------------------------------------------------
# SQL — PostgreSQL-flavoured (ALTER TABLE … ADD COLUMN IF NOT EXISTS)
# ---------------------------------------------------------------------------

MIGRATIONS = [
    # fees_paid: estimated taker fee in USDT at the time of open/close
    """
    ALTER TABLE trades
    ADD COLUMN IF NOT EXISTS fees_paid REAL;
    """,

    # signal_snapshot: JSON string of indicator values captured at signal time
    # e.g. {"rsi": 72.3, "macd_hist": 0.012, "atr": 45.2, "score": 81.5}
    """
    ALTER TABLE trades
    ADD COLUMN IF NOT EXISTS signal_snapshot TEXT;
    """,

    # index for time-range queries used by daily_strategy_report()
    """
    CREATE INDEX IF NOT EXISTS ix_trades_opened_at ON trades (opened_at);
    """,
]


def run(engine=None):
    """
    Run all migrations against the given SQLAlchemy engine.
    If engine is None, builds one from DATABASE_URL env var.
    """
    if engine is None:
        from sqlalchemy import create_engine
        from sqlalchemy.pool import NullPool

        db_url = os.getenv("DATABASE_URL", "")
        if not db_url:
            print("[MIGRATION] DATABASE_URL not set — skipping")
            return

        connect_args = {"sslmode": "require"} if "railway" in db_url else {}
        engine = create_engine(db_url, poolclass=NullPool, connect_args=connect_args)

    from sqlalchemy import text

    with engine.connect() as conn:
        for sql in MIGRATIONS:
            stmt = sql.strip()
            try:
                conn.execute(text(stmt))
                conn.commit()
                # Print first 60 chars of the statement as summary
                summary = " ".join(stmt.split())[:60]
                print(f"[MIGRATION] OK  — {summary}")
            except Exception as e:
                # Non-fatal: log and continue (e.g. index already exists with different def)
                print(f"[MIGRATION] WARN — {e}")

    print("[MIGRATION] attribution migration complete")


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    run()
