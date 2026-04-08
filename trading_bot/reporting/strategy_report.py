"""
Per-strategy attribution reporting.
Queries the `trades` table and returns grouped performance stats.
"""

from __future__ import annotations
from datetime import datetime, timedelta, timezone
from loguru import logger


def daily_strategy_report(db, days: int = 1) -> dict:
    """
    Returns per-strategy performance stats for the last `days` day(s).

    Args:
        db: DB instance (trading_bot.models.database.DB)
        days: lookback window in days (default 1 = today)

    Returns dict keyed by strategy name:
        {
            "MOMENTUM": {
                "trades": 5,
                "wins": 3,
                "losses": 2,
                "win_rate": 60.0,
                "profit_factor": 1.8,
                "total_pnl_usdt": 2.34,
                "total_fees_usdt": 0.45,
                "net_pnl_usdt": 1.89,
                "avg_win_pct": 0.52,
                "avg_loss_pct": -0.29,
                "top_loss_symbol": "SOL/USDT:USDT",
                "top_loss_usdt": -1.12,
            },
            ...
            "_totals": { ... aggregate across all strategies ... }
        }
    """
    if not db.enabled:
        return {}

    try:
        from sqlalchemy.orm import Session
        from sqlalchemy import text
        from trading_bot.models.database import Trade

        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

        with Session(db.engine) as s:
            closed = (
                s.query(Trade)
                .filter(
                    Trade.status == "closed",
                    Trade.closed_at >= cutoff,
                )
                .all()
            )

        if not closed:
            return {}

        # Group by strategy
        by_strategy: dict[str, list] = {}
        for t in closed:
            strat = t.strategy or "unknown"
            by_strategy.setdefault(strat, []).append(t)

        result = {}
        all_pnl_usdt_wins = []
        all_pnl_usdt_losses = []
        all_fees = 0.0
        all_trades = 0

        for strat, trades in by_strategy.items():
            wins   = [t for t in trades if (t.pnl_usdt or 0) > 0]
            losses = [t for t in trades if (t.pnl_usdt or 0) <= 0]
            total  = len(trades)

            gross_wins   = sum(t.pnl_usdt or 0 for t in wins)
            gross_losses = abs(sum(t.pnl_usdt or 0 for t in losses))
            profit_factor = round(gross_wins / gross_losses, 2) if gross_losses > 0 else float("inf")

            total_pnl   = round(sum(t.pnl_usdt or 0 for t in trades), 4)
            total_fees  = round(sum(t.fees_paid or 0 for t in trades), 4)
            net_pnl     = round(total_pnl - total_fees, 4)

            avg_win_pct  = round(sum(t.pnl_pct or 0 for t in wins)   / len(wins)   if wins   else 0, 3)
            avg_loss_pct = round(sum(t.pnl_pct or 0 for t in losses) / len(losses) if losses else 0, 3)

            # Worst single trade by pnl_usdt
            worst = min(trades, key=lambda t: t.pnl_usdt or 0)

            result[strat] = {
                "trades":          total,
                "wins":            len(wins),
                "losses":          len(losses),
                "win_rate":        round(len(wins) / total * 100, 1) if total else 0,
                "profit_factor":   profit_factor,
                "total_pnl_usdt":  total_pnl,
                "total_fees_usdt": total_fees,
                "net_pnl_usdt":    net_pnl,
                "avg_win_pct":     avg_win_pct,
                "avg_loss_pct":    avg_loss_pct,
                "top_loss_symbol": worst.symbol,
                "top_loss_usdt":   round(worst.pnl_usdt or 0, 4),
            }

            all_pnl_usdt_wins  += [t.pnl_usdt or 0 for t in wins]
            all_pnl_usdt_losses += [t.pnl_usdt or 0 for t in losses]
            all_fees   += total_fees
            all_trades += total

        # Aggregate totals
        total_gross_wins   = sum(all_pnl_usdt_wins)
        total_gross_losses = abs(sum(all_pnl_usdt_losses))
        total_pnl_all      = round(total_gross_wins - total_gross_losses, 4)

        result["_totals"] = {
            "trades":          all_trades,
            "wins":            len(all_pnl_usdt_wins),
            "losses":          len(all_pnl_usdt_losses),
            "win_rate":        round(len(all_pnl_usdt_wins) / all_trades * 100, 1) if all_trades else 0,
            "profit_factor":   round(total_gross_wins / total_gross_losses, 2) if total_gross_losses > 0 else float("inf"),
            "total_pnl_usdt":  total_pnl_all,
            "total_fees_usdt": round(all_fees, 4),
            "net_pnl_usdt":    round(total_pnl_all - all_fees, 4),
        }

        return result

    except Exception as e:
        logger.error(f"[REPORT] daily_strategy_report error: {e}")
        return {}


def format_daily_report(report: dict, balance: float, mode: str, date: str = None) -> str:
    """Format report dict as a Telegram-ready HTML string."""
    if not report:
        return "📊 <b>Daily Report</b> — no closed trades today."

    if date is None:
        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    lines = [
        f"📊 <b>Daily Report — {date}</b>",
        f"Mode: <b>{'🔴 LIVE' if mode == 'live' else '🟡 PAPER'}</b>  |  Balance: <b>{balance:.2f} USDT</b>",
        "",
    ]

    totals = report.get("_totals", {})

    for strat, stats in sorted(report.items()):
        if strat == "_totals":
            continue
        pf_str = f"{stats['profit_factor']:.2f}" if stats['profit_factor'] != float("inf") else "∞"
        lines += [
            f"<b>{strat}</b>",
            f"  Trades: {stats['trades']}  |  Win rate: {stats['win_rate']}%",
            f"  Profit factor: {pf_str}",
            f"  Net PnL: <b>{stats['net_pnl_usdt']:+.4f} USDT</b>",
            f"  Fees: {stats['total_fees_usdt']:.4f} USDT",
            f"  Top loser: {stats['top_loss_symbol']} ({stats['top_loss_usdt']:+.4f} USDT)",
            "",
        ]

    if totals:
        pf_str = f"{totals.get('profit_factor', 0):.2f}" if totals.get('profit_factor') != float("inf") else "∞"
        lines += [
            "─────────────────",
            f"<b>TOTAL</b>  {totals.get('trades', 0)} trades | WR {totals.get('win_rate', 0)}%",
            f"Net PnL: <b>{totals.get('net_pnl_usdt', 0):+.4f} USDT</b>  |  Fees: {totals.get('total_fees_usdt', 0):.4f} USDT",
        ]

    return "\n".join(lines)
