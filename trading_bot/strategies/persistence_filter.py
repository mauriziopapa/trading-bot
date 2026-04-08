"""
Momentum Persistence Filter (CWPE)
===================================
Requires a symbol's signal to survive `required_cycles` consecutive scan
cycles in the same direction, with each consecutive pair of observations
no more than `max_gap_seconds` apart. Filters one-cycle flukes where a
single noisy bar briefly flips EMA/MACD alignment before reverting.

Usage (future integration):
    self._persistence = MomentumPersistenceFilter(required_cycles=2, max_gap_seconds=180)
    ...
    self._persistence.record_signal(symbol, direction, scanner_score)
    if not self._persistence.is_persistent(symbol, direction):
        return None  # single-cycle fluke — skip entry

    # Periodic housekeeping
    self._persistence.cleanup_stale()

State is per-symbol, keyed by the exchange symbol string. A single
instance held as an attribute of a singleton MomentumStrategy will
share state across all scan cycles for that strategy's lifetime.
"""

from __future__ import annotations

import time
from collections import defaultdict, deque
from typing import Deque, Optional, Tuple

from loguru import logger


# Symbol is considered "stale" and removed entirely if we have not seen
# any signal for it within this window.
_STALE_SYMBOL_SECONDS = 10 * 60  # 10 minutes


class MomentumPersistenceFilter:
    """Rolling per-symbol signal history with direction-consistency check."""

    def __init__(self, required_cycles: int = 2, max_gap_seconds: int = 180):
        if required_cycles < 1:
            raise ValueError("required_cycles must be >= 1")
        if max_gap_seconds <= 0:
            raise ValueError("max_gap_seconds must be > 0")

        self.required_cycles: int = required_cycles
        self.max_gap_seconds: int = max_gap_seconds

        # Each entry: (timestamp, direction, scanner_score)
        # Keep a bit more than required_cycles so cleanup-on-write is cheap.
        self._history: defaultdict[str, Deque[Tuple[float, str, float]]] = defaultdict(
            lambda: deque(maxlen=max(4, self.required_cycles * 2))
        )

    # ────────────────────────────────────────────────────────────────────

    def record_signal(self, symbol: str, direction: str, score: float) -> None:
        """Append a single signal observation for `symbol`."""
        if not symbol or not direction:
            return
        self._history[symbol].append((time.time(), direction, float(score)))

    def is_persistent(self, symbol: str, direction: str) -> bool:
        """
        Return True iff the last `required_cycles` recorded signals for
        `symbol` all match `direction` AND each consecutive pair of
        observations is within `max_gap_seconds` of each other.
        """
        if not symbol or not direction:
            return False

        hist = self._history.get(symbol)
        if hist is None or len(hist) < self.required_cycles:
            return False

        # Take the most recent required_cycles entries
        recent = list(hist)[-self.required_cycles:]

        # Direction consistency across the full window
        for _, d, _ in recent:
            if d != direction:
                logger.debug(
                    f"[CWPE PERSIST REJECT] {symbol} direction flip within window"
                )
                return False

        # Max gap between consecutive entries
        for i in range(1, len(recent)):
            gap = recent[i][0] - recent[i - 1][0]
            if gap > self.max_gap_seconds:
                logger.debug(
                    f"[CWPE PERSIST REJECT] {symbol} gap={gap:.0f}s > "
                    f"max_gap={self.max_gap_seconds}s"
                )
                return False

        logger.debug(
            f"[CWPE PERSIST] {symbol} {direction} confirmed across "
            f"{self.required_cycles} cycles"
        )
        return True

    def cleanup_stale(self, now_ts: Optional[float] = None) -> int:
        """
        Remove symbols whose most recent signal is older than
        _STALE_SYMBOL_SECONDS (default 10 minutes). Returns # of symbols
        removed.
        """
        now = now_ts if now_ts is not None else time.time()
        cutoff = now - _STALE_SYMBOL_SECONDS
        removed = 0

        for symbol in list(self._history.keys()):
            q = self._history[symbol]
            if not q or q[-1][0] < cutoff:
                del self._history[symbol]
                removed += 1

        if removed:
            logger.debug(f"[CWPE PERSIST] cleanup_stale removed symbols={removed}")
        return removed

    def reset(self, symbol: Optional[str] = None) -> None:
        """Clear history for one symbol, or all symbols if `symbol` is None."""
        if symbol is None:
            self._history.clear()
            logger.info("[CWPE PERSIST] reset all")
        else:
            self._history.pop(symbol, None)
            logger.info(f"[CWPE PERSIST] reset {symbol}")
