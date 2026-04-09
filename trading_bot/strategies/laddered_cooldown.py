"""
Laddered Cooldown (CWPE)
========================
Protects against model degradation. When trades start losing in a
streak — especially on a conviction_play (large size) — the bot
progressively throttles itself and ultimately halts.

Rules:
    * Any win  → reset consecutive_losses to 0 (and clear active cooldown)
    * 1 loss on a conviction_play trade → 4h cooldown on ALL trades
    * 2 consecutive losses (any tier)   → 12h cooldown on ALL trades
    * 3 consecutive losses (any tier)   → halted = True,
      manual_reset() required to resume

`can_trade()` is the single gate the orchestrator should consult before
opening a new position. It returns (allowed, reason).

State is instance-level. A single LadderedCooldown held by a singleton
MomentumStrategy will share state across all scan/close cycles for that
strategy's lifetime.
"""

from __future__ import annotations

import time

from loguru import logger


_CONVICTION_PLAY_COOLDOWN_SECONDS = 4 * 3600     # loss #1 on conviction_play
_DOUBLE_LOSS_COOLDOWN_SECONDS = 12 * 3600        # loss #2 (any tier)
_HALT_AFTER_N_LOSSES = 3


class LadderedCooldown:
    """Loss-streak brake with a cooldown ladder and a terminal halt."""

    def __init__(self) -> None:
        self.consecutive_losses: int = 0
        self.cooldown_until_ts: float = 0.0
        self.halted: bool = False

    # ────────────────────────────────────────────────────────────────────

    def record_trade_result(self, pnl: float, was_conviction_play: bool) -> None:
        """
        Feed a closed trade into the ladder.

        pnl > 0  →  reset the streak (and clear any active cooldown)
        pnl <= 0 →  advance the ladder, trigger cooldown / halt as per rules
        """
        if pnl > 0:
            if self.consecutive_losses > 0 or self.cooldown_until_ts > 0:
                logger.info(
                    f"[CWPE COOLDOWN] win pnl={pnl:.2f} — ladder reset "
                    f"(was losses={self.consecutive_losses})"
                )
            self.consecutive_losses = 0
            self.cooldown_until_ts = 0.0
            # A win does NOT clear a manual halt; that requires manual_reset().
            return

        # Loss path
        self.consecutive_losses += 1
        n = self.consecutive_losses

        if n >= _HALT_AFTER_N_LOSSES:
            self.halted = True
            self.cooldown_until_ts = 0.0  # halt supersedes timed cooldown
            logger.error(
                f"[CWPE HALT] {_HALT_AFTER_N_LOSSES} consecutive losses "
                f"(last pnl={pnl:.2f}) — manual_reset() required"
            )
            return

        if n == 2:
            self.cooldown_until_ts = time.time() + _DOUBLE_LOSS_COOLDOWN_SECONDS
            logger.warning(
                f"[CWPE COOLDOWN] loss#2 (pnl={pnl:.2f}) → "
                f"{_DOUBLE_LOSS_COOLDOWN_SECONDS // 3600}h cooldown on all trades"
            )
            return

        # n == 1
        if was_conviction_play:
            self.cooldown_until_ts = time.time() + _CONVICTION_PLAY_COOLDOWN_SECONDS
            logger.warning(
                f"[CWPE COOLDOWN] conviction_play loss (pnl={pnl:.2f}) → "
                f"{_CONVICTION_PLAY_COOLDOWN_SECONDS // 3600}h cooldown on all trades"
            )
        else:
            # First normal-tier loss: counter advances but no timed cooldown.
            logger.info(
                f"[CWPE COOLDOWN] loss#1 (pnl={pnl:.2f}, not conviction_play) "
                f"— counter advanced, no timed cooldown"
            )

    # ────────────────────────────────────────────────────────────────────

    def can_trade(self) -> tuple[bool, str]:
        """
        Return (allowed, reason).

        allowed=True  → "ok"
        allowed=False → "halted_after_3_losses" | "cooldown_Xmin"
        """
        if self.halted:
            return (False, f"halted_after_{_HALT_AFTER_N_LOSSES}_losses")

        now = time.time()
        if now < self.cooldown_until_ts:
            remaining_min = (self.cooldown_until_ts - now) / 60.0
            return (False, f"cooldown_{remaining_min:.0f}min")

        return (True, "ok")

    # ────────────────────────────────────────────────────────────────────

    def manual_reset(self) -> None:
        """Clear halted flag, counters, and any active cooldown."""
        was_halted = self.halted
        self.consecutive_losses = 0
        self.cooldown_until_ts = 0.0
        self.halted = False
        logger.info(f"[CWPE COOLDOWN] manual_reset (was_halted={was_halted})")

    def state(self) -> dict:
        """Snapshot for monitoring / Telegram alerts."""
        now = time.time()
        remaining = max(0.0, self.cooldown_until_ts - now)
        return {
            "consecutive_losses": self.consecutive_losses,
            "cooldown_until_ts": self.cooldown_until_ts,
            "halted": self.halted,
            "remaining_seconds": round(remaining, 1),
        }
