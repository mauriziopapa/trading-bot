"""
Telegram Notifier
Invia alert per trade aperti/chiusi, errori critici e report giornaliero.
"""

import asyncio
from datetime import datetime, timezone
from loguru import logger

try:
    from telegram import Bot
    from telegram.error import TelegramError
    TELEGRAM_AVAILABLE = True
except ImportError:
    TELEGRAM_AVAILABLE = False

from trading_bot.config import settings


class TelegramNotifier:

    def __init__(self):
        self.enabled = (
            TELEGRAM_AVAILABLE
            and bool(settings.TELEGRAM_TOKEN)
            and bool(settings.TELEGRAM_CHAT_ID)
        )
        self.bot = Bot(token=settings.TELEGRAM_TOKEN) if self.enabled else None

        if self.enabled:
            logger.info("Telegram notifier attivo")
        else:
            logger.warning("Telegram notifier disabilitato (token/chat_id mancanti)")

    def send(self, text: str):
        if not self.enabled:
            return
        try:
            asyncio.run(self.bot.send_message(
                chat_id=settings.TELEGRAM_CHAT_ID,
                text=text,
                parse_mode="HTML"
            ))
        except Exception as e:
            logger.warning(f"Telegram send error: {e}")

    # ─── Trade Messages ──────────────────────────────────────────────────────

    def trade_opened(self, symbol: str, side: str, size: float, entry: float,
                     stop_loss: float, take_profit: float, market: str,
                     strategy: str, confidence: float):
        if not settings.NOTIFY_TRADES:
            return
        emoji = "🟢" if side == "buy" else "🔴"
        mode  = "[PAPER]" if not settings.IS_LIVE else ""
        self.send(
            f"{emoji} <b>TRADE APERTO {mode}</b>\n"
            f"📌 {symbol} | {market.upper()}\n"
            f"📊 Strategia: {strategy} ({confidence:.0f}%)\n"
            f"{'Direzione':10}: {'LONG' if side=='buy' else 'SHORT'}\n"
            f"{'Entry':10}: {entry:.4f}\n"
            f"{'Size':10}: {size:.6f}\n"
            f"{'Stop Loss':10}: {stop_loss:.4f}\n"
            f"{'Take Profit':10}: {take_profit:.4f}\n"
            f"⏰ {_now()}"
        )

    def trade_closed(self, symbol: str, side: str, entry: float, exit_price: float,
                     pnl_pct: float, pnl_usdt: float, reason: str, market: str):
        if not settings.NOTIFY_TRADES:
            return
        emoji = "✅" if pnl_pct > 0 else "❌"
        self.send(
            f"{emoji} <b>TRADE CHIUSO</b>\n"
            f"📌 {symbol} | {market.upper()}\n"
            f"{'Motivo':10}: {reason.upper()}\n"
            f"{'Entry':10}: {entry:.4f}\n"
            f"{'Exit':10}: {exit_price:.4f}\n"
            f"{'PnL':10}: {pnl_pct:+.2f}% ({pnl_usdt:+.2f} USDT)\n"
            f"⏰ {_now()}"
        )

    def error(self, message: str):
        if not settings.NOTIFY_ERRORS:
            return
        self.send(f"⚠️ <b>ERRORE BOT</b>\n{message}\n⏰ {_now()}")

    def daily_report(self, stats: dict, balance_spot: float, balance_futures: float):
        if not settings.NOTIFY_DAILY_REPORT:
            return
        wr = stats.get("win_rate", 0)
        self.send(
            f"📈 <b>REPORT GIORNALIERO</b>\n"
            f"{'Balance Spot':16}: {balance_spot:.2f} USDT\n"
            f"{'Balance Futures':16}: {balance_futures:.2f} USDT\n"
            f"{'PnL Giorno':16}: {stats.get('daily_pnl', 0):+.2f}%\n"
            f"{'Trade Oggi':16}: {stats.get('daily_trades', 0)}\n"
            f"{'Win Rate':16}: {wr:.1f}%\n"
            f"{'Avg Win':16}: +{stats.get('avg_win_pct', 0):.2f}%\n"
            f"{'Avg Loss':16}: -{stats.get('avg_loss_pct', 0):.2f}%\n"
            f"⏰ {_now()}"
        )

    def circuit_breaker(self, reason: str):
        self.send(f"🚨 <b>CIRCUIT BREAKER ATTIVATO</b>\n{reason}\nBot in pausa fino a reset giornaliero.")

    def startup(self, mode: str, symbols_spot: list, symbols_futures: list):
        self.send(
            f"🚀 <b>BOT AVVIATO</b>\n"
            f"Modalita: <b>{'🔴 LIVE' if mode=='live' else '🟡 PAPER'}</b>\n"
            f"Spot:    {', '.join(symbols_spot)}\n"
            f"Futures: {', '.join(symbols_futures)}\n"
            f"⏰ {_now()}"
        )


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
