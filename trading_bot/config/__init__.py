# trading_bot/config/__init__.py
#
# Re-esporta il singleton 'settings' in modo che:
#
#   from trading_bot.config import settings
#
# restituisca l'OGGETTO DynamicSettings (non il modulo settings.py).
#
# Tutti i moduli del bot usano questo import — nessuno va toccato.

from trading_bot.config.settings import settings   # noqa: F401

__all__ = ["settings"]
