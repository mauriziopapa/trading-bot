#!/bin/bash
set -e

export PYTHONPATH=/app

# v4: Il bot avvia uvicorn internamente (stesso processo).
# Così _bot_ref è condiviso e /api/sync, /api/rebalance funzionano.
# Non serve più lanciare uvicorn separatamente.

echo "Starting Trading Bot v4 (dashboard integrata)..."
exec python -m trading_bot.main
