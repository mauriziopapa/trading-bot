#!/bin/bash
set -e

export PYTHONPATH=/app

# FIX #5: uvicorn era avviato in background senza supervisione.
# Con 'wait -n' se uno dei due processi muore, il wrapper esce
# e Railway vede exit != 0 → restart automatico.

# Avvia dashboard uvicorn in background
uvicorn trading_bot.dashboard.server:app \
    --host 0.0.0.0 \
    --port "${PORT:-8000}" \
    --log-level warning &
UVICORN_PID=$!

# Avvia il bot principale in background
python -m trading_bot.main &
BOT_PID=$!

echo "PIDs: uvicorn=$UVICORN_PID bot=$BOT_PID"

# Cleanup su SIGTERM/SIGINT (Railway invia SIGTERM per stop)
cleanup() {
    echo "Signal ricevuto — arresto processi..."
    kill "$UVICORN_PID" "$BOT_PID" 2>/dev/null || true
    wait "$UVICORN_PID" "$BOT_PID" 2>/dev/null || true
    exit 0
}
trap cleanup SIGTERM SIGINT

# Aspetta che UNO dei due processi termini
# Se termina, Railway fa restart dell'intero servizio
wait -n "$UVICORN_PID" "$BOT_PID"
EXIT_CODE=$?

echo "Processo terminato (exit=$EXIT_CODE) — arresto entrambi..."
kill "$UVICORN_PID" "$BOT_PID" 2>/dev/null || true
exit "$EXIT_CODE"
