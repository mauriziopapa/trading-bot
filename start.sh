
#!/bin/bash
set -e

export PYTHONPATH=/app

uvicorn trading_bot.dashboard.server:app --host 0.0.0.0 --port ${PORT:-8000} &

python -m trading_bot.main
