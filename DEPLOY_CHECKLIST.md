# Deploy Checklist — momentum-only paper mode

## Pre-deploy (manual, reviewer responsibility)

- [ ] DB backup from Railway Postgres (pg_dump or dashboard backup)
- [ ] Verify `TRADING_MODE=paper` in Railway env vars
- [ ] Verify `ENABLE_MOMENTUM=true`, all other `ENABLE_*=false` in bot_config table
- [ ] Run `scripts/migrate_attribution.py` on production DB (idempotent, safe)
- [ ] Run `scripts/seed_momentum_config.py` on production DB (idempotent, safe)
- [ ] Verify Telegram credentials still working (test message)

## Deploy

- [ ] Merge PR refactor/momentum-only-attribution → main
- [ ] Push to main triggers Railway deploy
- [ ] Monitor Railway logs for 10 minutes

## Post-deploy verification (first 10 minutes)

- [ ] Log shows: `[MOMENTUM strategy active, others disabled]`
- [ ] Log shows: `mode=paper`
- [ ] `risk_manager.get_block_reason()` returns specific sub-cause (not generic "global_stop")
- [ ] If global_stop is active, execute manual unlock via Railway shell:
```
      python -c "from trading_bot.utils.risk_manager import RiskManager; \
                 from trading_bot.models.database import DB; \
                 rm = RiskManager(DB()); \
                 rm.manual_unlock(reason='post-refactor paper validation start')"
```
- [ ] Telegram receives "bot started" message
- [ ] First scan cycle completes without exception
- [ ] `trades_attribution` query works (even if empty):
      `SELECT strategy, COUNT(*) FROM trades WHERE opened_at > NOW() - INTERVAL '1 hour' GROUP BY strategy;`

## Validation gate (48-72 hours in paper mode)

Do NOT switch to live trading until:

- [ ] Minimum 20 paper trades executed
- [ ] Win rate >= 45%
- [ ] Profit factor >= 1.3
- [ ] Fee/PnL ratio <= 30% (vs 72% in old bot)
- [ ] Max drawdown <= 10%
- [ ] Daily report Telegram received successfully
- [ ] Zero unexpected exceptions in Railway logs
- [ ] No [MOMENTUM REJECT] storm (< 95% rejection rate = strategy is too tight)

## If validation gates pass

- [ ] Switch Railway env var `TRADING_MODE=live`
- [ ] Start with reduced capital (e.g. 20 USDT, keep 50% buffer)
- [ ] Monitor first 10 live trades closely
- [ ] Run daily report review for first week

## If validation gates fail

- [ ] Keep in paper mode
- [ ] Analyze `trades_attribution` + `signal_snapshot` data for failing trades
- [ ] Adjust parameters (MOMENTUM_MIN_SCORE, ATR bounds, etc) via bot_config DB
- [ ] Restart validation gate count from 0
