# month_end_trader work log

Branch: `claude/blissful-mccarthy-mt2shh`. Scheduled check-ins (4 hourly tasks, staggered ~15 min) continue this work.

## Hard rules for every session
- Never set `mode: live`, never place orders against a real account, never ask for or store IBKR credentials.
- Push only to `claude/blissful-mccarthy-mt2shh`. Run `pytest -q` before every commit.

## Done
- [x] Core modules: config, calendar, signal, guardrails, executor (tick state machine), state, alerts, fake broker, IBKR adapter, monitor, CLI, dry run
- [x] Dry run 2025-08..2025-11 matches paper ledger

## TODO (take the top unchecked item each check-in)
- [x] Test suite (49 tests): calendar, signal vs backtest_signals.csv, sizing, guardrails, executor failure modes, restart recovery, monitor/SPRT
- [x] README + config.example.yaml + deploy (systemd, docker-compose with IB Gateway, IBC notes)
- [ ] Review agents: correctness, trading-safety, IBKR API usage (3 launched 07:3x UTC 20 Nov); fix findings
- [ ] Historical replay test across all 2002-2026 month-ends via FakeBroker vs paper ledger
- [ ] Hardening: IBKR adapter mock tests (ib_async objects), reconnect during hold, clock skew check vs IB server time
- [ ] Hardening: ES+MES mix for large accounts; contract roll edge cases (Dec 31 after Dec expiry)
- [ ] Monitoring: intraday timing indicators (60m bars), weekly heartbeat summary
