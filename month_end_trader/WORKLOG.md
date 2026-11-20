# month_end_trader work log

Branch: `claude/blissful-mccarthy-mt2shh`. Scheduled check-ins (4 hourly tasks, staggered ~15 min) continue this work.

## Hard rules for every session
- Never set `mode: live`, never place orders against a real account, never ask for or store IBKR credentials.
- Push only to `claude/blissful-mccarthy-mt2shh`. Run `pytest -q` before every commit.

## Done
- [x] Core modules: config, calendar, signal, guardrails, executor (tick state machine), state, alerts, fake broker, IBKR adapter, monitor, CLI, dry run
- [x] Dry run 2025-08..2025-11 matches paper ledger

## TODO (take the top unchecked item each check-in)
- [x] IBKR adapter unit tests with fake ib_async objects (fill_summary, status mapping incl. Inactive/Timeout/Unknown, what-if NaN -> BrokerError, FX conversion SGD->USD, stale ticker, account rows filter)
- [x] README: IBKR operational settings (bypass order precautions, Gateway restart time outside 09:00-16:30 ET, dedicated API username, US equity data for ADJUSTED_LAST); document MET_EXTRA_CLOSURES
- [x] Script `met validate-data`: compare IBKR ADJUSTED_LAST vs Yahoo adjclose ratios over 120d incl. TLT ex-dates (user runs once)
- [x] Speed up tests (reporter off by default in env fixture; plotting optional)
- [ ] Review round 2: IBKR/ops review (15 findings) FIXED; executor/state review pending -> fix its findings
- [x] Test suite (49 tests): calendar, signal vs backtest_signals.csv, sizing, guardrails, executor failure modes, restart recovery, monitor/SPRT
- [x] README + config.example.yaml + deploy (systemd, docker-compose with IB Gateway, IBC notes)
- [x] Review round 1 (3 agents: safety 18, IBKR 15, signal/monitor 12 findings) -> fixed: broker-sized exits, late-fill/timeout
      resolution, unfinished-trade scan across midnight, fresh-clock entry deadline, contract rebuilt from state, fail-closed
      guardrails (what-if NaN, monitor.json, sanity), USD conversion for non-USD base, fresh positions, request timeouts,
      IB error events (1100/10197/201/326), stale-price detection, CLI clientId+1, alerts async+dedupe, fsync+.bak state,
      daemon lock, config ranges, Yahoo adjclose required, source gap tolerance, clamped+winsorised SPRT, 5y calendar
      horizon with daily refresh, monitor NaN/partial-bar fixes. 20 regression tests in tests/test_review_fixes.py
- [x] Historical replay test (48 sampled month-ends; MET_SLOW=1 for all 291)
- [ ] Hardening: IBKR adapter mock tests (ib_async objects), reconnect during hold, clock skew check vs IB server time
- [ ] Hardening: ES+MES mix for large accounts; contract roll edge cases (Dec 31 after Dec expiry)
- [x] Weekly status alert (Mondays 08:00 ET)
- [ ] Monitoring: intraday timing indicators (60m bars)
