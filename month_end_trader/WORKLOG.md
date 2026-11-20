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
- [x] Review round 2: IBKR/ops (15) + executor/state (13) fixed: owned-quantity exits (no reversal on stale reads), exit backoff + CME-closed check + one manual alert, reconcile never touches non-owned positions, Feb-29 calendar crash, unfinished trades handled after today's work, stuck orders cancelled, entry stays SUBMITTED while an order works, disconnected-with-open-trade alert, sticky 'dead' SPRT, event cap, strict config types, no legacy contract guessing. 150 tests
- [x] Test suite (49 tests): calendar, signal vs backtest_signals.csv, sizing, guardrails, executor failure modes, restart recovery, monitor/SPRT
- [x] README + config.example.yaml + deploy (systemd, docker-compose with IB Gateway, IBC notes)
- [x] Review round 1 (3 agents: safety 18, IBKR 15, signal/monitor 12 findings) -> fixed: broker-sized exits, late-fill/timeout
      resolution, unfinished-trade scan across midnight, fresh-clock entry deadline, contract rebuilt from state, fail-closed
      guardrails (what-if NaN, monitor.json, sanity), USD conversion for non-USD base, fresh positions, request timeouts,
      IB error events (1100/10197/201/326), stale-price detection, CLI clientId+1, alerts async+dedupe, fsync+.bak state,
      daemon lock, config ranges, Yahoo adjclose required, source gap tolerance, clamped+winsorised SPRT, 5y calendar
      horizon with daily refresh, monitor NaN/partial-bar fixes. 20 regression tests in tests/test_review_fixes.py
- [x] Historical replay test (48 sampled month-ends; MET_SLOW=1 for all 291)
- [x] Hardening: IBKR adapter mock tests, reconnect during hold, clock skew check vs IB server time
- [x] Contract roll tests (Feb/Mar/Jun/Sep/Dec). ES+MES mix: not needed (40 MES ~ $1.5M notional covers the account); documented
- [x] Weekly status alert (Mondays 08:00 ET)
- [x] Monitoring: intraday timing indicators (60m bars; first hour / closing bar share; warns if closing share collapses)
- [x] Review round 3 (whole package, 13 findings + README inaccuracies) fixed: adoption of late/unreported fills in the bot's
      own contract (same direction, <= order size) at reconcile, no 'external close' inference while holding, ENTRY_SUBMITTED
      resolved on later days, daemon starts with IB down and alerts (any day >10 min; T-1 evening), CLI flatten/test-order
      refuse while daemon runs, preflight retries transient blockers to 09:27, require_all_sources option, CME half-day /
      holiday halts, 45s guard quiet window, monitor refreshed if stale, config validation, README corrected. 159 tests
- [x] Review round 4 (adoption, 11 findings) fixed: adopt only after uncertain outcomes (never after Rejected), only until the
      next session opens, at most twice; late entry fills adopted intraday so guards + scheduled exit apply; adoption price
      never post-close (P&L flagged ESTIMATED); undecided adoption defers instead of closing the trade; reported fills count;
      preflight blocked by unfinished older trades; no false criticals during preflight retries; blocker prefixes; EXIT_SUBMITTED
      finalised on reconcile; CME full closures (25 Dec, 1 Jan, Good Friday); half-day reconcile at close+2m; announce/signal
      apply the same source rules; FakeBroker rejects orders while CME is closed. 168 tests
- [x] Review round 5 (adversarial, 8 reproduced breaks) -> REDESIGN: ownership from IBKR executions (orderRef MET-<date>-*,
      dedup by execId incl. corrections), persisted fills; exits only when executions AND position agree; exact P&L from
      fills; adoption heuristics removed; evening execution watch until midnight; late/dead-then-filled orders and
      reversals closed; exit rounds capped (10, then hourly); 30-min alert dedupe; no orders while executions unreadable. 176 tests
- [x] Review round 6 (execution semantics, 11 findings) fixed: `met resolve` + MET-<date>-manual ref for manual closes,
      RESOLVE NEEDED alert, expired-executions detection with accurate alerts (no guessing), Gateway timezone documented +
      missing-execution warning, CLI flatten/test-order/resolve use the daemon's clientId (lock-protected), fills merged
      straight from order results, execId corrections by suffix, two agreeing reads before an exit when sources differ,
      30s post-reconnect grace, strict BOT/SLD, FA accounts refused; FakeBroker executions expire at midnight ET. 181 tests
- [x] Paper-test checklist for the user (PAPER_TEST.md) (exact commands for 30 Nov: check, validate-data, test-order, verify reqExecutions after a Gateway restart, MET-manual ref test)
- [x] End-to-end 'paper rehearsal' script (met rehearse): replays 27-30 Nov 2026 with a synthetic path and prints the exact alert timeline
- [ ] Sizing: floor rounding gives 1.78x on ~$120k (6 MES); consider nearest-contract rounding capped by max_leverage (ask user before changing behaviour)
