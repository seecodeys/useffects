# month_end_trader work log

Branch: `claude/sweet-goldberg-pvjfgq` (merged from `claude/blissful-mccarthy-mt2shh`). Scheduled check-ins
(4 hourly tasks at :07/:22/:37/:52 UTC) continue this work.

Strategy now = SPY core + leg A (013 month-end MES, vol-tilted per research 168) + leg B (research 276 month-end
Treasury bid, long ZN over the last 3 sessions). Backtest with IBKR costs: backtest/summary.md.

## Hard rules for every session
- Never set `mode: live`, never place orders against a real account, never ask for or store IBKR credentials.
- Push only to `claude/sweet-goldberg-pvjfgq`. Run `pytest -q` before every commit.

## Done
- [x] Core modules: config, calendar, signal, guardrails, executor (tick state machine), state, alerts, fake broker, IBKR adapter, monitor, CLI, dry run
- [x] Dry run 2025-08..2025-11 matches paper ledger

## Leg B / vol tilt (2026-11-21)
- [x] Executor parametrized per leg (REF prefix, instrument, state prefix, entry date); no behaviour change for A
- [x] Vol tilt for leg A (strategy.vol_tilt, fixed threshold 0.008369 daily = 2002-14 median, low_vol_scale 0.5;
      sources disagreeing or vol unavailable -> low size). tests/test_vol_tilt.py
- [x] Leg B: met/bond.py (BondExecutor + bond_preflight + first-notice-safe contract choice), met/bond_monitor.py
      (IEF paper ledger + SPRT -> bond_monitor.json), met/daemon.py (one daemon, both legs), FakeBroker ZN support,
      CLI run/status/flatten/bond-check/bond-report, config `bond:` section. tests/test_bond.py (28)
- [x] Full IBKR-cost backtest (backtest/: trades.csv, yearly.csv, equity.png, summary.md)

## TODO (take the top unchecked item each check-in)
- [ ] Review round B1 (agents): bond leg correctness (ownership across days, exits, half days, holidays, CBOT hours
      vs futures_open, first notice), and daemon interplay of legs A and B on the month-end day
- [ ] Review round B2 (agents): IBKR adapter for ZN (CBOT contract details: lastTradeDateOrContractMonth semantics,
      multiplier '1000', price increments 1/64 & 1/128, what-if margin with an open ZN position, market data permission)
- [ ] Dry run + rehearsal with both legs (met dry-run / met rehearse) incl. ledger comparison for leg B (IEF paper)
- [ ] Historical replay of leg B over all month-ends 2003-2026 on the FakeBroker (ZN path from IEF), compare to ledger
- [ ] Weekly status / announce: include leg B (next window, position, bond monitor status)
- [ ] CBOT holiday/early-close calendar for Treasury futures (SIFMA early closes 14:00 ET; Good Friday) vs futures_open
- [ ] README + PAPER_TEST.md: leg B section (ZN permissions, CBOT data, first paper window 24 -> 30 Nov 2026)
- [ ] Combined-exposure guardrail: refuse if MES + ZN notional / net liq exceeds a configured ceiling
- [ ] validate-data: add IEF; bond-check prints contract choice + first notice day
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
- [x] Sizing: opt-in `strategy.sizing_rounding: nearest` (closest contract count, still capped by max_leverage/max_contracts);
      default stays `floor` (6 MES, 1.78x on ~$120k) until the user chooses. 192 tests
- [x] Test for `met resolve` CLI
- [x] Review round 7 (12 findings) fixed: reversal/after-exit late fills close immediately, CLI flatten/resolve sync first and
      resolve requires a price, alert texts for fills-vs-broker disagreement (never 'close it in TWS' when broker is flat),
      two-read check keeps a good second read (exit stays before 16:00), exit vwap from closing-side fills only + reversal
      flag (trade_bp None), guard re-reads before mismatch alerts, exit counters reset on re-entry, entry status persisted,
      user '-manual' orders not treated as bot orders, executions filtered by contract, PAPER_TEST/systemd notes. 187 tests
- [x] Review round 8 (5 findings, no P&L/ownership bugs) fixed: disagreement flag resets when sources agree, bot waits
      while the user's MET-<date>-manual order is working (no double close), Unknown/Timeout entries wait for executions,
      guard re-read only for new mismatches, mismatch flag clears. 189 tests
- [x] Full replay of all 291 TLT-era month-ends (MET_SLOW=1) after round 8: 291/291 match the paper ledger
- [x] Review round 9 (monitor/CLI/deploy, 6 findings) fixed: Telegram delivery checked + retries + dedupe released on
      failure + `met alert-test`, provisional signal/check before T-1's close, atomic monitor.json + robust `status`,
      explicit edge_dead flag + half-size recovery, kill/unkill permission errors explained (docker exec), intraday shares
      only for positive totals, PAPER_TEST updated. 192 tests
- [ ] Waiting on user: set sizing_rounding (floor vs nearest); whether to pause the 15-min check-ins until after the 30 Nov paper run
