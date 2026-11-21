# Alpha research loop — protocol

This file is the standing instruction set for every research session (including
ones resumed by the scheduled task). Read it, then BACKLOG.md and the tail of LOG.md.

## Goal
Find signals for **US-listed instruments, entered at the open and exited at the
close (long or short)**, that survive honest out-of-sample testing after costs.
Original motivation: use how the rest of the world traded since the last US close.
Broader intraday (open→close) signals are in scope too.
**Holding periods (user OK, 2026-11-20):** for behavioral signals, overnight and multi-day holds are allowed
(close→close, close→open, k-day). Use `lib.backtest_weights` (turnover-based costs). Because a signal computed from
today's close and traded AT that close can pick up closing-print bounce, always report a **lag-1** version (signal
from close t, position from close t+1) as the realistic one; the same-close version is labelled best-case.
**Options are in scope (user, 2026-11-20).** Free data options: CBOE strategy-index histories (cdn.cboe.com/api/global/
us_indices/daily_prices/<NAME>_History.csv — PUT, BXM, CNDR, BFLY, WPUT, PPUT …; follow redirects), VIX-family indices
(^VIX, ^VIX9D, ^VIX1D 2023+, ^VVIX, ^SKEW), VIX ETPs (VIXY/SVXY 2011+, VXX 2018+; watch SVXY's 2018 leverage change),
and model-priced SPY options (Black-Scholes with VIX/VIX9D as implied vol — no skew, no real spreads: haircut premium
≥5% per side and label results 'model-priced').
**Current focus (user, 2026-11-20, revised): participant BEHAVIOR — flows and mechanical/constrained trading
(rebalancing, index & fund flows, hedging, leveraged-ETF rebalancing, buybacks, dividend reinvestment, roll/expiry
mechanics). NOT psychology/mood.** The month-end stock-bond rebalancing result (013) is the template. Lesson from 004/006/015: build signals from information known at the PRIOR CLOSE (returns,
volume, streaks), never from the opening print itself, which carries bid/ask-bounce artifacts.

## Data periods (never change these)
| Period | Dates | Use |
|---|---|---|
| TRAIN | 2000-01-01 → 2014-12-31 | Explore freely, pick directions/params |
| VALID | 2015-01-01 → 2022-12-31 | Confirm. No re-tuning after looking |
| HOLDOUT | 2023-01-01 → today | **One look per idea**, only if it passes the gate |

Note: the first study (global_open_close/) looked at 2015-2026 before this protocol
existed, so the holdout is slightly contaminated for SPY + overseas-index signals.

## Rules
1. Every variant examined goes through `lib.register()` → `results/registry.csv`.
   Don't eyeball unregistered variants. The registry count is the multiple-testing tally.
2. Information timing: features must be knowable before the 9:30 ET open. Anything
   printed at the open itself (other instruments' opening prints) is "best-case" and
   must be labelled as such. Watch timezones, holidays, DST and dividends.
3. Costs: default 0.5bp per side for liquid ETFs; ≥2bp per side for single stocks,
   more for small caps / low price. Report at least two cost levels for any candidate.
4. **Promotion gate** (`lib.passes_gate`): train t ≥ 2, valid t ≥ 2, valid Sharpe ≥ 0.5,
   valid ≥ 1bp per unit traded after costs. Also require: same sign train & valid, not
   driven by one year (check yearly P&L), and a plausible economic story.
   Gate passers get exactly one `lib.holdout_eval()` call; result logged and final.
4b. **Descriptive tables must stop at 2022-12-31** (added after the 140 slip, where an event table printed 2023+ stats
   and spent that idea's holdout). Only `lib.holdout_eval` may look at 2023+.
5. As the test count grows, be stricter: with N registered tests, a t-stat of
   ~sqrt(2·ln N) is what pure luck produces for the best one. Mention it.
6. Survivorship: the repo's old US/ folder only contains stocks listed in 2023 and its
   prices are rounded to 2dp. Prefer fresh Yahoo data; flag survivorship whenever a
   universe is chosen with hindsight.
7. Keep scripts small: one file per experiment in `experiments/NNN_name.py`, writing
   outputs to `results/NNN_*`. Re-runnable from scratch (`data/` is a cache, not committed).

## Each iteration
1. Pick the top unclaimed item in BACKLOG.md (or a better idea; add it first).
2. Write & run the experiment; register every variant.
3. Append an entry to LOG.md: hypothesis, what was tested, key numbers, verdict
   (dead / weak / promising / gate-passed / holdout result), next ideas spawned.
4. Update BACKLOG.md (mark done, add follow-ups) and FINDINGS.md if anything is
   gate-passed or holdout-tested.
5. `git add -A alpha_research && git commit && git push` to the session branch
   after every experiment — the container can disappear at any time.
6. Continue to the next item until the turn ends. **Cadence (user, 2026-11-20): four hourly scheduled tasks at :01, :16, :31, :46** keep rounds running back-to-back
   (hourly is the scheduler's minimum per task). No follow-up scheduling needed.
    **Never disable the scheduled task** (user's instruction,
   2026-11-20: "keep going, don't stop the loop"). When the backlog runs low, brainstorm new hypotheses with an
   economic story (flows, scheduled events, structural constraints), add them to BACKLOG.md, and keep testing.
   Stay honest as N grows: prefer few, pre-specified, well-motivated variants over parameter sweeps.

## 7. Forward paper-trading ledger (added 2026-11-21)
`tools/paper_ledger.py` keeps a pre-committed, timestamped out-of-sample record of the two findings in
`results/paper_trades.csv`. Each tick: run `python tools/paper_ledger.py plan` (adds/refreshes the next month-end's
EQ and BOND rows; the EQ side is final only when planned with data through the session before the last trading day)
and `python tools/paper_ledger.py settle` (fills realized bp once the exit date has data), then commit. Never edit a
settled row or a row marked final=True by hand. Git history is the timestamp proof.
