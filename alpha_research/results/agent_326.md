# 326 — Next-morning reversal after the biggest days (SPY + QQQ)

**What.** Event at close t: |close->close| above the 95th pct of the prior 252 sessions' |cc| (top-5% day), per ticker.
Fade it (position = -sign of the day's move), 50/50 SPY+QQQ book, 0.5bp/side. Mechanism: LETF rebalancing and
short-gamma dealer hedging push the close too far on big days. Script: `experiments/326_big_day_reversal.py`;
registry: `results/registry_agent_326.csv` (4 rows). Nothing from 2023+ printed; holdout untouched.

**Variants (pre-specified)** and results (train 2000-14 / valid 2015-22). Events: 427 / 271 ticker-days.

| Variant | train t | valid t | valid Sharpe | valid bp/unit | timing t (tr / va) | gate |
|---|---|---|---|---|---|---|
| A fade close->next open (BEST-CASE: MOC entry on the signal close) | -0.92 | +1.54 | +0.55 | +23.6 | -0.97 / +1.53 | no |
| B fade next-day open->close (realistic) | +1.33 | +0.63 | +0.22 | +7.8 | +1.34 / +0.62 | no |
| C fade close->close, lag 1 (realistic) | +0.31 | -0.58 | -0.21 | -12.8 | +0.27 / -0.62 | no |
| D fade close->close, lag 0 (BEST-CASE reference) | +0.91 | +1.66 | +0.59 | +32.9 | +0.86 / +1.63 | no |

**Notes.** Valid strength of A and D is mostly 2020 (A: 2020 = +2526 of +3197bp summed 2015-22, ~79%;
D: 2020 = +4373 of +4403bp, ~99%). A has the wrong sign in train (2008: -1067bp), i.e. big-day overnight
reversal is not stable pre-2009 — consistent with 112 (valid-only). The realistic versions (B, C) are not significant
in either period. With ~450 registered tests in the main registry, luck-level t is ~sqrt(2 ln 450) = 3.5.

**Verdict: dead** (weak, regime/2020-driven hint in the best-case overnight leg only; no variant passes `lib.passes_gate`).
