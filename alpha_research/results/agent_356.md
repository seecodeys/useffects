# 356 — Country-ETF fade after home-market holiday closures (idea card 8, results/ideas_options_etf.md)

Script: `experiments/356_holiday_closure_etf.py`. Events: `results/356_events.csv` (through 2022 only). Registry:
`results/registry_agent_356.csv` (4 rows). No overlap with LOG 028, which tested only Japan's fiscal year-end; its
China/HK closure note was never run.

**Setup.** Pairs: FXI and EWH map to ^HSI, ASHR (2014+) to 000001.SS, EWT to ^TWII, EWY to ^KS11, EWJ to ^N225 and
EWZ to ^BVSP. A closure day is a US trading day with no home-index bar on Yahoo. A closure window is a run of 2 to 8
such days. X is the ETF's cumulative return minus β times SPY's, with β from a trailing 250-day estimate ending
before the window. The trade is −sign(X)·(ETF − β·SPY), held from the US close of the last closure day to the next
US close, by which time the home market has reopened. The reopen date is assumed known in advance from the published
exchange holiday calendar. Events on the same day get equal weight. Costs are 2bp/side on the ETF and 0.5bp/side on
SPY. The 229 closure windows from 2002-22 break down as HSI 58, TWII 48, N225 47, KS11 39, SSE 29 and BVSP 17.

| Variant (pre-specified) | events train/valid | train t | valid t | valid Sharpe | valid net bp/trade | gate |
|---|---|---|---|---|---|---|
| A lag-1 (X to the penultimate closure close), \|X\|>1% | 36 / 33 | +0.98 | +0.42 | +0.15 | +14.3 | no |
| B A, Lunar New Year only | 14 / 16 | +0.11 | −0.08 | −0.03 | −4.4 | no |
| C A, \|X\|>2% | 10 / 7 | +1.15 | −0.80 | −0.28 | −64.3 | no |
| D best case, same close (X to the last closure close) | 41 / 37 | +0.69 | +1.57 | +0.55 | +64.7 | no |

Descriptive check, no thresholds, all events: the slope of the reopen-day hedged return on X. For X_lag0 it is
−0.30 in train (n=110, corr −0.21) and −0.13 in valid (n=119, corr −0.09). For X_lag1 it is −0.30 in train and
+0.12 in valid. Some of the move gives back, but the effect is small and noisy, and the lag-1 version flips sign in
valid. Yearly P&L swings both ways with only 1-9 events a year. With N≈4 here (and about 380 or more registered
across the project), luck alone gives t≈2.6 or more.

**Verdict: dead.** No variant passes `lib.passes_gate`. The best-case same-close variant D is right-signed in both
periods but falls short of the gate (valid t 1.57, train t 0.69). With about 35 tradable events per period the test
has little power, and the 2bp ETF costs are small next to the 1-2% per-event noise. No holdout look was taken.
