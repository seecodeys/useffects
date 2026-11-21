# 360 — Pension funded-status glide paths → month-ahead long-end flattener (idea card P7, results/ideas_papers.md)

Script: `experiments/360_pension_ldi_flattener.py`. Events: `results/360_events.csv` (month-ends through 2022 only).
Registry: `results/registry_agent_360.csv` (8 rows = 4 variants × {raw, timing value}). No overlap with 013/250
(1-day month-/quarter-end rebalancing), 256 (last-day TLT−IEF on the MTD gap) or 275-319 (the turn-of-month bond bid).

**Setup.** Decision at the last trading day of month t. FS is the trailing 3-month SPY total return minus the TLT total
return (equity-heavy assets, long-duration liabilities). Position held over month t+1 using `lib.backtest_weights(lag=1)`.
Flattener = TLT − 1.9×IEF. Costs 1bp/side on turnover. Tercile threshold is the TRAIN-only 67th percentile
(FS > 6.5%). TLT/IEF start 2002-07, so train has 147 month-ends and valid has 95. B is a synthetic DV01-neutral 20s−10s
flattener from FRED DGS20/DGS10 (1994+). It uses yields only through t−1, to respect the H.15 lag, and ^GSPC price
returns. Timing value = net − mean signed position × the always-on flattener.

| Variant (pre-specified) | train t | valid t | valid Sharpe | valid net bp/trade-day | TV train t / valid t | gate |
|---|---|---|---|---|---|---|
| A top-tercile FS → long TLT−1.9×IEF | −0.32 | −0.59 | −0.21 | −0.8 | +0.84 / −0.45 | no |
| B FRED synthetic 20s−10s, top tercile | +1.97 | +0.11 | +0.04 | +0.1 | +2.10 / +0.08 | no |
| C A, holding months Oct–Mar only | +0.06 | −0.65 | −0.23 | −1.2 | +0.84 / −0.53 | no |
| D continuous z(FS) (clip ±2) | +0.44 | +0.35 | +0.12 | +0.7 | +0.07 / +0.11 | no |

**Descriptive (≤2022).** corr(FS, next-month TLT−1.9×IEF return) is +0.03 in both train and valid. The next-month
flattener return by FS tercile is not monotone in either period (train low/mid/high +2/−54/−8bp; valid −27/+15/−17bp).
On the FRED proxy, corr(FS, next-month 20s−10s flattening) is +0.37 for 1994-99, +0.19 for 2000-14 and +0.10 for
2015-22. A top-tercile FS month is followed by +47bp, +18bp and +1bp of flattener P&L in those three periods. The
effect, if it ever existed, faded after the 1990s and is absent in valid. Yearly P&L swings both ways in every variant.

**Verdict: dead.** No variant passes `lib.passes_gate`. The only near-miss is B in train, where the timing-value
t is 2.10. It is roughly 0 in valid, and train t≈2 is well below the luck level for N≈450 (≈3.5). No holdout look was taken.
