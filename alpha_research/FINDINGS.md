# Findings (gate-passed and holdout-tested signals only)

| # | Signal | Valid (2015-22) | Holdout (2023+) | Status |
|---|---|---|---|---|
| 001 | Short QQQ open→close on monthly OPEX Friday (3rd Fri) | t=+2.22, Sharpe 0.78, +26bp/trade | t=+0.83, Sharpe 0.42, +12.6bp/trade (44 trades) | Weak-but-alive; tiny capacity in time (12 trades/yr) |
| 004 | Cross-sectional ADR catch-up vs home share (price ≥ $20, long 2 / short 2, open→close) | t=+4.9, Sharpe 1.73, +4.7bp/day @3bp | t=+2.56, Sharpe 1.30 @3bp; ~0 @5bp. Edge sits in the first hour (open→10:30) | **Downgraded (015):** edge vanishes if entry is 5 min after open → likely opening-print artifact |
| 006 | Large-cap overnight-gap reversal (decile L/S vs SPY gap, price ≥ $20) | @1bp: t=+2.10, Sharpe 0.74 | @1bp: t=+1.0, Sharpe 0.51; @2bp: 0. Edge all in open→10:30 | **Downgraded (015):** gone by 9:35-9:40 entry → likely opening-print artifact |
| **013** | **Month-end rebalancing: on the last trading day, SPY open→close = −sign(SPY MTD − TLT MTD)** | t=+2.01, Sharpe 0.71, +17bp/trade | **t=+3.30, Sharpe 1.68, +37.5bp/trade, 67% hit (46 trades); @2bp t=+3.09** | **Survived holdout.** Dose-response in |gap|. 12 trades/yr |
| 013c | Same month-end rule on IWM / QQQ / DIA (1bp/side) | t=+2.38 / +2.07 / +2.03 | t=+2.77 / +3.55 / +2.39; +38 / +48 / +26bp/trade | Confirms 013 across equity ETFs; last hour 81% hit (MOC flows) |
| 040 | 013 rule back-cast to 1993-2002 (TYX bond proxy, never-seen data) | — | t=+0.60, Sharpe 0.20, +6.6bp/trade (114 trades) | **Caveat:** weak before 2002 → era-dependent flow effect |
| **083** | **Month-end signal via options: sell a same-day ATM SPX/SPY option against the signal (put if long, call if short)** — model-priced, calibrated | t=+2.71, +11bp/trade | **t=+3.53, Sharpe 1.80, +15.7bp/trade, 80% hit, worst −78bp (46 trades)** | Survived holdout (model-priced; 087 shows the model overstates seller edge ~35% → treat as upper bound, ~+10bp/trade realistic) |
| 013g | Month-end rebalancing as a SPY−TLT pair (both sides of the flow) | t=+2.17, +25bp/trade | t=+2.67, Sharpe 1.36, +39bp/trade, 76% hit | Survived; similar to SPY-only with higher hit rate |
| 168 | 013 only when SPY 20d vol > train median (≈6 trades/yr) | t=+2.53, Sharpe 0.90, +44bp/trade | t=+2.90, Sharpe 1.47, +61.5bp/trade, 78% hit (18 trades) | Survived; edge scales with volatility → size up on volatile month-ends |
| 013h | Permutation tests of 013 (2002-22) | sign-shuffle p<0.0001; same rule on random non-month-end days: +0.8bp (p≈0.0005) | — | Confirms it's a month-END effect, not a generic stock-bond reversal |
| 257 | Month-end rule replicated in Canada / UK / Australia / euro area (local equity vs bond ETFs) | all 4 positive in 2015-22, pooled t=+3.35; Canada t=+2.42 | Canada (only gate-passer): t=−0.19, +1.9bp gross | International in 2015-22; Canada failed 2023+ → caution on persistence |
| 275 | Long TLT over the last 3 sessions of each month (bond index extension / month-end bond buying) | t=+2.32, Sharpe 0.82; 21/21 positive years 2002-22 | t=+0.60, Sharpe 0.31; +2.2/+4.8/+3.8/−5.3% by year | Weak-but-alive; long-running, smaller recently |
| 276 | Same month-end Treasury bid on IEF and SHY | IEF t=+3.24, SHY t=+3.21 (Sharpe ~1.1) | IEF (276b, 2nd family look): Sharpe 0.83, t=+1.63, +1.9%/yr, 3/4 years up | Effect across the curve; IEF weak-but-alive in 2023+ |
| 277/278 | Month-end bond bid in LQD, HYG, Canada, UK, euro govt bonds (consistency) | right sign in all 10 train/valid pairs; LQD +2.67, XBB +2.64 valid t | (not looked) | Broad month-end bond-index-extension effect |
| 280 | Month-end bond bid in MBB / TIP / MUB (consistency) | MBB t=+3.23, TIP t=+2.91, MUB t=+0.98 | (not looked) | Present in index-tracked taxable bonds, weak in munis → consistent with index extension |
| 285 | Controls: same last-3-session rule on GLD / SPY / UUP | t ≤ +0.2 in train, negative in valid | — | Bid is bond-specific → supports index-extension mechanism |
| 276e | Permutation test of the bond bid (2002-22) | IEF +23bp vs random-window +2.7bp; TLT +40 vs +2.6bp; p<0.0002 | — | Month-end window is extreme vs any other 3-day window |

- 2026-11-20 (309, clock-corrected 2026-11-21): 013 passes the delayed-entry test that killed 004/006 — entering at
  09:35/09:45/10:00 keeps +8.9/+10.1/+10.1bp of the +10.4bp from 09:30 (SPX CFD 2010-22, t 1.4-1.6; corrected clock); same on NSX.

- 2026-11-20 (315): the month-end bond bid replicates on never-used FRED yield data back to 1962: absent 1962-82 (before
  bond indexing), present at ~2-3bp of yield decline per month-end in 1983-92, 1993-2001 and 2002-22 (pre-2002 permutation
  p = 0.006-0.03 across 2y/10y/30y). Timeline matches the index-extension mechanism.
- 2026-11-21 (318): control — 3-month bills (outside the >1y bond indices) show no meaningful month-end bid (pre-2002 p=0.44)
  while 2y notes do (p=0.006); the effect follows index eligibility.
