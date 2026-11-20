# Findings (gate-passed and holdout-tested signals only)

| # | Signal | Valid (2015-22) | Holdout (2023+) | Status |
|---|---|---|---|---|
| 001 | Short QQQ open→close on monthly OPEX Friday (3rd Fri) | t=+2.22, Sharpe 0.78, +26bp/trade | t=+0.83, Sharpe 0.42, +12.6bp/trade (44 trades) | Weak-but-alive; tiny capacity in time (12 trades/yr) |
| 004 | Cross-sectional ADR catch-up vs home share (price ≥ $20, long 2 / short 2, open→close) | t=+4.9, Sharpe 1.73, +4.7bp/day @3bp | t=+2.56, Sharpe 1.30 @3bp; ~0 @5bp. Edge sits in the first hour (open→10:30) | **Downgraded (015):** edge vanishes if entry is 5 min after open → likely opening-print artifact |
| 006 | Large-cap overnight-gap reversal (decile L/S vs SPY gap, price ≥ $20) | @1bp: t=+2.10, Sharpe 0.74 | @1bp: t=+1.0, Sharpe 0.51; @2bp: 0. Edge all in open→10:30 | **Downgraded (015):** gone by 9:35-9:40 entry → likely opening-print artifact |
| **013** | **Month-end rebalancing: on the last trading day, SPY open→close = −sign(SPY MTD − TLT MTD)** | t=+2.01, Sharpe 0.71, +17bp/trade | **t=+3.30, Sharpe 1.68, +37.5bp/trade, 67% hit (46 trades); @2bp t=+3.09** | **Survived holdout.** Dose-response in |gap|. 12 trades/yr |
| 013c | Same month-end rule on IWM / QQQ / DIA (1bp/side) | t=+2.38 / +2.07 / +2.03 | t=+2.77 / +3.55 / +2.39; +38 / +48 / +26bp/trade | Confirms 013 across equity ETFs; last hour 81% hit (MOC flows) |
| 040 | 013 rule back-cast to 1993-2002 (TYX bond proxy, never-seen data) | — | t=+0.60, Sharpe 0.20, +6.6bp/trade (114 trades) | **Caveat:** weak before 2002 → era-dependent flow effect |
