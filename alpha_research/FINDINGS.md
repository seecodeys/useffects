# Findings (gate-passed and holdout-tested signals only)

| # | Signal | Valid (2015-22) | Holdout (2023+) | Status |
|---|---|---|---|---|
| 001 | Short QQQ open→close on monthly OPEX Friday (3rd Fri) | t=+2.22, Sharpe 0.78, +26bp/trade | t=+0.83, Sharpe 0.42, +12.6bp/trade (44 trades) | Weak-but-alive; tiny capacity in time (12 trades/yr) |
| 004 | Cross-sectional ADR catch-up vs home share (price ≥ $20, long 2 / short 2, open→close) | t=+4.9, Sharpe 1.73, +4.7bp/day @3bp | t=+2.56, Sharpe 1.30 @3bp; ~0 @5bp. Edge sits in the first hour (open→10:30) | Execution-dependent: needs real opening-auction fills to confirm it isn't bid/ask bounce |
| 006 | Large-cap overnight-gap reversal (decile L/S vs SPY gap, price ≥ $20) | @1bp: t=+2.10, Sharpe 0.74 | @1bp: t=+1.0, Sharpe 0.51; @2bp: 0. Edge all in open→10:30 | Weak; needs ≤1bp opening-auction execution |
