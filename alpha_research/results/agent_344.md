# 344 — P3 same-half-hour persistence (Heston-Korajczyk-Sadka 2010), cost-aware

Script: `experiments/344_same_halfhour_persistence.py`. Registry: `results/registry_agent_344.csv` (4 variants).
Data: HistData SPXUSD/NSXUSD 1-min CFD, NY clock, 2010-2022 only (nothing 2023+ loaded). 13 RTH half-hour bins.
Signal for bin k on day t: sign(r_k(t-1)) + sign(r_k(t-5)); trade +1/-1 only when lag-1 and lag-5 agree, else flat.
Cost 0.5bp/side per bin trade (1bp round trip). Train = 2010-14 (CFD data start), valid = 2015-22.

| Variant | events (bin-trades/day, valid) | train t | valid t | valid Sharpe | valid net bp/bin-trade | gross bp train / valid | long-only same bins (valid) | gate |
|---|---|---|---|---|---|---|---|---|
| V1 SPX all 13 bins | 6.06 | -3.24 | -4.93 | -1.74 | -1.00 | +0.12 / -0.00 | +0.10 | fail |
| V2 SPX top-3 train bins (10:30, 11:30, 12:30) | 1.42 | +1.56 | -0.25 | -0.09 | -0.10 | +1.78 / +0.90 | +0.50 | fail |
| V3 NSX all 13 bins | 6.24 | -2.12 | -4.52 | -1.60 | -1.18 | +0.36 / -0.18 | +0.13 | fail |
| V4 NSX top-3 train bins (11:30, 12:30, 11:30-12:00) | 1.48 | +1.10 | -0.38 | -0.14 | -0.17 | +1.56 / +0.83 | -0.05 | fail |

(V4 bins = 6,4,5 = 12:30, 11:30, 12:00 starts.)

Findings
- Gross same-slot persistence on the index CFDs is essentially zero across all 13 bins (valid gross ~0 bp/trade);
  the HKS effect is a cross-sectional single-stock phenomenon and does not show up in the index.
- Train-selected midday bins keep a small positive gross edge out of sample (~+0.85 bp/trade valid) but below the
  1bp round-trip cost; it is also lumpy (2022 and 2011 carry most of it). Timing value vs exposure-matched long-only
  in the same bins: SPX +0.40bp, NSX +0.88bp gross — too small to pay costs.
- N=4 in this registry; luck t ~1.67. Nothing near the gate.

Verdict: **dead** (net of costs). No holdout look taken.
