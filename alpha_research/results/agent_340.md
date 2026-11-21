# 340 - Primary-dealer Treasury inventory build-ups -> IEF/TLT (card B4)

Script: `experiments/340_dealer_inventory.py`; registry: `results/registry_agent_340.csv` (4 variants).

**Data.** NY Fed markets API, weekly as-of-Wednesday net outright Treasury positions, taken per series break:
SBP2013 (2001-07 to 2013-03): ex-TIPS total = `PDPUSGTNOP - PDPUSGTIISNOP` (checked: the total equals bills plus coupon
buckets plus TIPS exactly). SBN2013, SBN2015 and SBN2022 use `PDPOSGST-TOT`, and coupons = TOT minus bills
`PDPOSGS-B`. The older breaks are in the API through `/api/pd/get/{seriesbreak}/timeseries/{keys}.json`, so no
spreadsheets were needed. Values are the current vintage, not point-in-time.
**Lag.** Data as of Wednesday W is released Thursday W+8 after the close. The signal is dated to the first trading day
on or after W+8, and `backtest_weights(lag=1)` puts the position on from the next close, normally Friday's.
**Signal.** z = 4-week change minus its rolling 52-week mean, divided by the rolling sd. It is computed within each
series break, with at least 26 observations and nothing spliced across breaks. Cost is 1bp/side. Train runs
2002-08 to 2014; valid runs 2015-22.

| variant | events tr/va | train t | valid t | valid Sharpe | net bp/held day tr/va | net bp per full trade tr/va | timing-value t tr/va | gate |
|---|---|---|---|---|---|---|---|---|
| a z>+1 long IEF, 4w ladder | 100/59 | +2.04 | +1.75 | +0.62 | +1.26/+1.21 | +62/+54 | +0.46/+1.58 | no |
| b symmetric +-1 IEF, 4w | 194/118 | +0.37 | +1.64 | +0.58 | +0.17/+0.78 | +7/+38 | +0.20/+1.67 | no |
| c z>+1 long TLT, 1w | 100/59 | +1.29 | +1.53 | +0.54 | +5.5/+11.0 | +26/+52 | +0.49/+1.49 | no |
| d coupons z>+1 long IEF, 4w | 98/59 | +1.79 | +1.70 | +0.60 | +1.28/+1.24 | +57/+60 | +0.38/+1.55 | no |

How to read it: (a) is long about 40%/33% of days at an average weight of 0.42. Most of the raw P&L is ordinary bond
carry and duration premium. The timing value against exposure-matched IEF holdings is insignificant in train
(t 0.46) and about 1.5-1.7 in valid. Train P&L depends heavily on 2008 (+992bp of variant a's train total), and 2009
is the worst year. Luck threshold for the best of N=4 is about 1.67.

**Verdict: dead (fails gate).** Every variant has the "right" sign in both periods. But no variant has valid t >= 2,
and the edge over simply holding IEF/TLT is not significant in train. Build-ups in dealer inventory do not add
reliable timing value beyond the long-duration premium. Possible follow-up, not tested: interact with auction
calendar (B5/331) or dealer balance-sheet constraints (SLR, quarter-ends).
