# 358 - SNB weekly sight deposits -> EURCHF next week (card X6)

**Script:** `experiments/358_snb_sight_deposits.py` | **Output:** `results/358_output.txt`, `358_summary.csv`, `358_events.csv`
| **Registry:** `results/registry_agent_358.csv` (4 rows)

**Data:** SNB data portal public CSV API, cube `snbgwdchfsgw`, item GI (domestic banks' sight deposits, CHF mn,
Friday obs from 2009-01-09). No access restrictions. Yahoo EURCHF=X, CHF=X (spot close->close, no carry).
**Timing:** released Monday ~09:00 CET (Swiss bank holidays push to the next weekday); signal dated at the first trading
day >= release, `backtest_weights(lag=1)` -> entered at Tuesday close, held 5 trading days. Costs 2bp/side.
**Floor:** signals and P&L 2011-09-06..2015-01-15 excluded, so TRAIN = 2009-01..2011-09 only (2.7 yr, low power).
VALID 2015-01-16..2022. Nothing 2023+ read.

| Variant | episodes tr/va | train t | valid t | valid Sharpe | net bp/episode tr / va | gate |
|---|---|---|---|---|---|---|
| a dGI > +2bn: long EURCHF 5d | 22 / 75 | -0.47 | +0.26 | +0.09 | -26.8 / +5.0 | no |
| b dGI < -2bn: short EURCHF 5d | 23 / 63 | +0.64 | +0.03 | +0.01 | +21.5 / +2.3 | no |
| c z-scaled dGI, train sign (+, corr 0.08) | ~always on | -0.17 | -0.40 | -0.14 | n/a | no |
| d dGI > +2bn: long USDCHF 5d | 22 / 75 | -0.06 | +0.13 | +0.04 | -1.8 / +4.0 | no |

(Registry `valid_bp_per_trade` is per calendar day of the net series, ~0, because P&L is registered as a net series.)
Timing value vs exposure-matched holdings: t within +/-0.9 for all variants in both periods. Yearly P&L flips sign
year to year (e.g. (a) valid: +321, +143, +396 bp in 2015-17, then negative every year 2018-22).
Luck threshold with N=4 in this registry ~1.67; nothing approaches it.

**Verdict: dead.** A large weekly rise (or fall) in sight deposits carries no usable information about EURCHF/USDCHF
over the following week after release. Plausible reasons: the market sees intervention in real time (the price move
happens in the intervention week), and since 2015 sight-deposit changes are polluted by negative-rate exemption
changes, SNB bills and repo operations. Train window is tiny because the floor is excluded.
