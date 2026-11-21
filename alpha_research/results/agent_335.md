# 335 European-open overnight drift (idea P1): DEAD

Script: `experiments/335_european_open_drift.py`. Registry: `results/registry_agent_335.csv` (4 tests). Table: `results/335_european_open_drift.csv`.
Data: HistData SPXUSD/NSXUSD 1-min CFD, NY local timestamps (no re-conversion), 2010-2022 only loaded (train 2010-14, valid 2015-22; 2023+ never loaded or looked at).
Trade: 02:00 to 03:00 NY on each SPY trading day D. Signal: the CFD's own 15:00 to 16:00 return on the prior US day (known at entry). Cost 0.5bp/side (1bp/side sensitivity).

Pre-specified variants (docstring, before results): A long every day; B long only if the prior last hour < 0; C -sign(prior last hour) long/short; D = B on NSX.

| variant | events tr/va | train t | valid t | valid SR | train bp gross/net | valid bp gross/net | valid net @1bp | gate |
|---|---|---|---|---|---|---|---|---|
| A long 02-03 SPX | 1006/1915 | -0.04 | -1.17 | -0.42 | 0.98 / -0.02 | 0.59 / -0.41 | -1.41 | no |
| B long after neg last hour SPX | 467/915 | 1.02 | 0.28 | 0.10 | 1.73 / 0.73 | 1.14 / 0.14 | -0.86 | no |
| C -sign(last hour) SPX | 974/1886 | -0.44 | -1.45 | -0.53 | 0.80 / -0.20 | 0.48 / -0.52 | -1.52 | no |
| D long after neg last hour NSX | 500/907 | 0.58 | 0.53 | 0.19 | 1.37 / 0.37 | 1.34 / 0.34 | -0.66 | no |

Timing value: the 02-03 hour is among the best overnight hours (mean 0.94bp train / 0.63bp valid, against an average of 0.15 / 0.09bp for the other 15 hours from 17:00 to 09:00). The direction matches the paper. But a gross edge under 1bp per hour cannot pay a 1bp round trip.
Conditioning on last-hour selling (B, D) raises gross to 1.1-1.7bp, but the t-stats stay at or below 1 and the net edge is about 0.1-0.7bp. The long/short version (C) fails: the effect, if real, is not symmetric. Yearly P&L is noisy, with 2020 the largest positive year in B and D.
Verdict: no variant passes `lib.passes_gate`. The gross drift agrees with Boyarchenko-Larsen-Whelan, but it is too small to trade with a CFD/ETF at these costs. Luck bound for N=4: about 1.7.
