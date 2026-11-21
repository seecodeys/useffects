# 361 — Treasury-futures quarterly roll window (idea card S1, results/ideas_settlement.md)

Script: `experiments/361_treasury_roll_window.py`. Output: `results/361_output.txt`. Events: `results/361_events.csv`;
ZN=F roll jumps: `results/361_zn_jumps.csv` (all through 2022 only). Registry: `results/registry_agent_361.csv` (4 rows).

**Setup.** CBOT rule: First Notice Day (FND) is the last business day of the month before delivery, i.e. the last
business day of Feb/May/Aug/Nov for the Mar/Jun/Sep/Dec contracts. IEF trading days stand in for CBOT business days.
Day 0 is the last business day of the month. The roll window holds days −8..−4: enter at the close of day −9, exit
at the close of day −4. Day −3 and the last 3 sessions are excluded, so the window never overlaps the month-end bond
bid (LOG 275/276). The control is the same days in the other 8 months, which also nets out any generic intra-month
seasonal. The delivery window holds business days +1..+5 of Mar/Jun/Sep/Dec, against the same days in other months.
That control nets out the early-month reversal from LOG 281/319.

The trade is the difference-in-means turned into a strategy: s·(+1) on event-month windows and s·(−0.5) on
control-month windows. Over a year that is duration-neutral and exposure-matched (4×1 = 8×0.5). The sign s is set
from the train difference only. The calendar is known in advance, so lag 0 is used. Costs are 1bp/side. Train has 50
event windows (2002-14) and valid has 32 (2015-22).

| Variant (pre-specified) | train diff bp/window (Welch t) | valid diff bp/window (Welch t) | train t | valid t | valid Sharpe | gate |
|---|---|---|---|---|---|---|
| A IEF roll −8..−4, Feb/May/Aug/Nov vs other months | +28.9 (1.86) | +4.6 (0.27) | +1.53 | +0.04 | +0.01 | no |
| B TLT same window | +61.5 (1.89) | +14.1 (0.35) | +1.75 | +0.26 | +0.09 | no |
| C IEF delivery +1..+5, Mar/Jun/Sep/Dec vs other months | −7.1 (−0.40) | +5.3 (0.24) | +0.16 | −0.43 | −0.15 | no |
| D A, only after a "rich" previous ZN=F roll jump | +16.8 (0.79) | −16.3 (−0.81) | +0.58 | −0.97 | −0.34 | no |

Per-window net bp: A's valid gross edge is about +4.6bp per event-vs-control pair. The round trip costs 2bp per
window, so net is about 0 (the registry shows +0.04bp per held day). B nets about +12bp per pair, with 50-60bp
per-window noise.

**Notes.** The train edge in A/B is concentrated in 2006-11 (yearly P&L): positive in 2006-11, mixed elsewhere, and
2016/20/21 were large losers in valid. The richness proxy in D picks the ZN=F switch day as the day with the maximum
volume ratio in the second half of the delivery month. Detected switch days fall on the 19th-22nd with volume ratios
around 100×, which is plausible. However, the residual jump (ZN=F minus β·IEF) is a noisy proxy for the calendar
spread. Feb/May/Aug/Nov are also refunding months, but the refunding auctions fall in the 2nd week, before this
window, and 276d found no refunding-month difference in the month-end bid. The project has about 430 or more
registered tests, so luck alone gives a best t of about √(2 ln N) ≈ 3.5. Nothing here comes close.

**Verdict: dead.** No variant passes `lib.passes_gate`. The roll-window bid in IEF/TLT looked suggestive in train
(Welch t about 1.9, and the direction was chosen in train) but vanished in 2015-22. The delivery window and the
richness conditioning show nothing. No holdout look was taken.
