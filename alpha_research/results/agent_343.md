# 343 - Closing-auction pressure reversal after the close (card P2)

Script: `experiments/343_close_auction_reversal.py`; table `results/343_close_auction_reversal.csv`; registry
`results/registry_agent_343.csv` (4 variants, all pre-specified). Data: HistData SPX/NSX CFD 1-min, 2010-2022 only
(2023+ never loaded). Signal s = 15:50→16:00 CFD return; position −sign(s); entry at the 16:01 bar (one bar after the
signal bar); 0.5bp/side. Early-close days dropped (Jul 3, Dec 24, day after Thanksgiving). ~2,950 SPX days with data
(CFD gaps); TRAIN here = 2010-14 only.

| Variant | n trades | train t | valid t | valid Sharpe | gross bp/trade train / valid | net bp/trade valid | gate |
|---|---|---|---|---|---|---|---|
| A SPX fade, 16:01→16:14 | 2817 | −6.25 | −3.95 | −1.43 | −1.12 / +0.02 | −0.98 | no |
| B SPX fade, 16:01→next 09:29 | 2816 | −0.59 | +0.18 | +0.07 | −0.15 / +1.28 | +0.28 | no |
| C A only if abs(s) > train p80 (14.9bp) | 665 | −3.37 | −2.19 | −0.79 | −2.67 / −0.58 | −1.58 | no |
| D NSX fade, 16:01→16:14 | 2898 | −2.26 | −2.72 | −0.98 | +0.09 / +0.08 | −0.92 | no |

Timing vs exposure: the always-long return over the window is −0.56 / −0.10bp (16:01-16:14, train/valid) and
+2.28 / +2.03bp (overnight, train/valid). B's gross +1.28bp in valid is below the long-only overnight drift. It is noise
(t=0.18, and the yearly sign flips).

Findings: there is no after-close reversal in the index. In 2010-14 the 15:50→16:00 move *continues* over 16:01-16:14
(corr(s, r) = +0.12 train, +0.07 valid; fading lost 1.1bp gross per trade in train). By 2015-22 the gross effect is
about zero, so the net result is roughly −1bp, which is just the cost. A larger push (C) does not help. NSX shows the same.
The negative t-stats are mostly costs on a ~0bp gross edge (net = gross − 1bp). The continuation is real only in train,
and is gone after costs in valid. The reversal that Bogousslavsky-Muravyev document is a single-stock deviation from
the 4pm midquote. It does not show up at the index-futures level, where the auction imbalance is largely hedged and
arbitraged in real time.

Implementability: the CFD prices off the futures session, so A/C/D would mean trading ES/NQ between 16:00 and 16:15 ET
(possible; ES traded through 16:15 then halted to 16:30 before 2021). B is an ES overnight hold. All are implementable
in principle at about 0.5bp/side, but none has a positive edge.

Verdict: **dead** (0/4 pass `lib.passes_gate`; no holdout look). The opposite direction (follow) was not tested: it
would be a new variant chosen after seeing results, and valid gross ≈ 0 anyway.
