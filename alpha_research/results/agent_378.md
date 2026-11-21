# 378 — MSCI index-review windows: close-in pressure and post-effective reversal (review vs other month-ends)

Script: `experiments/378_msci_review_window.py`. Registry: `results/registry_agent_378.csv` (4 rows). Data stops at
2022-12-31; no holdout look. Extends 178, which tested only review-day open->close and close->next open vs SPY (dead).

**Setup.** D = last US trading day of Feb/May/Aug/Nov (MSCI review effective close). Spreads are beta-hedged
X − β·SPY, with β = 250-day close->close beta measured through the entry close. Calendar-known, so lag 0. Costs
2bp/side EEM, 1bp/side EFA and SPY (turnover per leg). Direction = sign of the train mean (178 convention). Each event's
net P&L is booked on its exit day, so bp/trade = net bp per event. Overlap with the month-end work (013/017/257/275-284)
is handled by variant D: the same trade on non-review month-ends. B − D isolates the MSCI part.

| Variant (pre-specified) | sign | events tr/va | train t | valid t | valid Sh | net bp/event tr / va | timing t tr/va | gate |
|---|---|---|---|---|---|---|---|---|
| A EEM−βSPY, last 3 sessions into D, all review months | + | 42/32 | +0.10 | −1.69 | −0.60 | +2.5 / −62.0 | +0.05/−1.50 | no |
| B EEM−βSPY, close D → close D+2, May/Nov | + | 22/16 | +1.42 | +1.83 | +0.65 | +43.2 / +57.1 | +1.42/+2.20 | no |
| C EFA−βSPY, close D → close D+2, May/Nov | + | 22/16 | +2.77 | +1.90 | +0.67 | +54.9 / +33.9 | +3.49/+2.31 | no |
| D placebo: B's trade on non-review month-ends | + | 84/64 | +1.34 | +0.16 | +0.06 | +19.7 / +2.8 | +1.28/+0.49 | no |

Timing value = per-event net minus the same-sign unconditional mean of the same-length hedged spread.

**B vs D (MSCI part):** train +43 vs +20bp (Welch t +0.71); valid +57 vs +3bp (t +1.61). The difference points the
right way but is not significant.

**Notes.** The sign is "foreign outperforms SPY after the semi-annual review". That is a continuation in foreign markets
after the effective day, not the reversal the hypothesis expected (A shows no consistent pressure into D). C is the best
variant, but its valid result rests on 16 events and 2020 contributes +327 of +566bp. B's valid is similar (2020 +441bp).
The registry holds ~700 tests, so pure luck produces a best t of about sqrt(2 ln 700) ≈ 3.6. VWO was left out (it moved
from MSCI to the FTSE index in 2013).

**Verdict: dead/weak.** No variant passes `lib.passes_gate`. C (EFA after May/Nov reviews) is a weak, small-sample hint,
and its gap over ordinary month-ends is not significant.
