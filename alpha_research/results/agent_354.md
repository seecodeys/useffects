# 354 — VVIX (vol-of-vol) -> tail-hedge returns  (card 5, ideas_options_etf.md; Park, FEDS 2013-54)

Script: `experiments/354_vvix_tail_hedge.py` · registry: `results/registry_agent_354.csv` (4 rows).
Data: Cboe VVIX + VXTH history (cdn.cboe.com, copied to data/cboe/), data/cboe/PPUT.csv, Yahoo VIXY/^VIX/^SP500TR.
VVIX prints < 40 (2006-07 bad ticks) dropped. Signals known at close t, positions lag-1 via lib.backtest_weights.
Registered series = timing value vs exposure-matched always-on leg (per period). Thresholds fixed on 2006-14.
Not a duplicate of 082 (VIX spikes, contango), 154 (VIXY big-day fade) or 301 (CFTC positioning).

| Variant | Events train / valid | Train t | Valid t | Valid Sharpe | Timing value train / valid (bp/yr, net) | Gate |
|---|---|---|---|---|---|---|
| A short VIXY 5d after VVIX/MA60 > p80 (5bp/side), 2011+ | 56 / 79 | +0.70 | −0.21 | −0.07 | +1141 / −296 | no |
| B short PPUT−SPXTR 20d after VVIX top tercile (2bp/side) | 99 / 87 | −0.20 | −0.06 | −0.02 | −29 / −1 | no |
| C as B, signal VVIX/VIX top tercile | 53 / 46 | +0.10 | −1.93 | −0.68 | +18 / −257 (2020: −2165) | no |
| D short VXTH−SPXTR 20d after VVIX top tercile | 99 / 87 | −0.43 | −0.75 | −0.27 | −54 / −21 | no |

Descriptive (forward 20-session leg return by train tercile, ≤2022): no monotone pattern. PPUT−SPXTR by VVIX
tercile: train −22/−25/−32bp, valid −27/−20/−27bp. VXTH−SPXTR: train +10/+7/−15, valid −16/−31/+15 (sign flips).
By VVIX/VIX the valid ordering is the OPPOSITE of the hypothesis (−69/−45/−10bp: hedges cheapest when vol-of-vol per
unit vol is LOW).

Caveat: VVIX's level drifted up after 2014, so the level-tercile variants (B, D) are ~96% of valid days "on" — they
barely differ from always-short and have little power. The relative signals (A, C) don't have that problem, and both fail.

Verdict: **dead.** None passes lib.passes_gate. High VVIX does not predict worse subsequent tail-hedge returns
(VIXY, PPUT put leg, VXTH VIX-call leg) at a 5–20 day horizon after costs. Luck t for the best of N=4 is about 1.7.
No holdout look.
