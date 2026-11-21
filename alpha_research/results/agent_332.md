# 332 — Mortgage convexity hedging: big 10y-yield move continuation (idea B3)

**What.** Trigger at close t: |5-day change in FRED DGS10| > 21.0bp (90th pct of 2002-14). Trade in the direction of the
yield move (yields up → short bonds), hold 5 sessions, entered lag-1 at the close (close t+1 → close t+6), 1bp/side.
"Convexity zone" (known at t): last released MORTGAGE30US (PMMS, used only from the session after its Thursday date)
within ±75bp of its own mean from 3-7 years earlier. The zone covers 35% of 2002-22 days, clustered in 2006-09 and
2015-20 (none in 2002-05 or 2010-14). Script: `experiments/332_convexity_hedging.py`; registry:
`results/registry_agent_332.csv` (4 rows). Nothing from 2023+ printed. Holdout untouched.

**Events (trigger days, train 2002-14 / valid 2015-22):** all 289 / 116; in-zone 81 / 57; out-of-zone 208 / 59.

| Variant (pre-specified) | train t | valid t | valid Sharpe | valid bp/unit | timing t (tr / va) | gate |
|---|---|---|---|---|---|---|
| A TLT, all triggers (unconditional) | +0.49 | +0.17 | +0.06 | +1.4 | +0.51 / +0.15 | no |
| B TLT, in-zone triggers only | +0.72 | -0.44 | -0.16 | -6.6 | +0.63 / -0.45 | no |
| C IEF, in-zone triggers only | +0.05 | +0.14 | +0.05 | +0.8 | -0.08 / +0.11 | no |
| D short MBB / long IEF after in-zone trigger (2007+) | +1.14 | +0.38 | +0.14 | +1.0 | +0.80 / +0.34 | no |

**Interaction (in-zone vs out-of-zone).** Per-event 5-day trade return, with overlapping events, so the t-stats are overstated:
- TLT: train in +19bp vs out +10bp (diff t +0.29); valid in +9bp vs out +25bp (diff t −0.26). The sign flips between periods.
- IEF: train in −4bp vs out +1bp (diff t −0.24); valid in +25bp vs out −7bp (diff t +1.19).

The conditioning adds nothing consistent. Card variant (b), the 1-day >10bp trigger with a 3-day hold, was dropped to keep
to 4 variants.

**Notes.** The yearly P&L is lumpy: B is hurt by 2020 (−1422bp, the March 2020 Treasury dislocation), while the unconditional
A's best years are 2003, 2008 and 2022. D (MBS underperforming during hedging) has the right sign in both periods, but
2008-09 (+947bp) drives it and its t is ≈1. With only 4 local tests here and hundreds in the main registry,
the luck-level t is ~3.5.

**Verdict: dead.** No variant passes `lib.passes_gate`. There is no measurable continuation of big 10y moves in TLT/IEF at
lag 1, and the convexity-zone conditioning does not strengthen it. The MBB−IEF spread is a weak hint driven by 2008-09
and not worth pursuing.
