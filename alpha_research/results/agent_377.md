# 377 — TIPS-specific month-end extension (idea E4)

Script: `experiments/377_tips_extension.py` · registry: `results/registry_agent_377.csv` (4 rows) · no holdout look.

**Hypothesis.** TIPS index extension (larger when a new or reopened TIPS settles) makes benchmarked managers buy TIPS
over nominals into month-end, so the 10y breakeven and TIP − h·IEF should rise over the last 3 sessions (W3), more
in "TIPS months".

**Data and timing.** TIPS calendar from fiscaldata `auctions_query` with `inflation_index_security=Yes`. The API has no
'TIPS Note'/'TIPS Bond' security_type: TIPS are 'Note'/'Bond'. A month is a TIPS month if a TIPS issue_date is in it
and was announced before W3 starts. FRED T10YIE comes from H.15 end-of-day data published the next business day. It is
only the outcome measure, never the signal, so the release lag cannot leak; the signal is the pre-announced calendar.
BE pseudo-return = 8 × ΔT10YIE. TIP/IEF are adjusted close→close, with static h = 0.726 (OLS, 2004-14 train only).
lag=1, costs 1bp/side per leg. Pre-specified V4 (DFII10 − DGS10 change) is identical to ΔT10YIE, so it was replaced
before the run by the unconditional spread.

| Variant | train t | valid t | valid Sharpe | valid bp/unit/day | events tr/va | net bp/event tr/va | gate |
|---|---|---|---|---|---|---|---|
| V1 long BE, W3, all months | −0.23 | −0.33 | −0.12 | −0.50 | 143/96 | −2.8 / −3.5 | no |
| V2 long BE, W3, TIPS months | +0.35 | −0.33 | −0.12 | −0.50 | 82/96 | −0.4 / −3.5 | no |
| V3 TIP − h·IEF, W3, TIPS months | +0.84 | +0.76 | +0.27 | +0.66 | 80/96 | +1.6 / +1.7 | no |
| V4 TIP − h·IEF, W3, all months | +1.44 | +0.76 | +0.27 | +0.66 | 132/96 | +3.5 / +1.7 | no |

Descriptive (gross bp per W3): TIPS vs non-TIPS months, train only. BE: +3.6 vs −2.1 (diff t +0.76). Spread: +5.1 vs
+9.8 (diff t −0.56), which is the wrong sign for the dose-response. Timing value (W3 minus the average 3-day return in
the same month): spread +6.0 (t 1.56) train / +3.2 (t 0.68) valid; BE +1.2 / −0.3.

**Key caveat.** From 2015 every month has a TIPS settlement (monthly auction/reopening cycle), so the TIPS-month flag
cannot be tested in valid: V2 = V1 and V3 = V4 there. Before 2015 the contrast has the wrong sign for the spread.

**Comparison.** 280 (TIP outright, last 3 sessions, +3.31 / +2.91) is mostly the nominal duration bid. With the
nominal bid hedged out, the TIPS-specific residual is about +2-5bp per event gross, about 0 net, and not significant.
337 (accrual seasonality) is a different mechanism; there is no overlap.

**Verdict: dead.** No breakeven widening into month-end and no TIPS-supply dose-response. The TIPS month-end bid is
the nominal Treasury extension bid. With 4 tests here (and about 520+ in total), the luck threshold is t ≈ 3.5.
