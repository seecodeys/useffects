# 373 — Fallen angels around month-end IG-index exits (card E5): ANGL − h·HYG

Script: `experiments/373_fallen_angel_month_end.py` (output `results/373_output.txt`, summary `results/373_summary.csv`,
registry `results/registry_agent_373.csv`, N=4).

**Setup.** Yahoo adjusted closes (dividends included). Spread S = ANGL − h·HYG, with h = 0.267, the OLS beta of daily
returns over 2012-05..2014-12, fixed after that. **The train sample is short:** ANGL starts 2012-04, so train has only 32
month-ends (2012-05..2014). FALN starts 2016-06 and is used for a descriptive check only. W3 runs from the close of the
4th-last session to the close of the last session. F3 runs from the month-end close to the close of the 3rd session of
the new month. The signals are calendar-only, implemented with `backtest_weights(lag=1)`. Costs are 2bp/side per leg,
with 1bp/side shown as a second level. The LQD/IEF hedge is h2 = 0.581 (2003-14). No 2023+ data was used.

| Variant | events tr/va | train t | valid t | valid Sharpe | net bp/event tr / va (2bp) | @1bp tr / va | gate |
|---|---|---|---|---|---|---|---|
| V1 short S over W3 | 32/95 | −0.88 | −2.41 | −0.85 | −9.2 / −16.2 | −6.7 / −13.7 | no |
| V2 long S over F3 | 32/95 | +0.75 | +2.76 | +0.97 | +10.3 / +19.8 | +12.8 / +22.4 | no (train t) |
| V3 V2 if LQD−IEF < −1% in the prior month | 3/17 | −0.89 | +2.50 | +0.88 | −47 / +72 | −45 / +75 | no |
| V4 long LQD − h2·IEF over F3 | 144/95 | −0.76 | −0.93 | −0.33 | −3.4 / −4.9 | −0.2 / −1.8 | no |

**V2 net P&L by year (bp, summed over events):** 2012 +311, 2013 −187, 2014 +206 | 2015 +255, 2016 +191, 2017 +138,
2018 +65, 2019 +30, **2020 +1115**, 2021 +124, 2022 −35. Valid is positive in 7 of 8 years, but 2020 is about 60% of the
valid total. Excluding 2020, valid is about +9bp/event.

**Timing value vs exposure-matched holding.** For gross S, F3 beats an unconditional 3-day hold of S by +8.5bp in train
(t +0.61) and +18.3bp in valid (t +2.62). W3 shows no weakness into month-end: the gross W3 return is +4bp in train and
+11bp in valid, the opposite sign to the card. FALN−0.80·HYG (2016-07..2022, descriptive) shows F3 at +9.7bp (t +2.0)
with a timing value of +6.6bp (t +1.36), driven by 2016-17 and 2020. FALN's W3 is also not negative.

**Reading.** Only half of the mechanism appears: a month-start rebound with no month-end pressure. The low hedge ratio
(0.27) means S is mostly outright ANGL credit beta. Two other explanations fit the F3 effect: a generic month-start HY
or credit inflow, and stale ANGL prices in the early years. V3 (the widening filter) is a handful of events
dominated by 2020. V4 (IG relief) is flat to negative. With N=4, the luck threshold is t ≈ 1.67. V2's valid t of 2.76
clears it, but its train t of 0.75 on 32 events does not.

**Verdict: weak / not gate-passed.** V2 fails `lib.passes_gate` on train t. V1, the pressure leg, has the wrong sign.
The holdout was not touched. A possible follow-up is to test F3 on HYG−h·IEF itself, to separate a fallen-angel effect
from a general month-start credit effect. That would need pre-registration.
