# 337 — P6 TIPS seasonal inflation accrual (agent 337)

Script: `experiments/337_tips_seasonal.py`; output: `results/337_tips_seasonal.txt`; registry: `results/registry_agent_337.csv` (4 variants, all pre-specified in the docstring before running).

**Setup.** Accrual for month M = CPI_NSA(M-2)/CPI_NSA(M-3)-1 (FRED CPIAUCNS). CPI(M-2) comes out mid-month M-1, so it is known at the M-1 month-end close. Known excess carry x_M = accrual minus the mean NSA m/m over the 12 months ending M-2. This is point-in-time and uses no revised SA factors. Positions are set at the month-end close and held all of month M on close-to-close Yahoo adjusted returns, which already include the accrual. Cost: 1bp/side on turnover. Hedge ("duration-matched"): TIP - h·IEF, where h is the 252-day OLS beta of TIP on IEF known at the decision date (mean h = 0.72; it absorbs the real/nominal yield beta). The test asks whether the hedged spread earns the *known* seasonal carry. Under efficient pricing, the quoted real yield and clean price should offset it.

Seasonality is clear in the known excess carry (holding months Jan/Feb about -0.4%, May +0.37%, Dec -0.18%; mean |x| = 0.29%/month).

| Variant | train t (Sh) | valid t (Sh) | valid bp/unit/day | gate |
|---|---|---|---|---|
| A sign(x)·(TIP - h·IEF) | +2.25 (0.68) | +1.10 (0.39) | +0.37 | fail |
| B size = accrual/0.2% (clip ±2)·(TIP - h·IEF) | +1.60 (0.48) | +0.43 (0.15) | +0.20 | fail |
| C sign(x)·(VTIP - h·SHY), 2012-10+ | +1.73 (1.16, ~2y) | +0.43 (0.15) | +0.07 | fail |
| D control sign(x)·IEF alone | -0.74 | -1.02 | -0.90 | fail (as expected) |

Descriptive check: regressing the monthly hedged spread on x gives slope +0.33 (t 1.1) in train and +0.21 (t 0.6) in valid. A slope of 1 would mean the market ignores the known carry. The market prices most of it, and the residual is not significant. Most of A's train strength comes from 2008-09 (+489, +1099bp) and 2011. Valid yearly P&L is mixed (5 of 8 years positive).

**Verdict: dead / weak.** No variant passes `lib.passes_gate`. A's direction is right in both periods, but valid t is 1.1 and the residual pass-through is small, so the known TIPS seasonal accrual is largely priced. No holdout look was taken. Nothing from 2023+ was inspected.
