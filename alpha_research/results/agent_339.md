# 339 - Dealer capital shocks -> intermediated-asset premia (idea card P9, He-Kelly-Manela 2017)

Script: `experiments/339_dealer_capital.py` (output: `results/339_output.txt`); registry: `results/registry_agent_339.csv` (4 rows).

**Signal.** At each month-end close, D = 1-month return of an equal-weight GS/MS/JPM/C/BAC basket minus SPY.
Survivorship: Lehman/Bear/Merrill are missing. The quintile cut (q20 = -4.06%) and the z-score (mean +0.17%, sd 6.14%) use TRAIN months 2000-14 only.
Positions are held for the next month with lib.backtest_weights lag=1. Duration hedges are fixed in advance: HYG-0.5 IEF and LQD-1.1 IEF.
Costs are 1bp/side for credit and 0.5bp/side for SPY. Windows start when each asset has data (HYG 2007-04, LQD/IEF 2002-08) and end 2022-12-31.

**Events (trigger months).** 36 in train 2000-14, of which 26 fall in the HYG window 2007-04 to 2014. 20 in valid 2015-22.

| Variant (pre-specified) | train t | valid t | valid Sharpe | timing-value t (train / valid) | gate |
|---|---|---|---|---|---|
| A bottom-quintile D -> HYG-0.5IEF | +0.04 | +0.39 | +0.14 | -0.22 / +0.17 | fail |
| B bottom-quintile D -> LQD-1.1IEF | -0.83 | +0.09 | +0.03 | -0.84 / -0.04 | fail |
| C bottom-quintile D -> long SPY | -0.07 | +0.79 | +0.28 | -0.50 / +0.24 | fail |
| D continuous HYG-0.5IEF ~ -z(D) | -0.70 | +0.59 | +0.21 | -0.77 / +0.60 | fail |

"Timing value" is net P&L minus exposure-matched holdings: held weight times each asset's mean daily return in the same period.
Yearly P&L is driven by 2008-09 (A: -1033bp in 2008; D: -2644bp in 2009), with an outsized 2019 in valid. With N = 4 tests in this registry, a luck t is about 1.7.

**Verdict: DEAD.** No variant passes lib.passes_gate, and no train t is above 1. Dealer-relative equity shocks do not predict next-month credit or equity premia at the ETF level.
In 2008, the shocks were followed by further losses rather than the HKM rebound. A monthly sample of about 20-36 events is also too small to detect a modest effect.
