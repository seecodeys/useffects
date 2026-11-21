# Agent 367 - FINRA margin statistics: leverage / forced-deleveraging (data source 2)

Script: `experiments/367_finra_margin_deleverage.py`; output `results/367_output.txt`, `results/367_summary.csv`;
registry `results/registry_agent_367.csv` (N=8: run 1 had a NaN bug in the expanding-percentile helper, so 367C
never fired and B was slightly biased; fixed and re-run; both runs registered, run 2 rows are authoritative).

Data: FINRA margin-statistics.xlsx (1997-01+), debit balances D and total free credits F (cash + margin columns, which
splices the 2010-02 column break). **Lag used:** month-m data first used at the close of the last trading day of m+1
(release is ~3rd week of m+1), plus backtest_weights lag=1. So the position runs from the 1st trading day of m+2 for one
month. SPY, 0.5bp/side, monthly rebalancing. lib TRAIN = 2000-2014 (1997-99 is burn-in for 12-36m windows).

| Variant (pre-specified) | train t | valid t | valid Sharpe | valid net bp/day held | signal months tr/va | gate |
|---|---|---|---|---|---|---|
| A short SPY in deleveraging regime (12m chg <0 after >+20%) | +0.89 | +0.14 | +0.05 | +1.2 | 31 / 15 | no |
| B short SPY when 3m margin chg in bottom decile | -0.67 | -0.82 | -0.29 | -9.4 | 22 / 12 | no |
| C short SPY when debt/price (60m-demeaned) top quintile | -0.06 | n/a | n/a | n/a | 46 / 0 | no |
| D long SPY when free-credit/debt top decile | -0.41 | n/a | n/a | n/a | 46 / 0 | no |

Net per signal month: A +134bp train / +21bp valid (timing-value t +1.19 / +0.65 vs exposure-matched drift); driven by
2001 and 2008 (+1535, +3357bp) and lost in 2012 and 2019. B is wrong-signed in both periods (sharp margin falls
are followed by rebounds rather than more selling, but not significantly). C and D never fire in 2015-22 (debt grew
faster than prices and free credit), so they cannot be validated.

**Verdict: dead.** Nothing near the gate. A is the 2001/2008 bear market in disguise: margin debt is a lagging,
pro-cyclical stock with a 7-8 week effective lag, so it adds no reliable timing beyond the drawdown itself.
With N=8 here, luck alone gives a best |t| near sqrt(2 ln 8) = 2.0, and no variant reaches it. Nothing from 2023+ was viewed.
