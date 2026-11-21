# 348 — Japan MOF weekly securities flows -> next-week USDJPY / TLT / EWJ / DXJ (idea X5)

Script: `experiments/348_mof_weekly_flows.py`; registry `results/registry_agent_348.csv` (4 variants); summary `results/348_summary.csv`.

**Data:** MOF public `week.csv` (2005-01 onward, 100m JPY), current history (no vintages; weekly revisions small).
Residents' net foreign long-term debt purchases (bond signal); non-residents' net Japanese equity purchases (equity signal).
**Timing:** week ends Saturday; release 08:50 JST on the 4th Japanese business day after it (Thursday normally; 235 of 1141 weeks later due to JP holidays),
i.e. NY evening before. Signal assigned to the first US trading day >= that NY date, `backtest_weights(lag=1)` -> enter at the next close (normally Thursday close),
hold ~5 days until the next signal. z = flow / std(prior 52 weeks); trade |z| > 1.5. Costs 2bp/side USDJPY/EWJ/DXJ, 1bp TLT. Yahoo JPY=X: 4 bad prints (>8%) nulled.

| Variant | train t | valid t | valid Sharpe | trades tr/va | net bp/trade tr/va | timing-value t tr/va | gate |
|---|---|---|---|---|---|---|---|
| a USDJPY ~ bond z, +-1 | +0.48 | +0.34 | +0.12 | 67/45 | +13.9 / +7.7 | +0.47 / +0.31 | no |
| b TLT long ~ bond z > 1.5 | +0.15 | -0.60 | -0.21 | 42/27 | +6.7 / -30.5 | -0.46 / -0.61 | no |
| c EWJ ~ equity z, +-1 | -0.74 | -0.70 | -0.25 | 55/37 | -41.1 / -33.4 | -0.77 / -0.65 | no |
| d DXJ ~ equity z, +-1 | -0.68 | -1.16 | -0.41 | 50/37 | -39.7 / -66.7 | -0.73 / -1.08 | no |

Flows do persist (weekly AR(1): bond 0.13/0.20, equity 0.42/0.42 train/valid), but the persistence is not priced into next-week returns.
USDJPY has the right sign in both periods but t < 0.5; equity-flow chasing in EWJ/DXJ is mildly contrarian (wrong sign, insignificant). Luck threshold for N=4 ~1.67.

**Verdict: dead.** No variant passes `lib.passes_gate`; no holdout look taken. Nothing from 2023+ examined.
