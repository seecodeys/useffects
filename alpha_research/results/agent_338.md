# 338 — Risk-parity deleveraging on a stock-bond correlation flip (idea card P8)

Script: `experiments/338_riskparity_corr_flip.py` · output `results/338_riskparity_corr_flip.csv` ·
registry `results/registry_agent_338.csv` (4 rows). Data cut at 2022-12-31; holdout untouched.

**Hypothesis.** Risk-parity / vol-target stock-bond books scale to a portfolio-vol target. When the SPY-TLT correlation
turns positive, mix vol jumps and the rule forces sales of both legs (esp. levered bonds) over the next days.

**Trigger (pre-specified).** 21-session SPY-TLT close->close return correlation crosses above +0.2 at close t, was < 0
somewhere in the prior 63 sessions, no other up-crossing in the prior 42 sessions. Lag-1 (position from close t+1),
held 10 sessions close->close. Costs 0.5bp/side SPY, 1bp/side TLT. Timing value = net P&L minus period-mean exposure
x asset returns (exposure-matched constant holding).

| Variant | Events train/valid | Train t | Valid t | Valid Sharpe | Timing t (tr/va) | Event mean bp (tr/va) | Gate |
|---|---|---|---|---|---|---|---|
| A short 50/50 SPY+TLT 10d | 12 / 11 | -0.85 | -0.05 | -0.02 | -0.06 / +0.33 | -39 / -2 | no |
| B short TLT 10d | 12 / 11 | -0.70 | -0.86 | -0.30 | -0.23 / -0.85 | -45 / -62 | no |
| C = A when 21d mix vol > 1y median | 9 / 11 | -0.75 | -0.05 | -0.02 | -0.03 / +0.33 | -38 / -2 | no |
| D placebo: flip < -0.2 -> long 50/50 | 17 / 14 | -0.29 | +0.44 | +0.15 | -1.45 / -0.17 | -8 / +18 | no |

Yearly P&L is noisy and event-driven (e.g. A: 2021 -430bp, 2022 +322bp). The short-TLT leg (B), which the
mechanism says should be strongest, loses in both periods (hit 33% / 36%).

**Verdict: dead.** No variant has the predicted sign in train; nothing near the gate (luck t at N≈495 ≈ 3.5).
Only ~11-12 independent events per period, so power is low, but the point estimates point the wrong way.
A correlation-regime trigger shows no forced-selling footprint in SPY/TLT over the next 10 sessions, consistent with
018/122/325 (vol-target flows not visible on daily data).
