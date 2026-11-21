# 382 — Prior-year IRA/HSA contribution-deadline flows (agent-run)

Script: experiments/382_ira_deadline_flows.py · output: results/382_output.txt · registry: results/registry_agent_382.csv

**Story.** Prior-year IRA/HSA contributions are allowed until the federal filing deadline (~Apr 15, next business day on
weekends/Emancipation Day; Jul 15 2020, May 17 2021). Cash lands in brokerage/retirement accounts just before and is
invested → buying pressure into the deadline. Calendar-known, lag 0. Not a duplicate: 177 (short SPY into quarterly tax
dates) and 305 (TGA tax-date drain) test tax *payments*, not the contribution window.

**Events.** 1/year: SPY 15 train (2000-14) / 8 valid (2015-22); VTI 13 / 8 (starts 2001-06). PRE = 10 sessions ending
on the deadline session (inclusive); POST = 5 sessions after. Costs 0.5bp/side.

| Variant (pre-specified) | train t | valid t | valid Sharpe | net bp/event train / valid | gate |
|---|---|---|---|---|---|
| A long SPY PRE, timing value vs exposure-matched long | −0.52 | +0.62 | 0.22 | −51 / +57 | fail |
| B SPY PRE+POST in deadline month, short 1/11 same window in other 11 months | +0.98 | +0.61 | 0.22 | +128 / +75 | fail |
| C long VTI PRE, timing value | −0.24 | +0.56 | 0.20 | −21 / +53 | fail |

Per-year (bp, ≤2022) in results/382_output.txt: A is positive in 10/23 years; sign flips between train and valid;
largest years 2009 (+608) and 2000 (−662). Mean SPY cc during PRE: train −2.5bp/day vs +2.5 all days; valid +10.3 vs +4.5.
The POST-5 sessions averaged +32bp/day in train (+1.1 valid) — this is what drives B's train number; it's a post-hoc
observation (not a separate registered variant), so it is not promoted.

**Verdict: dead.** No variant near the gate; A/C wrong-signed in train. With ~715 prior registered tests, luck t ≈ 3.6.
