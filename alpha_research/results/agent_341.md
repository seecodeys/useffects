# 341 — Foreign official custody Treasury drawdowns -> short Treasuries (card B7)

Script: `experiments/341_foreign_custody.py` (output `results/341_output.txt`, registry `results/registry_agent_341.csv`, N=4).

**Setup.** FRED WMTSECL1 (Wednesday level, nonzero from 2007-07). H.4.1 is out Thursday 16:30 ET, so a Wednesday
observation counts as known at the close of the first trading day on or after Wed+2 (Friday). Positions start one more
day later (`backtest_weights lag=1`). g = 4-week % change. p10 and p90 come from train data only (2007-08..2014, n=388):
p10 = -0.71%, p90 = +2.93%. Cost is 1bp/side and is included in net. The train window is only about 7 years and holds
about 31 episodes, so power is low.

| Variant | trig wks tr/va | train t | valid t | valid Sharpe | valid bp/day | net bp/round trip tr / va | timing-value t tr / va | gate |
|---|---|---|---|---|---|---|---|---|
| a g<p10 → short TLT 20d | 39/88 | -0.40 | +0.65 | 0.23 | +0.98 | -81 / +90 | +0.37 / +0.70 | no |
| b a & UUP 20d>0 → short TLT 20d | 22/56 | -0.52 | +1.43 | 0.51 | +1.87 | -91 / +190 | +0.13 / +1.47 | no |
| c g<p10 → short IEF 10d | 39/88 | -0.14 | +0.81 | 0.29 | +0.45 | -7 / +30 | +0.78 / +0.94 | no |
| d g>p90 → long IEF 20d | 39/0 | +0.09 | n/a | n/a | 0 | +13 / — | -0.75 / — | no |

**Notes.**
- The train sign is negative for a, b and c. The valid result is positive mainly because of 2022, a year of Fed hikes
  and a strong dollar, when Japan also intervened: +1786bp in (a) and +2461bp in (b). Without 2022, the valid years are
  mixed.
- After 2014, custody holdings trend down structurally. That makes the p10 trigger fire more than twice as often in
  valid (88 weeks against 39 in train). It also means (d) never fires in valid, because the train p90 is not reached
  again.
- With N=4, the best-of-N luck t is about 1.67, and no variant reaches t=2 in either period.

**Verdict: dead.** No variant passes `lib.passes_gate`, train and valid signs disagree, and the valid result depends on
2022. The holdout was not touched.
