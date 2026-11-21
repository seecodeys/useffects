# 381 — Social Security payment Wednesdays (SPY; BIL placebo)

Script: `experiments/381_social_security_wednesdays.py` · registry: `results/registry_agent_381.csv` (4 variants)

Story: SSA pays post-1997 beneficiaries on the 2nd/3rd/4th Wednesday (holiday -> prior business day), ~$80-120bn/month;
part could be invested via brokerage/retirement accounts. Calendar-known, so positions set at the prior close (lag 0).
3rd-of-month cohort skipped (inside turn-of-month window, 110 dead); distinct from 165 (16th payday, dead).

Events: 540 SS payment days train (2000-14), 288 valid (2015-22).

Descriptive mean SPY close->close (≤2022): train SS days −6.8bp vs other Wednesdays +18.7bp vs all days +2.5bp;
valid +1.2bp vs +28.5bp vs +4.5bp. Following session: +8.2 / +2.2bp.

| Variant | train t | valid t | valid Sharpe | net bp/trade train / valid | gate |
|---|---|---|---|---|---|
| A long SPY on SS days (timing value, 0.5bp) | −1.98 | −0.71 | −0.25 | −10.3 / −4.4 | no |
| B SS day + next session (timing value) | −0.68 | −0.80 | −0.28 | −4.5 / −6.6 | no |
| C long SS Wed / short other Wed | −2.55 | −1.62 | −0.57 | −11.5 / −8.9 | no |
| D placebo BIL on SS days (timing value, 1bp) | −6.48 | −19.33 | −6.84 | −1.7 / −1.9 | no (pure cost drag, as expected) |

Yearly A is noisy (big negatives 2000, 2003, 2008, 2011, 2020). Wrong sign in both periods: SS days are, if anything,
weaker than other Wednesdays (the 1st Wednesday sits in the turn-of-month window and often near FOMC, which likely
explains the "other Wed" strength, not an SS effect). With N tests, |t|≈2.5 for the best of many is luck-level.

**Verdict: dead.** No equity footprint of Social Security payment flows.
