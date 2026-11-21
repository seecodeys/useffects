# 362 - Agency-MBS P&I payment-day reinvestment (card S2)

Script: `experiments/362_mbs_pi_reinvest.py`. Registry: `results/registry_agent_362.csv` (4 rows). The data runs from MBB's
2007 start through 2022; no 2023+ data was looked at. The trades are close->close with 1bp/side per leg. The signal is a
pure calendar rule, so it uses **lag 0**: the windows are known in advance. Any window that holds a return day among the
last 3 sessions of the month is dropped, so this test does not re-measure the month-end bond bid (LOG 275-319). The hedge
beta is a 60-session OLS slope of MBB on IEF, measured up to the entry close.

| Variant (pre-specified) | n train / valid | train t | valid t | valid Sharpe | valid net bp/trade | gate |
|---|---|---|---|---|---|---|
| V1 MBB long, P25-1 -> P25+2 | 0 / 0 | - | - | - | - | n/a |
| V2 MBB-beta*IEF, P25-1 -> P25+2 | 0 / 0 | - | - | - | - | n/a |
| V3 MBB long, P25-4 -> P25-1 (pre-positioning) | 92 / 93 | -1.46 | +0.92 | +0.33 | +4.40 | fail |
| V4 MBB-beta*IEF, P20-1 -> P20+2 (Ginnie) | 89 / 94 | -0.45 | -1.16 | -0.41 | -3.34 | fail |

**V1 and V2 have no events.** In 2007-2022 the first session on or after the 25th always falls 2-5 sessions before
month-end: the counts by sessions-to-month-end are 2:5, 3:13, 4:78 and 5:94. P+2 therefore always lands inside the last
3 sessions. With the month-end exclusion the card requires, the post-25th reinvestment window cannot be told apart from
the known month-end bond bid. It is untestable as specified.

**Timing value against exposure-matched control days:**
- V3: train is -5.3bp per trade against a +5.4bp 3-day control. Valid is +4.4bp against -4.2bp. The sign flips between
  periods, and the valid result rests on 2019-2020 (+161 and +297bp).
- V4: the timing value is about 0 in both periods.

With 4 tests, luck alone would produce a best t of about 1.67. No variant reaches that.

**Verdict: dead.** Nothing passes `lib.passes_gate`, and no holdout look was used. The Fannie/Freddie 25th effect
overlaps the month-end bid by construction. The Ginnie 20th window and the pre-25th window show nothing.
