# 380 — Weekend risk reduction (Friday-afternoon de-risking / Monday re-risking)

Script: `experiments/380_weekend_derisk.py` · registry: `results/registry_agent_380.csv` (6 rows: 4 candidates + 2
controls) · no holdout look, nothing from 2023+ was loaded or printed.

**Hypothesis.** Gamma and gap risk over the untradeable weekend make dealers and leveraged traders cut gross exposure into
the Friday close and add it back on Monday. If positioning follows the recent trend, Friday-afternoon de-risking trades
against the week's move and Monday-morning re-risking trades with it.

**What is new vs LOG.md.** 001 tested weekday open→close longs, 288-290 intraday patterns on all days, 311 a short on
non-OPEX Fridays 10:30-15:00, 327D a short SPY on the Friday before long weekends, and 057 Monday after a Friday drop. This
experiment adds the Friday last two hours, conditioned on the week's move, and conditional Monday re-risking.

**Design.** "Big week" means |week move| is above the 67th percentile of the previous 52 weeks of the same measure
(trailing). s = sign(week move) on big weeks, 0 otherwise. CFD = 50/50 SPX + NSX 1-min (HistData, NY local time), with
2010-14 as train (first ~6 months used to warm up the threshold) and 2015-22 as valid. SPY daily data covers 2000-22. All
positions are intraday or set by the calendar or by data known before entry. Costs are 0.5bp/side.

| Variant | train t | valid t | valid Sharpe | net bp/trade train / valid | events tr / va | gate |
|---|---|---|---|---|---|---|
| A short every Fri 14:00→16:00 (CFD) | +1.03 | **−2.72** | −0.96 | +2.4 / −6.7 | 202 / 379 | no |
| B fade big week, Fri 14:00→16:00 (CFD) | **−2.60** | −1.51 | −0.53 | −15.2 / −8.3 | 52 / 135 | no |
| C follow big prior week, Mon 09:30→11:00 (CFD) | +1.48 | +0.76 | +0.27 | +12.5 / +3.7 | 50 / 123 | no |
| D SPY: fade wk Fri open→close + follow Fri close→Mon open | −1.19 | −0.16 | −0.06 | −11.2 / −2.7 | 219 / 127 | no |
| A0 control: short Mon-Thu 14:00→16:00 | −0.74 | −0.51 | −0.18 | −1.1 / −0.7 | 822 / 1542 | — |
| B0 control: fade big 5-day move, Mon-Thu 14:00→16:00 | +0.28 | +0.99 | +0.35 | +0.9 / +2.9 | 263 / 551 | — |

Descriptive gross bp (t), train | valid:
- SPX/NSX Friday 14-16, all Fridays: −3.4 (−1.4) | +5.7 (+2.3). Mon-Thu 14-16: +0.1 | −0.3.
- Friday 14-16 after big UP weeks: +5.1 (+1.1, n=29) | +11.1 (+3.4, n=78). After big DOWN weeks: −25.8 (−2.4, n=23) |
  −2.0 (−0.2, n=57).
- SPY Friday close→Monday open on plain weekends: +3.7 (+1.3) | −2.2 (−0.4), against all other overnights +2.5 (+2.1) |
  +4.3 (+2.5). The weekend gap has no timing value over an ordinary overnight hold.
- D legs: −s × Friday open→close +2.4 | −14.7; +s × weekend gap −11.6 | +13.9. The signs flip between periods.

**Reading.**
- The de-risking story predicts the wrong sign. In both periods the Friday last two hours continue the week's move
  (B is negative in both: t −2.6 and −1.5). They do not fade it.
- The B0 control on other weekdays shows nothing. So the continuation is a Friday-specific effect, but it is not
  significant in valid. It is also concentrated after big up weeks in valid (+11bp, t 3.4) and after big down weeks in
  train (−26bp).
- Unconditional Friday afternoons flip sign between periods (A): weak in 2010-14, a rally in 2015-22.
- Monday re-risking (C) has the right sign but is not significant. 2011 alone is +817bp of its +1,099bp.
- Flipping B to "follow the week on Friday afternoon" would be post hoc. It would give +2.6 / +1.5, which fails the gate,
  and it should not be read as a finding. With N ≈ 715 registered tests, luck alone produces a best |t| of about
  sqrt(2·ln N) ≈ 3.6.

**Verdict: dead.** No variant passes `lib.passes_gate`. The pre-specified de-risking direction (B, D) is wrong-signed or
flat. The only hint is that Friday afternoons continue the week's move, which is the opposite of the story and below the
significance bar. If anyone follows it up, it needs fresh pre-registration, ideally on 2000-09 ES futures data.
