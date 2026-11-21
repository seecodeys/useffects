# 327 — Holiday-shortened weeks and long-weekend inventory flows

Script: `experiments/327_holiday_weeks.py`. Registry: `results/registry_agent_327.csv` (4 rows). Data stops at 2022-12-31; no holdout look.

Events: scheduled holiday = weekday with no SPY bar that `signals.nyse_closed` flags. This drops 9/11, Sandy and the
helper's pre-2022 Juneteenth false positives. All trades are known from the calendar ahead of time, so lag=0.
Variants and signs were fixed before the run. The close->next-open legs of (b) were dropped to stay within 4 variants.

| Variant | Events train/valid | Train Sh / t | Valid Sh / t | Valid bp/trade | Timing-value t (train / valid) | Verdict |
|---|---|---|---|---|---|---|
| A long SPY cc, all sessions of holiday weeks, 0.5bp | 133 / 71 weeks | +0.36 / +1.38 | +0.90 / +2.54 | +2.28 (per day) | +0.98 / +1.94 | weak (train t fails) |
| B long SPY open->close, session after holiday break, 0.5bp | 132 / 72 | +0.18 / +0.68 | +0.04 / +0.12 | +1.13 | +0.70 / -0.03 | dead |
| C short TLT open->close, session after holiday break, 1bp | 109 / 72 | -0.00 / -0.01 | -0.54 / -1.52 | -9.69 | +0.30 / -1.48 | dead |
| D short SPY open->close, last session before long weekend, 0.5bp | 95 / 57 | +0.14 / +0.53 | -0.57 / -1.62 | -13.80 | +0.53 / -1.47 | dead (sign flips) |

Timing value = net P&L minus (mean exposure x the same return series). It compares each variant with an exposure-matched long.

Notes: A is mostly market beta, since it is long about 18% of days. Its edge over the matched long is not significant in train
(t 0.98) and only borderline in valid (t 1.94). 2020 alone contributes +1776bp. With about 454 registered tests across the
repo's registries, the luck bar sqrt(2 ln N) is about 3.5. Nothing passes `lib.passes_gate`. **Verdict: dead overall.** A is
at most weak: holiday weeks earn slightly more than average in 2015-22, which fits the known pre-holiday drift (026/163).
It is not a new flow effect.
