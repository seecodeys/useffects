# 352 — Implied correlation (Cboe COR3M) / correlation risk premium → SPY and put-write (card 4, ideas_options_etf.md)

Script: `experiments/352_implied_corr_premium.py`. Registry: `results/registry_agent_352.csv`. Output: `results/352_output.txt`.

**Data.** Cboe COR3M (2006-01-03+, close; Cboe launched it in 2022, so 2006-21 is Cboe's backfill — flag). COR1M/6M/9M/10D/30D/70D
also exist (2006+). The older KCJ/ICJ/JCJ files return 403 AccessDenied (not bypassed). Realized corr = trailing 63-day average
pairwise correlation of the 9 sector SPDRs (×100). CRP = COR3M − realized corr. PUT = CBOE PUT index (real prices). Excess over ^IRX.

**Setup.** Monthly decision at the last close of the month using that close's values; `lib.backtest_weights(lag=1)` (enter at the
next close, hold ~1 month). Top-tercile cut-offs fixed on TRAIN month-ends 2006-14 (COR3M 57.17, CRP −11.82). Costs 0.5bp/side SPY,
2bp/side PUT. Each registered series is the **timing value**: strategy net excess minus (average exposure in the period) × always-long
excess return. 4 pre-specified variants, all registered.

| Variant | train t | valid t | valid Sharpe | events tr / va | mean net excess bp/event tr / va | gate |
|---|---|---|---|---|---|---|
| A SPY long if COR3M top tercile (monthly) | +1.11 | +1.99 | +0.70 | 36 / 3 | +174 / +909 | no |
| B SPY long if CRP top tercile (monthly) | +1.35 | +0.41 | +0.14 | 36 / 41 | +201 / +111 | no |
| C PUT long if CRP top tercile (monthly) | +1.05 | +0.47 | +0.17 | 36 / 41 | +119 / +68 | no |
| D SPY long if COR3M top tercile (quarterly, 3m hold) | +1.24 | +1.51 | +0.54 | 10 / 1 | +572 / +2476 | no |

(valid bp/day of timing value: A +1.23, B +0.50, C +0.43, D +1.10.)

**Notes.**
- A and D's valid numbers are not evidence: COR3M's level fell after 2014, so the train threshold fired only 3 times (1 for D) in
  2015-22, and the valid timing value is essentially the April-2020 month (+2170bp of A's valid total; other years within ±80bp).
- Train timing value is concentrated in 2008-12 (post-crisis rebound buying when correlation was high); 2006-07 and 2013-14 negative.
- The CRP variants (B, C), which are stationary and fire ~40% of months in valid, show positive but weak timing value (t ~0.4-0.5);
  yearly P&L flips sign often. Put-writing does not visibly earn a correlation premium beyond its unconditional return.
- With N=4 here, luck gives a best t ≈ 1.7; nothing clears that in both periods.

**Verdict: dead / weak.** No variant passes `lib.passes_gate`. Signs agree train/valid but magnitudes are small and episode-driven
(2008-12, 2020). Possible follow-up (low priority): a rank/z-scored COR3M (rolling percentile) to remove the level drift — would be a
new pre-specified test, not a re-tune.
