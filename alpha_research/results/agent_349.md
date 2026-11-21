# 349 — BCOM/GSCI January reweight: long last year's commodity losers, short the winners (card X1)

Script: `experiments/349_commodity_index_reweight.py`. Registry: `results/registry_agent_349.csv`. Events: `results/349_events.csv`.

**Setup.** The universe is USO, UNG, GLD, SLV, DBB, DBA, CORN, WEAT, SOYB and CPER. Each asset is used only once it has
a full look-back, so the universe is survivorship-free; there are 5-10 names per event. The signal is the prior
calendar-year return, known on Dec 31. The rule goes long the bottom k and short the top k (k=min(3,n/2)),
dollar-neutral with gross exposure 2. Positions run from the close of January bd4 to the close of bd10, using
`lib.backtest_weights(lag=1)` and 2bp/side. There are 3 pre-specified variants. The first run dropped the three
19-trading-day Januaries (2010/16/21) through a length-filter bug. Those 3 registry rows are superseded by the "[fix]"
rows (6 registered in total).

| Variant | train t | valid t | valid Sharpe | valid bp/day held | events tr/va | mean net bp/event tr / va | gate |
|---|---|---|---|---|---|---|---|
| V1 Jan bd4→10 L losers/S winners | -1.63 | -0.07 | -0.02 | -2.0 | 7 / 8 | -150 / +19 | no |
| V2 placebo, Feb-Dec bd4→10 (trailing 12m) | -0.97 | +0.18 | +0.06 | +1.2 | 77 / 88 | -41 / +11 | no |
| V3 reversal, Jan bd10→20 (opposite of V1) | +1.77 | +0.53 | +0.19 | +10.8 | 6 / 6 | +402 / +103 | no |

**Per-year V1 net bp:** 2008 +6, 2009 -648, 2010 -344, 2011 +179, 2012 -80, 2013 +36, 2014 -197, 2015 +351,
2016 -632, 2017 -131, 2018 +230, 2019 -347, 2020 +282, 2021 +815, 2022 -417. The hit rate is 7/15. The mean rank-IC
is +0.07 in train and +0.09 in valid (descriptive; noisy, roughly ±0.5 per year).

**Notes.**
- In the January window, the per-event dispersion of ±300-800bp swamps any plausible reweight footprint. The roughly
  $100bn of tracker flow is spread over 5 days and pre-announced, so it is easy to front-run or absorb.
- The January window looks no different from the placebo months, which is consistent with the academic event study
  (it found effects in OI/volume, not prices).
- V3's train t of 1.77 rests on 2009 (+1777bp) and 2014 (+876bp), and on its own V3 is just a cross-sectional
  reversal. It was not pursued.
- With N=6 tests here, luck alone would give a best t of about 1.9.
- Ruled out: the WEAT, SOYB and CPER listings start in 2011, so the cross-section is thin before 2011.

**Verdict: dead.** No variant passes `lib.passes_gate`, and the sign is wrong in train for the main variant.
Possible follow-up (low priority): X1(e), the announced BCOM weight change versus drift, using futures rather than
ETFs.
