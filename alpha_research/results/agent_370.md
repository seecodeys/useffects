# 370 — Treasury TIC monthly foreign flows → IEF / TLT / SPY-vs-EFA (data source 3)

Script: `experiments/370_tic_foreign_flows.py` (output `results/370_output.txt`, summary `results/370_summary.csv`,
registry `results/registry_agent_370.csv`, N=4).

**Data.** TIC S-form only (one consistent concept; SLT not used): `s1_globl.txt` (gross purchases/sales by country,
1977-01..2023-01; net = purchases − sales) and `tressect.txt` (net Treasury bond/note purchases split foreign official /
other foreigners, 1978-01..2023-01). Both from ticdata.treasury.gov, cached in `data/tic/`. The files hold revised
figures, not first-release vintages (a small look-ahead caveat).

**Release lag.** TIC for month m comes out around the 15th–18th of m+2. Month-m data is first used at the close of the
last trading day of m+2, which is at least about 10 days after release. With `backtest_weights lag=1`, the position
starts at the close of the first trading day of m+3 and is held for one month.

**Signal.** Take the 3-month sum of the net flow and z-score it against the prior 36 values of that sum. A trade fires
when |z| > 1. Costs are 1bp/side for IEF/TLT, 0.5bp for SPY and 2bp for EFA. Because the ETFs start in 2002-07, train
runs 2002-08..2014 and valid runs 2015-22.

| Variant | sig. months tr/va | train t | valid t | valid Sharpe | valid bp/unit/day | net bp/month tr / va | timing-value t tr / va | gate |
|---|---|---|---|---|---|---|---|---|
| A official UST flow z → ±IEF | 87/49 | +0.14 | −1.18 | −0.42 | −1.4 | +2 / −23 | −0.15 / −1.11 | no |
| B private UST flow z → ±IEF | 95/47 | +1.46 | −0.40 | −0.14 | −0.7 | +27 / −10 | +0.97 / −0.43 | no |
| C Caribbean UST build-up z>1 → short TLT | 27/30 | −1.64 | +1.01 | +0.36 | +4.8 | −121 / +81 | −0.92 / +1.04 | no |
| D foreign US-stock buying z → ±(SPY−EFA) | 89/66 | −0.62 | −0.24 | −0.08 | −0.4 | −19 / −7 | −0.58 / −0.24 | no |

**Notes.**
- No variant has t ≥ 2 in either period. With N=4, the best-of-N luck t is about 1.67.
- B's train result depends on 2008 (+1824bp) and 2010. Its sign flips in valid, and 2022 alone is −879bp.
- C has the wrong sign in train (the 2008 basis unwind came after Caribbean selling, not after buying). Its valid gain is
  mostly 2022 (+2494bp). This is the same 2022-only pattern as 341.
- A (official flows) gives nothing in train and is negative in valid. This is consistent with 341 (custody) and 348 (MOF):
  official and foreign flows show up in holdings, but they do not predict the next month's prices.
- D: foreign equity flows have no trend-chasing or price-pressure content for the SPY-vs-EFA spread.

**Verdict: dead.** Nothing passes `lib.passes_gate`, and no train/valid signs agree except trivially. The holdout was not touched.
