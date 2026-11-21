# 328 - Mega-cap earnings clusters / buyback blackouts

**Verdict: dead (A weak at best, not gate-passed).** Registry: `results/registry_agent_328.csv` (4 tests).

## Data
SEC EDGAR submissions JSON, 8-K filings with Item 2.02, for 15 long-lived mega-caps (hindsight universe):
AAPL MSFT AMZN GOOGL(Google Inc + Alphabet CIKs) XOM GE JNJ JPM WMT PG CVX IBM PFE INTC CSCO
(`tools/edgar_earnings.py`, cached `data/edgar_202_*.csv`). ~4 filings/yr per name; extras (CVX/INTC/IBM interim
updates ~E+8d, XOM late filings) removed by taking the last 2.02 filing in (quarter-end+10d, +55d]. All 72 quarters
2005Q1-2022Q3 had >= 14 of 15 names. Majority (8th report) date: median 25 days after quarter-end (range 19-31).
Dates look reliable (Item 2.02 filing = release day; after-close filings date-stamped same day).

## Pre-specified variants (close->close, 0.5bp/side; train 2005-14 / valid 2015-22)
| Variant | train Sh / t | valid Sh / t | valid bp/day |
|---|---|---|---|
| A long SPY 5 sessions after cluster week, lag-1 | +0.42 / +1.33 | +0.76 / +2.13 | +1.33 |
| B SPY after minus before (lag-0 best-case) | +0.13 / +0.40 | +0.11 / +0.32 | +0.28 |
| C QQQ-SPY after, lag-1 | +0.19 / +0.60 | -0.34 / -0.96 | -0.36 |
| D QQQ-SPY after minus before (lag-0) | +0.15 / +0.47 | -0.30 / -0.85 | -0.41 |

A timing value vs unconditional SPY drift: t +0.91 (train) / +1.63 (valid). A's P&L is dominated by 2008 (+1917bp)
and 2020 (+1555bp); most of it is just being long SPY ~6% of days. The after-minus-before contrast (B) - the actual
blackout-release test - is ~zero in both periods; the QQQ tilt (C, D) flips sign. Fails the gate (A train t < 2,
timing t < 2, year-concentrated). Consistent with 113: no tradeable buyback-blackout timing at index level.
