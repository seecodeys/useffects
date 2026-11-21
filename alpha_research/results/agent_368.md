# 368 — Treasury auction allotments by investor class (non-dealer classes) → IEF / TLT / SHY

**Data.** Treasury investor-class coupon allotments (data_sources_new.md item 4). The current file was scraped from
the index page (`November-9_2026-IC-Coupons.xls`, issues 2009-10+) and joined to the history file
(`Website-PDO-4-A-Coupons-Jan-2000-Sep 2009.xls`). Both are cached in `data/ic368/`. Nominal 10-Year Notes map to IEF
and 30-Year Bonds to TLT. TIPS, FRNs and the 20y are excluded. Only issue dates ≤2022-12-31 are used.
Script: `experiments/368_auction_investor_class.py`. Registry: `results/registry_agent_368.csv` (4 rows).
Summary: `results/368_summary.csv`. Nothing from 2023+ was looked at, and holdout_eval was never called.

**Publication timing.** The page says the data are released at 15:00 ET on about 2 dates a month. Old real-time
vintages do not exist, so I used conservative availability rules:
- Issue month m (2009-10+): public by the end of month m+1.
- Before 2009-10: the table was published only as PDO-4 in the quarterly Treasury Bulletin. Public by the end of the 3rd
  month after the issue quarter.

The decision is made at the first close on or after that date, and the position starts one day later (lag 1). Costs
are 1bp/side, turnover-based. This makes it a slow signal, entered 1–6 months after the auction. A "days after the
auction" trade is not possible with this release schedule.

**How it differs from 331.** Every share is computed over end-investor allotments only, with
EI = total − SOMA − dealers&brokers. That removes dealer takedown by construction. Dealer share is never used. The fund
and foreign z-surprises correlate only +0.15 and −0.16 with the dealer-share z.

**Variants** (pre-specified; z = surprise vs the same tenor's previous 8 auctions; train 2002-07→2014, valid 2015-22)

| Variant | events tr / va | train t | valid t | valid Sh | valid bp/held-day | bp/decision tr / va | timing t tr / va | gate |
|---|---|---|---|---|---|---|---|---|
| A fund share z>±1 → long/short tenor ETF, 10d | 40L/34S · 34L/28S | −0.05 | −0.82 | −0.29 | −2.9 | −2.2 / −25.0 | −0.20 / −0.86 | no |
| B foreign share z>±1, 10d | 35L/40S · 28L/35S | −0.56 | +0.46 | +0.16 | +1.7 | −16.1 / +12.4 | −0.49 / +0.43 | no |
| C H4a: trailing-3m real-money (funds+pension) share vs 24m avg → ±(IEF − 0.45 TLT), monthly | 82 / 95 months | −1.04 | +0.63 | +0.22 | +0.2 | −11.7 / +5.0 | −0.39 / +0.67 | no |
| D H4b: foreign share fell 3 months running → −TLT +SHY, 1 month | 9 / 11 | +0.90 | +0.29 | +0.10 | +1.4 | +113 / +28 | +1.47 / +0.26 | no |

**Notes.** No variant gets |t| above 1.1 in both periods, and the signs flip between train and valid for B and C. D is
the only variant with a consistent sign, but it rests on 9 and 11 events, and the valid t is 0.29. For 4 tests, the t
that luck alone produces is about 1.7. The 2–6 month publication lag probably makes the information stale.

**Verdict: dead.** No variant passes `lib.passes_gate`.
