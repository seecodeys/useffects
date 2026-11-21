# 331 — Auction takedown: dealer share at 10y/30y auctions → IEF/TLT (idea card B5)

**What.** Nominal 10y (→IEF) and 30y (→TLT) auctions, new issues and reopenings, from fiscaldata `auctions_query`
(cached in `data/auct331/`; only auctions up to 2022-12-31 were downloaded). Dealer share = primary_dealer_accepted /
comp_accepted. Surprise = value minus the same tenor's mean over its previous 6 auctions. Hypothesis direction was fixed
in advance: stuffed dealers (share surprise > +5pp) → short; very low dealer share → long. Entry is at the auction-day
close, after the ~13:00 ET result. Cost is 1bp/side, turnover-based. Script: `experiments/331_auction_dealer_takedown.py`.
Registry: `results/registry_agent_331.csv` (4 rows). Nothing from 2023+ was examined and the holdout was not touched.

**Variants** (pre-specified in the docstring). Dealer variants: train 2008-05→2014, valid 2015-22. Bid-to-cover: train 2002-07→2014.

| Variant | events tr / va (short/long) | train t | valid t | valid Sharpe | timing t (tr / va) | gate |
|---|---|---|---|---|---|---|
| A dealer surprise ±5pp, close d→d+3 | 30/44 · 25/43 | +0.21 | −1.10 | −0.39 | +0.10 / −1.11 | no |
| B as A, close d→d+5 | same | −0.04 | −1.83 | −0.65 | −0.19 / −1.85 | no |
| C bid-to-cover surprise ±0.2, close d→d+3 | 39/52 · 16/13 | +0.90 | +0.67 | +0.24 | +0.80 / +0.68 | no |
| D as A, lag 1 (close d+1→d+4) | same as A | +0.39 | −1.17 | −0.42 | +0.28 / −1.19 | no |

**Descriptive** (d+1..d+3 ETF bp by dealer-share-surprise tercile; cuts are from 2008-14). For 2008-14, low/mid/high:
+29.9 / +36.6 / +20.2. For 2015-22: −5.7 / −16.3 / −1.8. There is no monotone pattern. In valid, the auctions where
dealers were stuffed did slightly *better* than the rest, so the sign goes against the hypothesis.

**Notes.** Dealer share falls steadily, from ~0.55 in 2009 to ~0.15 in 2022. The trailing-6 surprise removes most of
that trend. The dealer variants have the wrong sign in valid, and B (longer hold) has the most negative result. That fits
the known post-auction *recovery* (133b) better than continued dealer distribution. Bid-to-cover (C) has the right sign
in both periods but is far from significant (t < 1). Luck-level t for 4 tests is ~1.7. Measured against the main
registry's N, it is ~3.5.

**Verdict: dead.** No variant passes `lib.passes_gate`. Dealer takedown share does not predict IEF/TLT over the next 3-5 days.
