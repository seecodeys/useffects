# 383 — SPY dividend-payment reinvestment (post-pay turn of month)

**Story.** SPY pays its quarterly dividend on the last business day of Jan/Apr/Jul/Oct (SPDR S&P 500 ETF Trust
prospectus, SEC filing 497 of 2018, sec.gov/Archives/edgar/data/884394/000119312518014271). The ex-date is the 3rd Friday
of Mar/Jun/Sep/Dec: Yahoo events=div shows 133 of 136 ex-dates on a Friday. DRIP or rebalancing reinvestment should bid
SPY on P+1..P+3, the first 3 sessions of Feb/May/Aug/Nov. Those sessions overlap the turn of the month (110) and
month-end (013), so the dividend part is isolated with a diff-in-diff against the other 8 months.
Script: experiments/383_spy_dividend_reinvest.py. The test is SPY close→close, calendar-known (lag 0), 0.5bp/side.
Registry: results/registry_agent_383.csv (4 tests).

**Descriptive mean SPY cc, ≤2022.**
| | post-pay TOM1-3 | other TOM1-3 | pay day P | other month-ends |
|---|---|---|---|---|
| train 2000-14 | +6.4bp (180) | +10.3bp (360) | −5.8bp (60) | +3.8bp (120) |
| valid 2015-22 | +6.5bp (96) | +13.8bp (192) | −23.7bp (32) | −8.4bp (64) |

The post-pay turn of month is WEAKER than ordinary turns of month, which is the wrong sign.

| Variant | train t | valid t | valid Sharpe | net bp/event (train, valid) | gate |
|---|---|---|---|---|---|
| A long P+1..P+3, timing value (60/32 events) | +0.42 | +0.16 | +0.06 | +10.9, +5.1 per 3-day block | no |
| B DiD +1 post-pay TOM / −0.5 other TOM | −0.41 | −0.57 | −0.20 | −4.5, −7.9 per month | no |
| C DiD +1 pay day / −0.5 other month-ends | −0.71 | −0.75 | −0.26 | −3.9, −5.8 per month | no |
| D placebo DiD pre-pay TOM (Jan/Apr/Jul/Oct) | +0.63 | +0.52 | +0.18 | +7.4, +7.4 per month | no |

**Verdict: dead.** The SPY pay date leaves no detectable reinvestment signature. Variant A's small positive is just the
generic turn of month, and the DiD isolating the dividend part (B) has the wrong sign in both periods. The pay day itself
(C) is also wrong-signed relative to other month-ends. The placebo (D) is as large as anything else, so this is noise.
The yearly P&L for A/B flips sign often, with no trend. Likely reasons: SPY's payment (~0.3-0.4% of AUM) is spread
across brokers' DRIP timing, and the cash is small against turn-of-month flows overall.
**LOG 114 (Hartzmark-Solomon aggregate dividend price pressure):** this tests the one large payment whose date is exactly
known. It comes out negative, so 114 stays parked; a fair test still needs aggregate constituent pay dates (not free).
Luck benchmark for N=4 is t≈1.7, and nothing comes near it.
