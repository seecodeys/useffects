# Idea scout: 8 extensions of the two month-end mechanisms (2026-11-21, NOT tested)

Scope: take the two robust findings, 013 (month-end stock/bond rebalancing on the last day) and the month-end
bond-index extension bid (275-286, 298-319), and test them where the same participants should leave a footprint, or
where the mechanism predicts NO footprint (a falsification). Each card is pre-specified for PROTOCOL.md periods
(TRAIN 2000-14, VALID 2015-22, HOLDOUT 2023+ untouched). Where an ETF starts after ~2008, train = first full year→2014.
Default window "W3" = enter at the close of the 4th-last session, exit at the close of the last session of the month
(as in 275/276/310). Costs: 1bp/side for liquid bond ETFs, 2-3bp/side for the thinner ones below.

Duplicate check (grep '^## ' LOG.md, results/ideas_*.md, data_sources_new.md): already logged and NOT repeated here:
TLT/IEF/SHY/TIP/MBB/MUB/LQD/HYG/EMB/BWX, Canada XBB, UK IGLT, euro EUNH bond bids (275-284); GLD/SPY/UUP/XLU/VNQ
controls (285, 307); bills vs notes (318); issuance dose-response (298); asset-manager futures length (300);
SPY−EFA, EFA/EEM vs TLT, sector/size/country month-end rotation (016/017, 274); gold risk parity (226); TLT−IEF
(256); TDF first-days lag (213); quarterly/annual/weekly/threshold rebalancing (184, 250, 253); MSCI reviews (178);
quad witching (180); Treasury-futures roll (361); bond-ETF ex-dates (363); TIPS seasonal accrual (337).

---

## E1. FX-hedged and local-market bond-index bid outside the US/UK/Canada/euro (BNDX, Australia, Japan, EM local)

**Mechanism.** Global Aggregate ex-USD, Australian AusBond and JGB (NOMURA-BPI) benchmarks all extend duration at
month-end, so benchmarked managers buy then. 284 tested BWX unhedged, where FX noise swamped the bond leg; a hedged or
local-currency instrument isolates the bond price.
**Source.** Month-end extension projections are published for the Euro Treasury, Gilt and other indices (MNI, "Citi flag
slightly larger than normal EGB extension": https://www.mnimarkets.com/articles/citi-flag-slightly-larger-than-normal-egb-extension-small-gilt-contraction).
The NY Fed has documented the US month-end pattern: https://libertystreeteconomics.newyorkfed.org/2024/09/end-of-month-liquidity-in-the-treasury-market/
**Test (pre-specified).** Long over W3 each month (local trading calendar, local month-end). Signal: calendar only.
- V1 BNDX (Vanguard Total Intl Bond, USD-hedged; Jun 2013+). Train 2014, valid 2015-22.
- V2 IAF.AX (iShares Core Composite Bond, Australia; local AUD returns).
- V3 2510.T (NEXT FUNDS NOMURA-BPI; check the listing date, likely about 2017, so valid only).
- V4 EMLC − β·CEW (EM local-currency bonds with the FX basket removed; β is the train-period regression of EMLC on CEW
  daily returns; 2010+).
Report t, Sharpe, timing value versus always-long, and a pooled V1-V3 t.
**Data.** Yahoo daily: BNDX, IAF.AX, 2510.T, EMLC, CEW.
**Why new.** Tests 277/278/284 used unhedged BWX and EMB, which is USD sovereign debt rather than local. No hedged
global-aggregate, Australian, Japanese or EM-local bond-bid test exists. (257/258 used IAF.AX and 2510.T only as the
bond leg of 013.)

## E2. Index-eligibility natural experiments: STRIPS and leveraged loans (predicted null)

**Mechanism.** Bloomberg's flagship Treasury index excludes STRIPS ("their inclusion would result in double-counting")
and FRNs. Leveraged loans are outside every bond aggregate. If the bid is index extension, a duration-matched
STRIPS-minus-coupon spread and a loans-minus-HY spread should show no bid, or a negative one.
**Source.** Bloomberg US Treasury index page (STRIPS excluded): https://www.bloomberg.com/professional/products/indices/quote/LUATTRUU:IND.
FRN exclusion: https://www.wisdomtree.eu/-/media/eu-media-files/other-documents/index/wt/index-methodology/bloomberg-barclays-us-treasury-floating-rate-bond-index-methodology.pdf
**Test (pre-specified).** Over W3:
- V1 EDV (2008+) − h·TLT, with h the train-period OLS hedge ratio of daily returns. Prediction: the spread is ≤ 0. A
  positive spread with t>2 contradicts the eligibility story.
- V2 ZROZ (2009+) − h·TLT, same rule.
- V3 BKLN (2011+) − h·HYG. Prediction ≤ 0, while HYG alone is > 0 (277).
- V4 bid per duration-year: W3 return of EDV divided by its duration, versus TLT's (descriptive; ratio < 1 expected).
Verdict rule set in advance: the null "is consistent" if V1/V2/V3 have |t| < 1.5 or a negative sign in both periods.
**Data.** Yahoo daily EDV, ZROZ, TLT, BKLN, HYG.
**Why new.** 318 (bills vs notes) is the only eligibility control so far, and it is a maturity cut. This one tests
security-type eligibility at the same duration or credit risk. Not in LOG.

## E3. Hybrid benchmarks: convertibles (CWB) and preferreds (PFF), equity/rate beta removed

**Mechanism.** Convertible (Bloomberg US Convertibles Liquid) and preferred (ICE exchange-listed preferred & hybrid)
indices are tracked by dedicated funds that reinvest and rebalance monthly. Neither is in the Aggregate. A bid here
would mean that "benchmarked month-end cash deployment" is broader than Aggregate extension. No bid would sharpen the
index-extension reading.
**Source.** CWB tracks the Bloomberg US Convertible Liquid Bond Index: https://ssga.com/us/en/etfs/spdr-bloomberg-convertible-securities-etf-cwb.
Bloomberg rebalancing framework: https://data.bloomberglp.com/professional/sites/10/Bloomberg-Index-Publications-Fixed-Income-Index-Methodology.pdf
(rebalance day: the tester confirms the monthly schedule in the methodology).
**Test (pre-specified).** Over W3, residual return = ETF − b1·SPY − b2·IEF, with betas estimated on train-period
daily data and frozen:
- V1 CWB residual (2009+).
- V2 PFF residual (2007+).
- V3 raw CWB and PFF, timing value versus always-long.
- V4 control: the same residual over a random 3-session window (permutation, as in 276e).
**Data.** Yahoo daily CWB, PFF, SPY, IEF.
**Why new.** No convertible or preferred test exists in LOG or in the idea files. The 307 bond proxies (XLU, VNQ) are
equities.

## E4. TIPS-specific extension: breakeven bid that scales with new-TIPS months

**Mechanism.** The TIPS index (Bloomberg US TIPS) extends mainly in months when a new or reopened TIPS settles, which
the auction calendar makes known in advance. Benchmarked TIPS managers then buy TIPS rather than nominals, so the
10y breakeven (TIPS rich vs nominals) should widen into month-end, and more so in TIPS-settlement months. 298 found no
dose-response for total coupon issuance. A segment-specific index should show one if extension drives the bid.
**Source.** Bloomberg month-end extension estimates list TIPS separately from Treasuries (e.g. the summary at
https://www.mortgagenewsdaily.com/markets/57acd0e7f719ce832c9cf476). The extension mechanism is described at
https://amp.mortgagenewsdaily.com/article/663851
**Test (pre-specified).** Treasury fiscaldata auctions give TIPS issue dates, so a month is flagged "TIPS month" when a
TIPS issue settles in it (known before W3).
- V1 W3 change in FRED T10YIE (10y breakeven, 2003+), unconditional. Prediction: rises.
- V2 the same, TIPS months vs other months (difference-of-means t).
- V3 tradable form: TIP − h·IEF over W3 (h = train-period hedge ratio), TIPS months only.
- V4 duration-matched index version: DFII10 decline minus DGS10 decline over W3.
**Data.** FRED T10YIE, DFII10, DGS10; fiscaldata auctions_query (security_type TIPS); Yahoo TIP, IEF.
**Why new.** 280 tested TIP outright (which includes the nominal bid), 337 tested CPI accrual seasonality and 298 used
all-coupon issuance. No test of the TIPS-vs-nominal bid or of TIPS-specific supply exists.

## E5. Fallen angels around month-end IG-index exits (ANGL vs HYG)

**Mechanism.** A bond downgraded below IG leaves IG indices at the next month-end rebalance. IG holders sell into that
date, and HY/fallen-angel trackers can only buy after it, so fallen angels should be cheap into month-end and rebound
afterwards. The footprint should grow with the number of fresh downgrades (2008-09, 2015-16 energy, 2020).
**Source.** CFA Institute digest: "When the benchmark index is rebalanced at the end of the month, the price of the
fallen angels bounces back": https://rpc.cfainstitute.org/research/cfa-digest/2015/01/the-impact-of-fallen-angels-on-investment-grade-corporate-bonds-portfolios-evidence-from-the-eu.
Index entry/exit mechanics: https://www.vaneck.com/row/news-and-insights/blogs/income-investing/fallen-angels-new-downgrades-end-seven-month-drought
**Test (pre-specified).** Spread S = ANGL − h·HYG (h = train-period hedge ratio; ANGL 2012+, so train is 2012-14 only):
- V1 short S over W3 (pressure into the exit).
- V2 long S over the first 3 sessions of the new month (post-exit rebound).
- V3 V2 only when LQD underperformed IEF by >1% over the month before W3 (credit widening proxy, known in advance;
  pressure is larger after spread widening per the digest).
- V4 LQD − h·IEF over the first 3 sessions (IG relief after forced sales), descriptive.
**Data.** Yahoo ANGL, FALN (2016+, robustness), HYG, LQD, IEF. (FRED ICE BofA indices are truncated to about 3 years
of history, so they are not used.)
**Why new.** 205 (credit outflows after bad weeks) and 167 (HYG−IEF on the 013 signal) are not index-exit timing. The
ideas files have no fallen-angel card.

## E6. Equal-weight drift rebalancing: RSP (and QQQE) quarterly reset vs cap weight

**Mechanism.** At each quarterly reset, equal-weight trackers (RSP about $70bn plus EW sector funds) sell the
quarter's winners (now overweight) and buy losers. When mega-caps beat the average stock quarter-to-date, the reset
sells mega-caps and buys the rest. That is the same "fade the drift" rebalancing as 013, applied inside equities with
a mechanical, dated trigger.
**Source.** S&P DJI EW FAQ (quarterly rebalance, effective after the close of the rebalance date):
https://www.spglobal.com/spdji/en/education/article/sp-500-equal-weight-index-faq/. Reference-date change, Dec 2024:
https://spglobal.com/spdji/en/documents/indexnews/announcements/20241202-1475776/1475776_constituent-weighting-reference-date-update-20241202.pdf
**Test (pre-specified).** Event day D = the EW rebalance effective date (3rd Friday of Mar/Jun/Sep/Dec; confirm in
the methodology PDF). Signal g = QTD(RSP) − QTD(SPY) through the close of D−1 (known before the open).
- V1 position −sign(g) in (RSP − SPY), D open→close. If equal-weight lagged, RSP buys its laggards → long RSP−SPY.
- V2 the same, close(D−1)→close(D).
- V3 the same on QQQE − QQQ (2012+, Nasdaq-100 EW quarterly).
- V4 next-day reversal: +sign(g) in RSP − SPY from close D to close D+1.
About 4 events/yr, so expect low power. Report pooled V1+V3 t.
**Data.** Yahoo RSP (2003+), SPY, QQQE, QQQ.
**Why new.** 180 faded the SPY level after quad witching, and 016a/017a/274 were month-end sector/size/country
rotations. None conditions on the equal-weight vs cap-weight drift or uses the EW reset date.

## E7. Interaction: condition the bond-bid exit on the 013 stock-bond gap

**Mechanism.** On the last day the two flows add up in bonds when stocks led (balanced funds buy bonds plus index
extension) and offset when stocks lagged (funds sell bonds against the extension). 276c found the W3 bid similar in
both states. The last session is where 013 operates, though, so the split should show there and in a combined sizing.
**Source.** Project findings 013/027 (the bond leg carries the signal), 276c and 310. Month-end flow primer:
https://amp.mortgagenewsdaily.com/article/663851
**Test (pre-specified).** gap = SPY MTD − TLT MTD through the close of day −2 (013's definition, one day earlier, so
the decision is made before the last session opens).
- V1 IEF/TLT last-session open→close, long only when gap > 0 (stocks led), flat otherwise. Compare with unconditional
  (031).
- V2 bond-bid exit rule: hold W3 to the last close if gap > 0; exit at the close of day −2 if gap < 0. Compare with
  plain W3 (same months).
- V3 combined last-day book: SPY −sign(gap) at 1 unit plus TLT at +1 unit if gap > 0 or 0 if gap < 0 (versus 013g's
  symmetric ±1 pair).
- V4 the V2 rule on MBB and LQD (other index segments).
**Data.** Yahoo SPY, TLT, IEF, MBB, LQD (already cached).
**Why new.** 013 mirror on TLT and 031 were unconditional or symmetric, and 256 used the TLT−IEF curve. No test makes
the bond-bid exit or the TLT leg asymmetric on the gap sign.

## E8. Futures implementation and the cash-vs-futures split (ZN/ZB/ZF/ZT continuous, Yahoo)

**Mechanism.** Index extension buys cash bonds, and futures follow only through basis arbitrage. If the bid is a cash
benchmark flow, futures should capture less of it per DV01, and the cash-futures basis should richen into month-end.
For implementation, futures allow leverage (279b's 10% vol book) and trade near 24h, so the overnight leg (310) is
included.
**Source.** Bond-futures leverage need: project 279b. Month-end Treasury liquidity/flow evidence:
https://libertystreeteconomics.newyorkfed.org/2024/09/end-of-month-liquidity-in-the-treasury-market/.
Closing-time Treasury trading: https://libertystreeteconomics.newyorkfed.org/2026/09/treasury-trading-at-the-close/
**Test (pre-specified).** Yahoo continuous ZN=F, ZB=F, ZF=F, ZT=F daily closes (2000+). The front-contract roll
falls around first-notice day, which is the last business day of Feb/May/Aug/Nov, inside W3. Pre-registered handling:
exclude those four month-ends, or splice only if the tester can get dated contracts.
- V1 long ZN over W3, non-roll months: t, Sharpe, and bp per DV01 vs IEF over the same months.
- V2 same on ZB vs TLT, ZT vs SHY.
- V3 basis proxy: IEF − h·ZN (h = train hedge ratio) over W3. Prediction > 0, cash richens.
- V4 entry timing: ZN from the 4th-last settlement vs from the 5th-last (does the bid start before the window in
  near-24h trading?).
**Data.** Yahoo ZN=F/ZB=F/ZF=F/ZT=F, IEF/TLT/SHY. Check that Yahoo's futures "close" is the settlement time and note
the 15:00 ET vs 16:00 ETF close mismatch.
**Why new.** 361 tested the roll window itself and 299/300 used CFTC positioning. No test runs the bid on futures
prices or measures the cash-futures split.
