# Idea scout: 10 new market-behaviour ideas (2026-11-21, not tested)

How these were chosen: BACKLOG.md and all `## ` headings in LOG.md (tests 000-325) were checked first. Each idea below
uses a participant and mechanism the project has not tested yet, or adds a condition that earlier unconditional tests
did not use. The "Not a duplicate" line names the closest earlier test. Every rule uses the standard periods:
TRAIN 2000-14, VALID 2015-22, HOLDOUT 2023+ (one look). Where data starts late, this is stated. Lag-1 versions are
required wherever the signal uses a closing price.

---

## B1 — Gotobi-day Tokyo-fix dollar buying (USDJPY)
- **Sources:** Ito & Yamada, "Puzzles in the Forex Tokyo 'Fixing'", NBER w22820, https://www.nber.org/papers/w22820 ;
  desk commentary: https://investinglive.com/news/!/usdjpy-tokyo-fix-gotobi-toshins-repatriations-20120329
- **Participant / trade:** Japanese importers settle dollar invoices on "gotobi" days (the 5th, 10th, 15th, 20th, 25th
  and the month-end). They buy USD at the 09:55 JST bank fixing. Banks pre-hedge, so USDJPY tends to rise into 09:55
  and then fade.
- **Test:** HistData USDJPY 1-min, 2001+ (fixed EST clock: 09:55 JST = 19:55 EST on the previous calendar day). A
  gotobi day is a calendar 5/10/15/20/25/last day. If it falls on a weekend or a Japanese holiday, use the previous
  Tokyo business day. The calendar is known in advance.
  - (a) Long USDJPY 08:00→09:55 JST on gotobi days. Report the timing value against the same window on non-gotobi days.
  - (b) Short USDJPY 09:55→11:00 JST on gotobi days (post-fix fade).
  - (c) As (a), but only on the 25th and month-end gotobi days (the biggest settlement days).
  - (d) As (a), 2015+ only (era check).
  - Costs: 1 pip per side.
- **Data:** HistData USDJPY 1-min. The Japan holiday list is hard-coded or taken from the 1306.T trading calendar in data/.
- **Not a duplicate:** 003, 197 and 320 used USDJPY overnight into the US open, the London month-end fix, and the
  fiscal-year-end. None looked at the Tokyo morning fixing or gotobi dates.

## B2 — Daily London 4pm WM/R fix: pre-fix run-up, post-fix reversal
- **Sources:** Evans, "Forex trading and the WMR Fix", https://pages.stern.nyu.edu/~jh4/SternMicroMtg/Old/SternMicroMtg2015/Papers/4ExEvans.pdf ;
  CME practitioner note: https://www.cmegroup.com/news/2026/month-end-fx-flows-how-to-prepare.html
- **Participant / trade:** Asset managers and custodians execute benchmark FX orders "at the fix". Banks pre-hedge them
  in the 15-30 minutes before 16:00 London. Once the order flow is done, the price partly reverts.
- **Test:** HistData EURUSD, GBPUSD and USDJPY 1-min, 2010+. Convert HistData's fixed-EST clock to Europe/London time,
  because US and UK daylight-saving dates differ. The signal is R = the return from 15:30 to 15:59:59 London, known
  before entry.
  - (a) Every day except month-ends: trade −sign(R) from 16:00 to 16:30 London.
  - (b) As (a), only when |R| > 1σ, where σ is the trailing 60-day σ of R.
  - (c) Month-end days only (Evans reports bigger reversals there).
  - (d) Shorter window: R measured 15:45→16:00, held 16:00→16:15.
  - Pool the pairs. Costs: 0.5 pip per side for EURUSD, 1 pip for the others.
- **Data:** HistData FX 1-min.
- **Not a duplicate:** 197 tested a directional month-end hedge signal (SPY vs EFA gap) using UUP daily and USDJPY
  09:29→11:05 NY. It was not conditioned on the run into the fix, did not cover non-month-end days, and did not test a
  reversal.

## B3 — Mortgage convexity hedging: rate-move continuation inside the "convexity zone"
- **Sources:** NY Fed TMPG, "Convexity Hedging" (2021), https://www.newyorkfed.org/medialibrary/Microsites/tmpg/files/TMPG_Convexity_Hedging_presentation_03_23_21.pdf ;
  Reuters via Kitco (2026-10-12), https://www.kitco.com/news/off-the-wire/2026-10-12/us-yield-surge-triggers-mortgage-hedging-adds-bond-market-pressure
- **Participant / trade:** MBS holders and servicers are short convexity. When yields jump, their MBS duration
  extends, so they sell Treasuries or pay in swaps. When yields fall, they buy. This amplifies moves, and it does so
  most when mortgage rates are close to the coupons outstanding (maximum prepayment sensitivity).
- **Test:** Instruments TLT and IEF. Convexity zone (known at t) = FRED MORTGAGE30US within ±75bp of its own average
  from 3 to 7 years earlier (a proxy for the outstanding coupon stack). Trigger = |5-day Δ DGS10| above its train-period
  90th percentile. The trade follows the yield move: short TLT if yields rose, long if they fell. It is entered lag-1
  at the close.
  - (a) Hold 5 sessions. Compare the in-zone result with the out-of-zone result; the interaction is the test.
  - (b) 1-day trigger |ΔDGS10| > 10bp, hold 3 sessions.
  - (c) IEF instead of TLT (most hedging happens at the 5-10y point).
  - (d) MBB − IEF spread, 2007+ (does MBS underperform during the hedging?).
- **Data:** FRED DGS10 and MORTGAGE30US (1971+); Yahoo TLT, IEF, MBB.
- **Not a duplicate:** Earlier rates tests were calendar-based (month-end, auctions, refunding) or used positioning
  (299/300). No test has looked at rate-move continuation conditioned on hedger exposure. 151 (CTA) was SPY only.

## B4 — Primary-dealer Treasury inventory: being paid to hold supply
- **Sources:** Fleming, Nguyen & Rosenberg, "How Do Treasury Dealers Manage Their Positions?", NY Fed SR299,
  https://www.newyorkfed.org/research/staff_reports/sr299 ; Fed note "Treasury Issuance and Dealer Balance Sheets"
  (2026-10-30), https://www.federalreserve.gov/econres/notes/feds-notes/treasury-issuance-and-dealer-balance-sheets-20261030.html
- **Participant / trade:** Primary dealers absorb new issuance into inventory. They hedge it only partly and need to
  distribute it. SR299 finds they are paid for this through later price appreciation of the securities they hold. A
  large build-up of inventory should therefore predict positive Treasury returns.
- **Test:** NY Fed PD statistics API, `markets.newyorkfed.org/api/pd/get/PDPOSGST-TOT.json` (Treasury ex-TIPS net
  position, weekly as of Wednesday, from 2013 in the API; earlier data back to 1998 from the NY Fed historical
  spreadsheets). The data is released the following Thursday at about 16:15 ET, so enter at the Friday close. Signal =
  z-score of the 4-week change in net position, computed separately within each series break.
  - (a) z > +1 → long IEF for 4 weeks.
  - (b) Symmetric: z < −1 → short IEF.
  - (c) TLT, hold 1 week.
  - (d) The coupon sub-series only (exclude bills), if its key exists in the API list.
- **Data:** NY Fed markets API and historical PD files; Yahoo IEF, TLT.
- **Not a duplicate:** 299/300 used CFTC futures positioning by asset managers and levered funds. Dealer cash-bond
  inventory from the NY Fed has not been used.

## B5 — Auction takedown: post-auction recovery when dealers were stuffed
- **Sources:** Boston Fed WP 24-7, "The Effect of Primary Dealer Constraints on Intermediation in the Treasury Market",
  https://www.bostonfed.org/publications/research-department-working-paper/2024/the-effect-of-primary-dealer-constraints-on-intermediation-in-the-treasury-market ;
  SR299 (above).
- **Participant / trade:** When end-investor demand is weak, primary dealers have to take a large share of an auction.
  They then sell the paper down over the next days. This should give either continued pressure followed by a recovery,
  or a quick concession recovery, and the direction needs to be fixed on train data.
- **Test:** fiscaldata `v1/accounting/od/auctions_query`, using the fields primary_dealer_accepted, total_accepted and
  bid_to_cover_ratio (dealer share is available from 2008; bid-to-cover from 2000). The signal is known at the 13:00 ET
  result. Tenors: 10y → IEF, 30y → TLT. Dealer-share surprise = share − average share of the same tenor's previous 6
  auctions.
  - (a) Surprise > +5pp → long from the auction-day close to the close at d+3.
  - (b) As (a), but short d+1..d+2 (continued distribution). Pick only one of (a)/(b) on train before VALID.
  - (c) Low bid-to-cover (below the same-tenor trailing-6 average by more than 0.2) as the trigger. This works 2000+.
  - (d) Contrast: high-surprise minus low-surprise auctions.
- **Data:** fiscaldata auctions API (checked 2026-11-21: dealer fields populated from 2008-05); Yahoo IEF, TLT.
- **Not a duplicate:** 133/134 tested the unconditional auction supply cycle (Lou-Yan-Zhang timing). No test has
  conditioned on the auction outcome or on who absorbed the supply.

## B6 — Repo-funding stress spikes outside reporting dates (levered Treasury holders)
- **Sources:** Fed note "What Happens on Quarter-Ends in the Repo Market" (2025-06-06),
  https://www.federalreserve.gov/econres/notes/Feds-notes/what-happens-on-quarter-ends-in-the-repo-market-20250606.html ;
  NY Fed SR974 (repo spikes and reserves), https://www.newyorkfed.org/medialibrary/media/research/staff_reports/sr974.pdf
- **Participant / trade:** Relative-value hedge funds (cash-futures basis) and dealers finance long Treasury positions
  in repo. A SOFR spike that is not caused by a reporting date signals real balance-sheet scarcity. Levered holders cut
  their cash long positions, so cash Treasuries (especially the long end) cheapen for a few days.
- **Test:** Signal = SOFR − IORB (FRED IOER before 2021-07). Separately, the SOFR 99th−1st percentile spread from the
  NY Fed. The spike threshold is the trailing 120-day 95th percentile. Exclude the last 2 and first 2 business days of
  each month and the 15th ±1 (settlement dates). Enter lag-1 at the close.
  - (a) Short TLT for 3 sessions.
  - (b) Short TLT − long IEF for 3 sessions (long-end cheapening).
  - (c) Short SPY for 3 sessions (cross-asset deleveraging).
  - (d) Dispersion trigger (99th−1st percentile above its 95th percentile) instead of the level.
  - **Caveat:** SOFR starts in April 2018, so this is VALID+HOLDOUT only. Run it as a descriptive or pre-registered
    test with low power and say so.
- **Data:** NY Fed markets API (`/api/rates/secured/sofr/...`), FRED IORB/IOER; Yahoo.
- **Not a duplicate:** 142/304/305 used Fed balance-sheet quantities (WALCL, TGA, RRP). 176 used calendar quarter-end
  window dressing in TLT. No test has used repo rates, or excluded reporting dates to isolate stress.

## B7 — Foreign central banks selling custody Treasuries (FX-intervention funding)
- **Sources:** Bloomberg (2024-05-09), "Foreign Treasury Holdings Hint at Yen Intervention Funding",
  https://www.bloomberg.com/news/articles/2024-05-09/foreign-treasury-holdings-hint-at-yen-intervention-funding ;
  Central Banking, https://www.centralbanking.com/central-banking/news/1648700/central-banks-biggest-us-treasury-sell
- **Participant / trade:** When the dollar is strong, reserve managers (Japan, EM central banks) sell Treasuries out of
  custody at the NY Fed to raise cash for currency defence. This creates supply that is unrelated to the rates outlook.
- **Test:** FRED WMTSECL1 (marketable Treasuries held in custody for foreign official accounts; weekly Wednesday
  level; nonzero from 2007-07). It is released Thursday 16:30 ET, so enter at the Friday close.
  - (a) 4-week % change below its train 10th percentile → short TLT for 4 weeks.
  - (b) As (a), only when UUP's 4-week return > 0 (intervention context).
  - (c) IEF instead of TLT, hold 2 weeks.
  - (d) Reverse direction: large custody increases → long IEF for 4 weeks.
- **Data:** FRED WMTSECL1; Yahoo TLT, IEF, UUP. The train period is only 2007-14, so state the reduced power.
- **Not a duplicate:** The official sector has not been studied. 299/300 used CFTC futures; 142/304 used Fed
  holdings, not foreign custody.

## B8 — Quarter-end and year-end dollar funding squeeze in spot FX
- **Sources:** Boston Fed WP 18-6, "Demand Effects in the FX Forward Market: Micro Evidence from Banks' Dollar
  Hedging", https://www.bostonfed.org/publications/research-department-working-paper/2018/the-pricing-of-fx-forward-contracts.aspx ;
  BIS WP 836 (G-SIB year-end quoting), https://www.bis.org/publ/work836.pdf
- **Participant / trade:** Non-US banks report quarter-end balance-sheet snapshots. G-SIBs shrink their balance sheets
  at year-end to lower surcharges. Dollar demand that cannot be met synthetically spills into spot, so USD should be
  bid into the reporting date and give some back afterwards. The test checks whether the swap-market squeeze appears
  in spot.
- **Test:** UUP (2007+) and EURUSD/USDJPY HistData daily closes at 16:00 NY (2001+). The calendar is known in advance.
  - (a) Long USD from the close at d−5 to the last trading day of each quarter.
  - (b) Short USD over the first 3 sessions of the new quarter.
  - (c) Year-end only (December, both legs). Expect about 22 events, so this is descriptive.
  - (d) Control: the same windows at non-quarter month-ends. The effect should be absent there.
- **Data:** Yahoo UUP; HistData FX.
- **Not a duplicate:** 197 was a last-day month-end hedge-rebalancing signal driven by the SPY−EFA gap. 176 was TLT
  window dressing. 320 was JPY at fiscal-year-end. None tested an unconditional reporting-date USD premium with a
  post-date reversal.

## B9 — Mutual-fund October-31 fiscal year: tax-loss selling into Oct 31, rebound in November
- **Sources:** Gibson, Safieddine & Titman, "Tax-Motivated Trading and Price Pressure: An Analysis of Mutual Fund
  Holdings", JFQA, https://resolve.cambridge.org/core/journals/journal-of-financial-and-quantitative-analysis/article/taxmotivated-trading-and-price-pressure-an-analysis-of-mutual-fund-holdings/3D000AB295A4406FCC2C9F8C4A65ECA7 ;
  Evercore ISI via MarketWatch, https://www.itiger.com/news/2578961713
- **Participant / trade:** Since 1986, most US mutual funds have had a fiscal year ending October 31. They realise
  losses in their losing positions before that date to offset distributions. The selling pressure ends at Oct 31, and
  the losers rebound in November. This is mechanical, not driven by mood.
- **Test:** The signal is the YTD return (Jan 1 → Oct 20 close). Universes: (i) the ~95 large caps in data/;
  (ii) the 9 sector SPDRs plus the survivorship-free country-ETF list from 062.
  - (a) Long the bottom quintile minus the top quintile from the Oct 31 close to the Nov 30 close.
  - (b) Short the bottom quintile minus the top quintile from the Oct 20 close to the Oct 31 close (pressure leg).
  - (c) As (a), held only through the first 10 sessions of November.
  - (d) Placebo: the same rule with a June 30 cut-off (no tax deadline). It should show no rebound.
  - There is one event per year, so rely on the cross-section and report year-by-year signs.
- **Data:** Yahoo daily.
- **Not a duplicate:** 046 was the December/January effect via IWM−SPY. 016 was a one-day quarter-end sector trade.
  120/063 used flow-chasing and the capital-gains overhang. None tested the October fund deadline.

## B10 — Corporate buyback desks support dips, but only in the open window
- **Sources:** Bloomberg (2019-08-08), "Goldman Buyback Desk Saw Orders Rise 'Dramatically' During Rout",
  https://www.bloomberg.com/news/articles/2019-08-08/goldman-buyback-desk-saw-orders-rise-dramatically-during-rout ;
  BNN/Bloomberg (2024-08-15), https://bnnbloomberg.ca/business/company-news/2024/08/15/goldman-buyback-desk-flooded-with-record-orders-during-stock-swoon
- **Participant / trade:** 10b5-1 and discretionary buyback programmes run price-sensitive grids, so they buy more on
  down days. They are mostly inactive during earnings blackouts (from about 2 weeks before quarter-end until the
  results). Dip rebounds should therefore be stronger in the open window than during blackout.
- **Test:** SPY close→close, lag-1. Blackout is a calendar proxy: from 14 calendar days before quarter-end to 45 days
  after it. The open window is all other days. Trigger: SPY day return ≤ −1.5% at close t. Go long from close t+1 to
  close t+4.
  - (a) Open-window triggers minus blackout triggers. The interaction is the test, reported as timing value against
    exposure-matched buy-and-hold.
  - (b) Threshold −1% instead of −1.5%.
  - (c) Hold to close t+2 instead.
  - (d) 2010+ only (the large-buyback era; record it as a post-hoc era split).
- **Data:** Yahoo SPY.
- **Not a duplicate:** 113 tested the blackout calendar unconditionally and found nothing. 053/061 tested dip
  rebounds unconditionally. No test has looked at the buyback-window × dip interaction, which is the specific
  mechanism the desks describe.

---
**Notes on ranking:** B1, B2 and B5 have the cleanest mechanism and the most data (intraday or per-auction samples
with hundreds of events). B6 and B7 have short histories. B8 and B9 have few events per year and rely on
cross-section or controls.
