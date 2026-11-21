# Idea scout: 8 settlement / funding / calendar-mechanics ideas (2026-11-21, NOT tested)

Scope: predictable, non-psychological cash-flow and plumbing events in US ETFs, Treasuries and indices. I checked each
idea against LOG.md (000-360), BACKLOG.md and results/ideas_{behaviour,crossasset,options_etf,papers}.md. These are
excluded as already tested or listed: Treasury coupon/maturity 15th and month-end (209/286); SOQ (245); settlement-lag
day (268); tax dates (177/305); net liquidity/RRP level (142/304); POMO (233); auction cycle and takedown
(133/134/331); SPY dividend reinvestment and ex-date twins (234/353); index-futures roll (115); quarter-end TLT window
dressing (176); repo-stress spikes (B6) and quarter-end USD squeeze (B8), which are listed but untested; month-end bond
bid family (275-319).

Protocol: train 2002-14, valid 2015-22, holdout 2023+ untouched unless the gate passes. Use lag-1 or a pre-known
calendar. Costs are 1bp/side for Treasury ETFs and 2bp/side for MBB/MUB/HYG/EMB. Pick one variant on train before
valid.

---

## S1. Treasury-futures quarterly roll window (pre-First-Intention-Day roll in Feb/May/Aug/Nov)
- **Source:** CME, "A descriptive study of US Treasury rolls" (2018),
  https://www.cmegroup.com/market-data/files/a-descriptive-study-of-us-treasury-rolls-aug-20-2018.pdf ;
  CME Pace-of-the-Roll guide, https://cmegroup.com/trading/paceoftheroll/user-guide.html
- **Mechanism:** Futures-heavy asset managers and basis traders roll millions of TY/US contracts in the days before
  First Intention Day. In these four months the roll is a large, dated transfer of duration exposure between holders.
  When roll costs are rich, cash-futures basis trades are re-set (cash bonds are bought or sold against the deferred
  contract), which moves cash Treasuries in a known window.
- **Test:** The window is business days −8 to −4 before the last business day of Feb/May/Aug/Nov. It deliberately
  stops before the last-3-session bond bid (276). Enter at the close of day −9 and exit at the close of day −4.
  - (a) IEF in roll months minus the same window in the other 8 months (difference-in-means, two-sided).
  - (b) Same as (a) on TLT (the US/WN roll).
  - (c) Delivery window: IEF over the first 5 business days of Mar/Jun/Sep/Dec minus the same days in other months.
    Shorts acquire CTD cash bonds to deliver.
  - (d) Roll richness: Yahoo ZN=F continuous series. The back-adjust jump at the roll measures the calendar spread.
    Run (a) only in quarters where the previous roll's jump was above its median.
- **Data:** Yahoo IEF/TLT (2002+), ZN=F/ZB=F. Roll dates come from the CME rule (FND = last business day of the month
  before delivery).
- **Not a duplicate:** 115 was the equity-index roll week (SPY/IWM). 037 was commodities. No test has used the
  Treasury-futures roll or delivery calendar. The window excludes the month-end bond-bid days.

## S2. Agency-MBS P&I payment-day reinvestment (Ginnie II 20th, UMBS/Fannie/Freddie 25th)
- **Source:** Ginnie Mae issuer timeline (Ginnie II P&I drafted on the 20th),
  https://www.ginniemae.gov:443/issuers/issuer_training/NewIssuerTrainingPresentations/sf_monthly_reporting_workflow_timeline.pdf ;
  Freddie Mac UMBS (55-day delay, so the 25th), https://capitalmarkets.freddiemac.com/mbs/products/umbs
- **Mechanism:** Each month, agency MBS pay roughly $100-200bn of scheduled principal, prepayments and interest on two
  fixed days. Index-tracking MBS funds, banks and the Fed (until QT) reinvest that cash into TBAs and pools within days.
  This is a dated cash-deployment flow that is separate from the Treasury 15th and month-end.
- **Test:** P is the 25th, or the next business day.
  - (a) MBB long from the close of P−1 to the close of P+2 (reinvestment after the cash lands).
  - (b) MBB long from the close of P−4 to the close of P−1 (pre-positioning by TBA dealers).
  - (c) The (a) window on the MBB − IEF spread, duration-matched by 60-day regression beta, to isolate MBS-specific
    demand.
  - (d) Ginnie only: GNMA ETF (2012+, valid-heavy) over the (a) window anchored on the 20th.
  - Exclude any window that overlaps the last 3 sessions of the month (bond bid). This mainly affects February.
- **Data:** Yahoo MBB (2007+), GNMA (2012+), IEF.
- **Not a duplicate:** 209/286 were Treasury coupon/maturity reinvestment on the 15th and month-end (dead). 332 was
  convexity hedging, 345 was bank H.8 buying, and 280 was the MBB month-end bid. No test has used MBS payment dates.

## S3. Municipal "reinvestment season" (June 1 / July 1 principal and coupon wave)
- **Source:** LPL Research, "Summer Seasonal Technicals in Municipal Bonds",
  https://www.lpl.com/research/blog/summer-seasonal-technicals-in-municipal-bonds-a-reliable-tailwind.html ;
  Bond Buyer, https://www.bondbuyer.com/news/as-municipal-calendar-grows-to-7b-summer-starvation-for-supply-seen
- **Mechanism:** Municipal maturities, coupons and calls cluster on Jun 1 and Jul 1, at $40-50bn per month. Separately
  managed accounts and muni funds must reinvest that cash in a market where net supply is negative, so munis richen
  relative to Treasuries into and just after those dates.
- **Test:** The calendar is fixed in advance.
  - (a) MUB minus 0.8×IEF (approximate duration match; fix the hedge ratio on train) from the close 5 sessions before
    Jun 1 to the close 5 sessions after Jul 1, against all other windows of the same length.
  - (b) MUB long over the 10 sessions straddling Jul 1 (−5..+5) against the unconditional 10-session mean.
  - (c) Secondary peak: the same spread over −5..+5 around Jan 1 (Dec/Jan redemptions).
  - (d) Back-cast: the same window as (a) on the VWITX − VFITX spread (Vanguard muni vs Treasury mutual funds, Yahoo
    adjusted, 1990s+). This gives an extra out-of-sample period before 2007.
- **Data:** Yahoo MUB (2007+), VWITX, VFITX, IEF.
- **Not a duplicate:** 280 found no muni month-end bid. 342 covered fund tax-loss selling in October. No seasonal
  muni-redemption test exists.

## S4. Bond-ETF month-start ex-date: cum-day capture and ex-day drop vs distribution
- **Source:** iShares US distribution schedule,
  https://www.ishares.com:443/us/literature/shareholder-letters/isharesandblackrocketfsdistributionschedule.pdf ;
  LQD ex-date history (first business day each month), https://www.benzinga.com/quote/LQD/dividend
- **Mechanism:** Most US bond ETFs (TLT, IEF, LQD, HYG, AGG, BND, EMB) go ex on the first business day of each month.
  Their distributions are taxed as ordinary income, so taxable holders prefer to sell cum, and tax-exempt or
  dividend-capture buyers are needed to absorb that selling. The Elton-Gruber logic predicts an ex-day drop that is
  smaller than the distribution, with cum-day pressure in the other direction. The yield is 4-7%/yr for HY/EM, which is
  large compared with equity ETFs.
- **Test:** Ex-dates come from Yahoo dividends and are known in advance. Use total return, and take a rate residual
  r − β×(duration-proxy return), where the proxy is IEF for IG/HY/EM and β is a rolling 60-day estimate fixed before
  the event.
  - (a) Pooled ex-day residual (close of ex−1 to close of ex) across HYG, LQD, EMB, AGG, TLT. The null is 0.
  - (b) Cum-day residual (close of ex−2 to close of ex−1), which should be negative under capture-selling.
  - (c) Price-only drop divided by the distribution (median ratio vs 1) per ETF, as a descriptive measure.
  - (d) Trade: long HYG minus β×IEF from the close of ex−1 to the close of ex+1, HY/EM only (largest distributions).
- **Data:** Yahoo raw and adjusted closes plus dividends (2002/2007+).
- **Not a duplicate:** 353 tested SPY ex-day vs IVV/VOO (equity, quarterly). 281's early-month reversal leg used
  bond-ETF prices without isolating ex-days. No bond-ETF ex-date test exists. Also check whether price-only returns
  contaminated 281.

## S5. Bill-supply surges: debt-ceiling resolutions and announced net bill issuance
- **Source:** Reuters/Yahoo, "Markets brace for deluge of Treasury bill supply after US debt deal",
  https://finance.yahoo.com/news/markets-brace-deluge-treasury-bill-192544465.html ;
  Treasury financing statement (2025-07),
  https://www.treasurydirect.gov/instit/annceresult/press/preanre/2025/SPL_20250708_1.pdf
- **Mechanism:** After a debt-limit resolution, Treasury rebuilds the TGA by $400-700bn through bills within weeks.
  If MMFs fund the bills from RRP, reserves are untouched. If banks or other holders fund them, reserves drain and risk
  assets lose liquidity. Weekly bill sizes are announced 2-5 days before settlement, so net supply is known before entry.
- **Test:**
  - (a) Event study (8 events: 2011-08-02, 2013-10-17, 2015-11-02, 2018-02-09, 2019-08-02, 2021-12-16, 2023-06-03,
    2025-07-04). SPY excess return from the close of day +1 to +30 against the unconditional mean. Descriptive only
    (n=8).
  - (b) Continuous signal: announced net bill issuance over the next 4 weeks, computed as offering amount minus
    maturing bills from the fiscaldata auctions endpoint and keyed to announcement dates. Top quintile, with cut-offs
    set on train, means short SPY for the following week (Friday close to Friday close).
  - (c) Same signal applied to bill cheapening: change in DTB3 minus EFFR/SOFR (FRED) over the following week, as a
    descriptive mechanism check.
  - (d) Control split: run (b) when RRP > $300bn (2021-24, MMFs fund from RRP, so no effect is predicted) and when
    RRP < $300bn.
- **Data:** fiscaldata `v1/accounting/od/auctions_query`, FRED RRPONTSYD/DTB3/EFFR/SOFR, Yahoo SPY.
- **Not a duplicate:** 304 used the net-liquidity level (WALCL−TGA−RRP). 305 used tax-date TGA jumps. 133/331 were
  coupon auctions. None used announced bill supply or debt-ceiling rebuild windows.

## S6. Quarterly Refunding Announcement (QRA) window in long Treasuries
- **Source:** Hoover/Lopez-Salido et al., "Does Government Debt Management Matter? High-Frequency Identification of
  US Treasury Quarterly Refunding",
  https://www.hoover.org/research/does-government-debt-management-matter-high-frequency-identification-us-treasury-quarterly ;
  NBER w35098, https://www.nber.org/system/files/working_papers/w35098/w35098.pdf
- **Mechanism:** Coupon sizes for the next quarter are set at the QRA. The Monday borrowing estimate is followed by the
  Wednesday 8:30 ET policy statement in the first week of Feb/May/Aug/Nov. Duration holders demand a premium for
  carrying supply uncertainty into the announcement, which should be earned across the window, as in the pre-FOMC
  drift. A size change then reprices supply over several days.
- **Test:** Dates come from Treasury QRA archives (2002+; coupon sizes changed in 2006, 2009-10, 2014, 2018,
  2023-24).
  - (a) TLT long from the close of the Friday before the borrowing-estimate Monday to the close of QRA Wednesday.
  - (b) TLT on QRA Wednesday only (close→close).
  - (c) Directional: after an announced coupon-size increase (or decrease), short (or long) TLT from QRA close to +5
    sessions. Unchanged QRAs stay flat.
  - (d) (a) on IEF.
  - Exclude or flag QRAs that share a day with FOMC, and report results with and without them. This overlaps the
    first-3-sessions window of 281, so control for it with the same window in Jan/Apr/Jul/Oct+1.
- **Data:** Yahoo TLT/IEF; QRA dates and size changes from home.treasury.gov quarterly-refunding archives.
- **Not a duplicate:** 133/134 covered auction weeks, 276d refunding months for the month-end bid, and 209 refunding
  15ths for cash flows. The QRA announcement itself has never been tested.

## S7. Reserve-maintenance "settlement Wednesday" under scarce reserves (1991-2008) vs abundant reserves (2009-22)
- **Source:** Hamilton (1996), "The Daily Market for Federal Funds", JPE 104(1),
  https://ideas.repec.org/a/ucp/jpolec/v104y1996i1p26-56.html ; CME/Econoday glossary on reserve settlement,
  https://www.cmegroup.com/education/events/econoday/627633
- **Mechanism:** Before the Fed began paying interest on reserves in October 2008, banks had to meet reserve
  requirements by the last day of each two-week maintenance period (every other Wednesday). Fed funds spiked or
  collapsed that day, and dealers financing inventories faced funding squeezes. That squeeze is a pure plumbing shock
  to Treasury and equity carry, and it should vanish after 2008, which provides a built-in placebo.
- **Test:** Settlement Wednesdays come from a known anchor and a 14-day step, verified with the Wednesday |ΔDFF| spike
  in FRED DFF.
  - (a) Daily change in FRED DGS2 and DGS10 on settlement Wednesdays vs non-settlement Wednesdays, 1991-2008. Then run
    the same test on 2009-22 as the placebo.
  - (b) SPY open→close on settlement Wednesdays vs other Wednesdays, 1993-2008 (placebo 2009-22).
  - (c) Thursday after settlement (excess reserves released, rate drop): SPY open→close long, same eras.
  - (d) Condition (a)/(b) on the day's DFF − target spread (known by about 11:00 from the funds market; for the test use
    the prior-period average deviation as the ex-ante proxy).
  - The split replaces the standard protocol: estimate on 1991-2001, validate on 2002-08, placebo 2009-22.
- **Data:** FRED DFF/DFEDTAR/DGS2/DGS10 (1954+), Yahoo SPY (1993+), ^TNX.
- **Not a duplicate:** 233 tested POMO days, 305 tax-date reserve drains, and 142/304 balance-sheet levels. No test
  has used the maintenance-period calendar. It is also one of the few ideas that comes with a natural pre/post placebo.

## S8. Corporate-issuance windows and issuer rate-lock hedging (post-Labor Day and early January)
- **Source:** Reuters/Yahoo, "Post-Labor Day corporate debt spree revives U.S. Treasury selloff",
  https://ca.finance.yahoo.com/news/post-labor-day-corporate-debt-193515861.html ;
  Reuters/AOL, "Corporate hedging to save debt costs may have worsened 10yr sell-off",
  https://www.aol.com/news/analysis-corporate-hedging-save-debt-111243784.html
- **Mechanism:** Investment-grade issuance is heaviest in the first week or two after Labor Day and after New Year,
  often $100-190bn per window. Issuers and underwriters pre-hedge by selling Treasuries or locking rates, which
  pressures 7-30y Treasuries. New-issue concessions then cheapen secondary IG credit until the supply clears.
- **Test:** The calendar is fixed in advance.
  - (a) Short TLT from the close of the Friday before Labor Day to the close of the 7th session after it.
  - (b) January window: short TLT from the close of the 1st session to the close of the 8th session of January.
  - (c) LQD − β×IEF spread short over the (a) and (b) windows (new-issue concession in credit).
  - (d) Control: the same lengths in the week after the window (unwind and recovery: long TLT sessions 8-12). Also
    check that (a) is not just September FOMC timing by excluding FOMC days.
- **Data:** Yahoo TLT/IEF/LQD (2002+). SIFMA monthly IG issuance (free XLS) for a descriptive dose check by window.
- **Not a duplicate:** 133/331 were Treasury supply. 039/167/205 were credit month-end and outflow tests. 046/342 were
  tax-loss and January tests in equities. No test has used corporate issuance calendar windows or rate-lock hedging.
