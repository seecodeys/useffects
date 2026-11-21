# Idea scout (cross-asset / global mechanical flows): 8 new ideas (2026-11-21, NOT tested)

Scope: mechanical flows from foreign investors, central banks, reserve managers, commodity indices, bond-index
inclusions, benchmark fixings and gold ETF creations. The targets are US-tradable ETFs and FX. Psychology and
sentiment ideas are excluded.
Screened against BACKLOG.md, the LOG.md headings (000-344), results/ideas_behaviour.md (B1-B10) and
results/ideas_papers.md (P1-P10). These were dropped as duplicates: month-end FX hedge (197), WM/R fix (330), gotobi
(329), JPY fiscal year-end (028/320), the quarter-end USD squeeze (B8), commodity roll (037), CFTC (299/301/308),
international month-end (257/264/274/284), MSCI review days (178), foreign custody (341), Japan-hedged-equity month-end
re-hedge (too close to 197), and a "European close" SPX window (no source found).
Protocol reminders for every card: TRAIN = data start to 2014, VALID = 2015-22, one holdout look (2023+) only if the
card passes the gate. Signals must be known before entry, with lag-1 for anything published after a close. Register
every listed variant and no others. Costs are 2bp/side for ETFs and FX majors, and 3-5bp/side for CNH/INR/NOK.

---

## X1. BCOM / S&P GSCI January reweight: buy last year's commodity losers, sell the winners during the reweight window
- **Source:** Practitioner mechanics (the window is business days 5-9 of January and weights are pre-announced):
  https://david-whitcomb-ls5p.squarespace.com/trader-education-blog/commodity-index-rebalance . Academic counterpoint
  (event study 2006-21 finds effects in OI/volume, not clearly in prices):
  https://opus.bibliothek.uni-augsburg.de/opus4/files/97437/97437.pdf . 2026 weights announcement:
  https://www.wisdomtree.com/ch/en/insights/blog/the-bloomberg-commodity-index-bcom-2024-rebalance
- **Mechanism:** BCOM and GSCI reset fixed target weights once a year, inside the January roll (GSCI bd 5-9, BCOM bd
  6-10). Index trackers, roughly $100bn+ by most estimates, must sell commodities that outgrew their target last year
  and buy the laggards. The trade is fully predictable from the prior year's relative returns, so a price footprint
  should show up in the window.
- **Test:** Universe: USO, UNG, GLD, SLV, DBB (base metals), DBA (ags), CORN, WEAT, SOYB, CPER, plus DBE/DBP as
  group checks. Use whatever was listed at the time, so the universe is survivorship-free. Signal: each asset's return
  over the prior calendar year minus the universe mean, known on Dec 31. Hold from the close of January bd 4 to the close
  of bd 10.
  - (a) Long the bottom 3 and short the top 3, equal weight.
  - (b) Rank-IC per year between the signal and the window return (descriptive; about 25 Januaries).
  - (c) Reversal leg: the opposite position from the close of bd 10 to the close of bd 20.
  - (d) Placebo: the same rule in the bd 4-10 window of Feb-Dec, which has a roll but no reweight. The effect should
    show up only in January.
  - (e) 2010+ only: use the actual announced BCOM target-weight change (press releases) minus the drift as the signal.
- **Data:** Yahoo ETF closes (2006+ for most names; pre-2006 only GLD from 2004). For a longer back-cast, use Yahoo
  continuous futures (CL=F, NG=F, GC=F, HG=F, ZC=F, ZW=F, ZS=F) and flag January roll jumps.
- **Why not a duplicate:** 037 tested a monthly USO roll-window short with no cross-section and no reweight. 308 used
  COT positioning. No test has used the annual target-weight reset.

## X2. London gold PM fix (15:00 London): the price weakens into the fix and recovers after it
- **Source:** Caminschi & Heaney, "Fixing a Leaky Fixing", J. Futures Markets 34(11), 2014:
  https://ideas.repec.org/a/wly/jfutmk/v34y2014i11p1003-1039.html ; Abrantes-Metz & Metz study (large PM-fix moves
  were negative at least 2/3 of the time in six years from 2004-13):
  https://www.bloomberg.com/news/articles/2014-02-28/gold-fix-study-shows-signs-of-decade-of-bank-manipulation ;
  LBMA rebuttal: https://www.lbma.org.uk/alchemist/issue-73/has-there-been-a-decade-of-london-pm-gold-fixing-manipulation
- **Mechanism:** Physical benchmark orders from ETF creations (GLD prices at the LBMA PM), central banks and miners'
  hedges execute at the 15:00 London fix. Dealers pre-position, so the price should move into the fix and reverse after.
  The 2015 move from a bank panel to an IBA electronic auction is a structural break to test across.
- **Test:** HistData XAUUSD 1-min (cached), NY local clock converted to Europe/London. The fix is at 10:00 NY, except
  in the March and Oct/Nov DST-mismatch weeks, when it is at 11:00 NY.
  - (a) Short gold from 14:30 to 15:00 London every day (pre-fix pressure).
  - (b) Long gold from 15:05 to 16:00 London (post-fix recovery).
  - (c) Split (a) and (b) by era, 2009-Mar 2015 (panel fix) vs 2015-22 (IBA auction). The prior is a decay after 2015.
  - (d) Clock placebo: on DST-mismatch days, run the same NY-clock window without the fix. A real effect follows the
    London clock, not the NY one.
  - Tradable check for any survivor: the same window on GLD 5-min bars where available.
- **Data:** HistData XAUUSD (cached, 2009+; earlier years downloadable). Yahoo GLD.
- **Why not a duplicate:** 003 (part 3) used overnight gold as a predictor for SPY. 226 tested gold's month-end
  rebalancing. 330 tested the FX WM/R fix, not the gold benchmark. No test has looked at the gold fixing window.

## X3. Norges Bank's pre-announced daily NOK transactions for the oil fund
- **Source:** Norges Bank, "Foreign exchange transactions on behalf of the government" (monthly table since Jan 2000;
  negative = NOK purchases): https://www.norges-bank.no/en/topics/statistics/foreign-exchange-transactions-daily/ ;
  method (planned, smoothed, pre-announced on the last business day of the prior month):
  https://www.norges-bank.no/en/topics/liquidity-and-markets/Foreign-exchange-purchases-for-GPFG/
- **Mechanism:** Norges Bank buys or sells a fixed NOK amount every business day of the month on behalf of the
  government (petroleum cash flows vs budget deficit). It announces the amount in advance and then executes it
  mechanically. A month in which daily NOK buying steps up is a price-insensitive flow known before the month starts.
- **Test:** Signal: that month's net NOK purchases per business day (from the table, which matches the announcement)
  and its change vs the prior month, both known at the prior month's last business day. Hold from that day's close to
  the close of the month's last business day.
  - (a) Long NOK (short EURNOK) when the per-day NOK purchase rises by more than NOK 200m/day vs the prior month; short
    when it falls by the same amount.
  - (b) Level: long NOK vs EUR when the bank is a net NOK buyer, short when it is a net seller.
  - (c) Same as (a), in USDNOK.
  - (d) Tradable: NORW (2010+) minus EFA over the month on the (a) signal.
  - December is excluded or treated separately, since the bank pauses late in December.
- **Data:** Norges Bank table (HTML scrape, 2000-2026); Yahoo EURNOK=X, NOK=X, NORW, EFA; HistData EURNOK/USDNOK
  1-min if needed.
- **Why not a duplicate:** No Norway or oil-fund test exists. 341 (foreign custody) and 304 (Fed liquidity) used
  other central-bank balance sheets.

## X4. Bond-index inclusion tranches: CNY/INR and local bonds into the phased month-end effective dates
- **Source:** Bloomberg Global Agg China, 20 monthly 5% steps from Apr 2019:
  https://www.bloomberg.com/company/announcements/bloomberg-add-china-bloomberg-barclays-global-aggregate-indices ;
  FTSE WGBI China, phased over 36 months from 29 Oct 2021:
  https://www.lseg.com/en/media-centre/press-releases/ftse-russell/2021/ftse-russell-announces-results-semi-annual-country-classification-review-fixed-income-and ;
  JPM GBI-EM India, +1pt per month from 28 Jun 2024 to 31 Mar 2025:
  https://www.business-standard.com/markets/news/indian-govt-bonds-now-part-of-jp-morgan-s-bond-index-here-s-what-it-means-124062800086_1.html
- **Mechanism:** Every tranche raises the target weight at a known month-end close, so passive global bond funds
  (around $3-6bn per month for China) must buy local bonds and the currency on that date. This is the same mechanism as
  the US index-extension bid (275), transplanted to EM FX and bonds with an exact pre-published schedule.
- **Test:** Event set: about 66 China tranche month-ends (BBG Agg Apr 2019-Nov 2020, JPM GBI-EM Feb-Nov 2020, FTSE
  WGBI Nov 2021-Oct 2024) and about 20 India month-ends (JPM Jun 2024-Mar 2025, Bloomberg EM LC 2025). Controls are
  the non-tranche month-ends from 2017 to 2026.
  - (a) Short USDCNH over the last 3 sessions into tranche month-ends vs control month-ends.
  - (b) Short USDINR over the same window for the India tranches.
  - (c) Long CBON (China bond ETF, 2014+) over the last 3 sessions into tranche vs control month-ends.
  - (d) Reversal: long USDCNH/USDINR over the first 3 sessions after each tranche.
  - This is mostly descriptive: the whole sample sits after 2014, so it is VALID plus holdout only. Report it as a
    consistency card and do not claim a gate pass.
- **Data:** Yahoo CNH=X (or CNY=X), INR=X, CBON; the tranche calendar from the cited announcements.
- **Why not a duplicate:** 275/276/284 covered the US and global-aggregate month-end bid in USD bond ETFs (BWX, EMB),
  not inclusion tranches or EM currencies. 178 covered MSCI equity reviews.

## X5. Japanese investors' weekly portfolio flows (MOF): persistence into the next week
- **Source:** Japan MOF, "International Transactions in Securities (Weekly)" with a historical CSV, 2005+:
  https://www.mof.go.jp/english/policy/international_policy/reference/itn_transactions_in_securities/index.htm ;
  the release is watched by FX desks: https://www.forexlive.com/news/!/japan-buying-foreign-bonds-y-401-4b-plus-the-rest-of-this-data-20140918
- **Mechanism:** Japanese lifers, pensions and toshin buy or sell foreign bonds through multi-week allocation programmes
  (semi-annual investment plans, new-fund launches). Non-residents' buying of Japanese equities is similarly
  programme-like. Flows are autocorrelated, so a large week signals continued price-insensitive flow the following week.
- **Test:** The release is Thursday 08:50 JST (Wednesday ~19:50 NY), so enter at Thursday's US open (or the Wednesday
  close plus one) and hold to the next Wednesday close. Signal = the weekly flow divided by its trailing 52-week
  standard deviation.
  - (a) Residents' net foreign long-term bond purchases z > +1.5: long USDJPY (unhedged buying sells yen); z < −1.5:
    short.
  - (b) Same signal: long TLT (or BWX) on z > +1.5.
  - (c) Non-residents' net purchases of Japanese equities z > +1.5: long EWJ (z < −1.5: short).
  - (d) Same as (c) on DXJ (yen-hedged), to separate the equity flow from FX.
- **Data:** MOF CSV (2005+); Yahoo EWJ, DXJ, TLT, BWX, JPY=X; HistData USDJPY 1-min (cached) for exact entry.
- **Why not a duplicate:** 320/028 tested calendar windows for Japanese repatriation, 329 tested gotobi and 197 tested
  month-end hedging. None used the observed MOF flow data.

## X6. SNB intervention (weekly sight deposits) and the franc the following week
- **Source:** Basel WP 2020-04, "The Short-Run Impact of SNB Sight Deposits on Exchange Rates: Results from Weekly
  Data 2015-2018": https://ideas.repec.org/p/bsl/wpaper/2020-04.html ; Bloomberg on sight deposits as the intervention
  proxy: https://www.bloomberg.com/news/articles/2019-08-05/snb-sight-deposits-hit-record-suggesting-fx-interventions
- **Mechanism:** The SNB pays for FX purchases by crediting banks' sight deposits, so the weekly change in domestic
  sight deposits is a near-real-time read of central-bank franc selling (2009-21) or buying (2022-23). Interventions
  come in campaigns, so a big intervention week predicts more official flow, and possibly a persistent EURCHF move,
  the following week.
- **Test:** Data is released Monday ~09:00 CET for the prior week. Enter at Monday's NY close and hold to the
  following Monday's close. Exclude the EURCHF floor (Sep 2011-Jan 2015) entirely.
  - (a) Δ domestic sight deposits > +CHF 2bn: long EURCHF (short FXF).
  - (b) Δ < −CHF 2bn (SNB selling FX, mostly 2022-23): short EURCHF.
  - (c) Continuous: regress the next-week EURCHF return on the z-scored Δ. Pick the sign on TRAIN (2009-11) and
    confirm on VALID (2015-22).
  - (d) Same as (a), in USDCHF.
- **Data:** SNB data portal (data.snb.ch, weekly monetary data, free CSV); Yahoo EURCHF=X, CHF=X, FXF.
- **Why not a duplicate:** No CHF or SNB test exists. 341 covered foreign custody Treasuries and 304 the Fed balance
  sheet.

## X7. HKMA convertibility-undertaking triggers: liquidity drain and Hong Kong equities
- **Source:** HKMA, "How does the LERS work?" (a weak-side trigger contracts the Aggregate Balance and lifts HIBOR):
  https://www.hkma.gov.hk/eng/key-functions/money/linked-exchange-rate-system/how-does-the-lers-work/ ; example
  trigger notice: https://www.hkma.gov.hk/eng/news-and-media/press-releases/2018/08/20180815-5
- **Mechanism:** When USDHKD hits 7.85, the HKMA must buy HKD. The resulting Aggregate Balance drop is announced and
  settles at T+2, so it is known in advance. It mechanically tightens HKD liquidity and lifts HIBOR, which hits
  rate-sensitive HK property and banks (most of EWH). Strong-side triggers at 7.75 do the reverse, injecting liquidity.
- **Test:** Trigger days come from HKMA press releases, or from the proxy Yahoo HKD=X close ≥ 7.849 (weak) or ≤ 7.751
  (strong). Enter at the US close on the trigger day.
  - (a) Short EWH minus EEM over the next 10 sessions after the first weak-side trigger in a cluster (no trigger in
    the prior 20 sessions).
  - (b) Long EWH minus EEM over the next 10 sessions after the first strong-side trigger in a cluster (2008-15, 2020).
  - (c) Cumulative-intervention version: HKMA net purchase over the trailing 10 days larger than HK$20bn → short EWH
    minus EEM for 10 sessions.
  - (d) 20-session horizon for (a) and (b).
  - Clusters are few (2008-09, 2012, 2015, 2018, 2019, 2020, 2022-23, 2025), so report per-cluster results;
    descriptive unless more than 15 clusters exist.
- **Data:** HKMA press releases and HKMA open API (daily Aggregate Balance); Yahoo HKD=X, EWH, EEM.
- **Why not a duplicate:** No Hong Kong peg or liquidity test exists. 002/062 used EWH only for country reversal and
  home-market catch-up.

## X8. GLD creation/redemption flows (bullion tonnage) and gold over the following days
- **Source:** SPDR Gold Shares historical data archive (daily tonnes in trust, 2004+, XLSX download):
  https://spdrgoldshares.com/usa/historical-data
- **Mechanism:** GLD creations force authorised participants to buy London bullion, and redemptions force them to
  sell. The flow is mechanical and lumpy, and fund-flow programmes (model portfolios, CTAs, Asian demand via sister
  funds) persist for days. If APs hedge with futures and settle physical later, the bullion leg should leave a visible
  multi-day footprint.
- **Test:** The day-t tonnes are published after the close, so the signal uses data through t−1 at entry at t's close
  (lag-1). Signal = the 5-day change in tonnes divided by tonnes (%).
  - (a) Top-decile 5-day inflow: long GLD for the next 5 sessions. Bottom decile: short. Deciles use an expanding
    window.
  - (b) Same signal, but the trade is GDX minus GLD (do miners lag the bullion flow?).
  - (c) Reversal reading: the opposite sign for sessions 6-20 (temporary price pressure).
  - (d) 1-day version: a daily tonnage change larger than 1% → GLD next session close to close.
- **Data:** SPDR archive XLSX (2004+); Yahoo GLD, GDX (2006+), GC=F.
- **Why not a duplicate:** 065 tested extrapolation in GLD prices, 226 gold month-end and 285 GLD as a control. No
  test has used ETF bullion-flow data.

---
**Rough priority:** X2 (cached 1-min data, many events, clean placebo), X3 (exact pre-announced flow, 26 years), X5
(clean weekly data, 2005+), X1 (strong mechanism, low power), X6, X8, then X4 and X7, which are descriptive because
their samples are short or clustered.
