# Idea scout (papers): 10 new flow / mechanics ideas (2026-11-21, NOT tested)

Scope: mechanical flows and participant constraints in US index ETFs, Treasuries and option-linked products.
Screened against BACKLOG.md, LOG.md (through 332/333), registry.csv (446 rows), agent registries 329/331 and the
earlier scout list `results/ideas_behaviour.md`. These were dropped as duplicates: gotobi/4pm fix, mortgage convexity
(332), primary-dealer inventory, auction takedown, repo stress, foreign custody, buyback blackout, pre-FOMC drift (024),
auction cycle (133/134) and TIPS auction cycle (too close to 133).
Protocol reminders that apply to every card: TRAIN 2000-14 (or from data start), VALID 2015-22, one holdout look only
if the card passes the gate; lag-1 for signals formed at a close; register every variant listed (and no others).
The arXiv API was rate-limited ("Rate exceeded") during this round, so sources come from web search. Each source was
confirmed through its publisher, RePEc or a Fed page.

---

## P1. European-open overnight drift after closing sell pressure (dealer inventory)
- **Source:** Boyarchenko, Larsen & Whelan, "The Overnight Drift", *Review of Financial Studies* 36(9), 2023,
  3502-3547 (NY Fed Staff Report 917): https://www.newyorkfed.org/medialibrary/media/research/staff_reports/sr917.pdf.
  Follow-up: "The Disappearing Overnight Drift", Liberty Street Economics, Jul 2026:
  https://libertystreeteconomics.newyorkfed.org/2026/07/the-disappearing-overnight-drift/
- **Mechanism:** Dealers absorb end-of-day customer selling. They unload that inventory when European liquidity arrives
  (02:00-03:00 ET), which pays a premium in that hour. The premium is larger after negative closing order imbalances.
  In E-minis the hour earned about 3.7%/yr in 1998-2020 and was flat in 2021-25.
- **Test:** HistData SPX CFD 1-min, 2010-22 (fixed-EST clock; convert to NY local time). Signal from the prior day's
  15:00→16:00 ET return, a proxy for closing imbalance because signed volume isn't available.
  - A: long 02:00→03:00 ET every day. Report timing value against the mean of all other overnight hours (17:00-09:00).
  - B: long 02:00→03:00 only when the prior last-hour return is < 0.
  - C: −sign(prior 15:00→16:00 return) held 02:00→03:00 (long/short).
  - D: same as B on the NSX (Nasdaq-100) CFD.
  - Cost: 0.5bp/side, and again at 1bp. Window anchored to the Frankfurt 08:00 CET open is a sensitivity check only.
- **Data:** HistData SPXUSD/NSXUSD 1-min (already cached for 287-297).
- **Why new:** 111 tested the whole close→open interval. 112/326 condition on big days and use close→open. 289/290
  used only the US session. No test has isolated the European-open hour or conditioned on last-hour pressure.
  Caveat: the authors report the effect is gone in 2021-25, so expect a weak holdout.

## P2. Closing-auction price pressure reverses after the close
- **Source:** Bogousslavsky & Muravyev, "Who trades at the close? Implications for price discovery and liquidity",
  *Journal of Financial Markets* 66, 2023: https://ideas.repec.org/a/eee/finmar/v66y2023ics1386418123000502.html
- **Mechanism:** Passive, close-benchmarked institutions push the 4pm print. Price deviations from the 4pm midquote
  reverse about halfway within 20 minutes and fully overnight. At the index level, futures and CFDs keep trading after
  16:00 and should give back the close-auction push.
- **Test:** SPX CFD 1-min, 2010-22. Signal: the 15:50→16:00 return (NYSE imbalance publication starts 15:50).
  - A: −sign(15:50→16:00) held 16:00→16:14.
  - B: same signal held 16:00→next-day 09:29.
  - C: A, only when |15:50→16:00| > the TRAIN 80th percentile.
  - D: A on NSX.
  - Cost: 0.5bp/side for A/C/D (one futures tick ≈ 0.5bp).
- **Data:** HistData SPX/NSX 1-min.
- **Why new:** 290 faded the 09:30→15:30 move over 15:30→16:00, which is inside the session. 294 is a month-end
  overnight unwind only. 112/326 use daily big-day moves. No test has used the last-10-minute auction push or
  measured the after-close reversal window.

## P3. Same-half-hour persistence across days (periodic institutional order splitting)
- **Source:** Heston, Korajczyk & Sadka, "Intraday Patterns in the Cross-Section of Stock Returns", *Journal of
  Finance* 65(4), 2010, 1369-1407: https://arxiv.org/abs/1005.3535. Related: Bogousslavsky, "Infrequent Rebalancing,
  Return Autocorrelation, and Seasonality", *Journal of Finance* 71(6), 2016.
- **Mechanism:** Institutions run the same execution schedules (VWAP slices, fixed-time rebalancers) day after day.
  The return in half-hour k therefore predicts half-hour k on later days, at lags of 1, 2, 3 days and up to ~40 days.
- **Test:** SPX CFD 1-min, 2010-22, 13 half-hour bins 09:30-16:00. Position in bin k today = sign(signal), with:
  - A: signal = yesterday's bin-k return.
  - B: signal = mean bin-k return over the last 5 sessions.
  - C: signal = mean bin-k return over the last 20 sessions.
  - D: C traded only in the first (09:30) and last (15:30) bins, where institutional volume concentrates and costs
    matter least.
  - Cost: 0.5bp/side per bin traded, and report gross. Expect A-C to be cost-dominated.
- **Data:** HistData SPX/NSX 1-min.
- **Why new:** 288 tested within-day momentum (morning → last half-hour). Nothing has tested the cross-day,
  same-time-of-day periodicity that HKS document.

## P4. Volatility is cheap intraday and expensive overnight (option-dealer day/night pricing)
- **Source:** Muravyev & Ni, "Why do option returns change sign from day to night?", *Journal of Financial
  Economics* 136(1), 2020, 219-238: https://ideas.repec.org/a/eee/jfinec/v136y2020i1p219-238.html
- **Mechanism:** Delta-hedged S&P option returns are about −1%/day close→open and +0.3%/day open→close. Market makers
  price as if overnight variance were as large as intraday variance, so long-vol positions bleed overnight and earn
  intraday. VIX-futures ETPs should inherit part of this pattern.
- **Test:** Yahoo VIXY daily OHLC, 2011-22 (TRAIN 2011-14 only). Real tradable prices; never use the ^VIX open, which
  is an opening-quote artifact.
  - A: short VIXY close→next open, flat intraday. Report timing value against always-short.
  - B: long VIXY open→close every day (should be positive if the theory holds and short-vol carry is excluded).
  - C: A − B combined book (short night / long day).
  - D: A only when VIX9D/VIX < 1 (contango, normal regime).
  - Cost: 1bp/side and 3bp/side.
  - Descriptive only: ^VIX close→open vs open→close changes 2000-22 (not tradable).
- **Data:** Yahoo VIXY (SVXY as a mirror check; note the 2018 leverage change). CBOE VIX9D CSV.
- **Why new:** 219 is weekday seasonality. 082/186/189/255 are calendar or event timing of short vol. 111 split
  overnight/intraday for SPY only. No test has split volatility products day vs night.

## P5. Dealer gamma regime from the monthly roll: momentum when dealers are short gamma
- **Source:** Baltussen, Da, Lammers & Martens, "Hedging demand and market intraday momentum", *Journal of
  Financial Economics* 142(1), 2021: https://www3.nd.edu/~zda/intramom.pdf. Barbon & Buraschi, "Gamma Fragility"
  (WP 2020): https://www.alexandria.unisg.ch/261487
- **Mechanism:** Intraday momentum is present when dealers' net gamma is negative and reversal when it is positive.
  The free proxy uses the monthly roll. At monthly OPEX, end users roll protective puts (dealers short) and
  overwritten calls (dealers long) to strikes near spot. If SPX has since fallen, the dealer-short puts sit near the
  money (negative gamma). If it has risen, the dealer-long calls dominate (positive gamma).
- **Test:** SPX CFD 1-min 2010-22. Regime R = SPX close at t−1 relative to its close on the last monthly OPEX Friday,
  known before the open.
  - A: when R < −2%, trade sign(prev close→15:30) over 15:30→16:00 (momentum).
  - B: when R > +2%, trade −sign(09:30→15:30) over 15:30→16:00 (reversal).
  - C: A + B combined.
  - D: A with R < −4%.
  - Cost: 0.5bp/side.
- **Data:** HistData SPX 1-min. Monthly OPEX calendar (rule-based third Friday, holiday-adjusted, as in 001/021).
- **Why new:** 212/288/290 tested momentum and reversal unconditionally (tiny/dead). 321/322 tested OPEX-week drift
  and pinning. Conditioning on a pre-specified gamma-sign proxy is the paper's actual prediction and hasn't been tried.

## P6. TIPS seasonal inflation accrual (NSA-CPI indexation mechanics)
- **Source:** Gapen, "Seasonal indexation bias in US Treasury Inflation-indexed Securities", *Applied Financial
  Economics* 13(7), 2003, 509-516: https://ideas.repec.org/a/taf/apfiec/v13y2003i7p509-516.html. Index-ratio rules:
  TreasuryDirect CPI/index-ratio tables.
- **Mechanism:** TIPS principal accretes daily by the not-seasonally-adjusted CPI with a 3-month lag. The accrual for
  month M is CPI_NSA(M−2)/CPI_NSA(M−3) − 1, which is published before month M starts. It is strongly seasonal: high in
  Feb-May accrual months, low or negative in Nov-Dec. If quoted real yields don't fully offset this known carry, TIP
  vs a nominal of similar duration earns it.
- **Test:** monthly, decision at the prior month-end. TIP vs IEF (duration about 7-8 vs about 7.5, so roughly matched;
  also report beta-hedged).
  - A: long TIP / short IEF in months where the known accrual minus SA-CPI m/m (the seasonal part) is > 0, reversed
    when < 0.
  - B: sized by the known total accrual (NSA m/m) instead of its seasonal part.
  - C: A with STIP-like short TIPS using VTIP (2012+), where the seasonal effect should be largest per Gapen.
  - D: control: same rule on IEF alone (should be zero).
  - TRAIN Jan 2004-2014.
- **Data:** FRED CPIAUCNS and CPIAUCSL (use ALFRED vintages for release timing). Yahoo TIP/IEF/VTIP.
- **Why new:** TIPS appear only in the month-end bond-bid tests (280) and issuance dose (298). Nothing has used
  CPI-indexation carry.

## P7. Pension funded-status glide paths buy long duration (LDI demand)
- **Source:** Klingler & Sundaresan, "An Explanation of Negative Swap Spreads: Demand for Duration from Underfunded
  Pension Plans", *Journal of Finance* 74(2), 2019, 675-710: https://ideas.repec.org/a/bla/jfinan/v74y2019i2p675-710.html.
  Greenwood & Vayanos, "Price Pressure in the Government Bond Market", *AER P&P* 100(2), 2010:
  https://researchonline.lse.ac.uk/id/eprint/28618
- **Mechanism:** Corporate DB plans run de-risking glide paths. When funded status improves (equities up, discount
  rates up), they sell equities and buy long-duration bonds and receiver swaps. That demand concentrates at the long
  end and flattens 10s-30s. Pension duration demand moves 30y but not short spreads (K-S).
- **Test:** monthly, signal at month-end t, hold month t+1. Funded-status proxy FS = trailing-3-month SPY total return
  minus TLT total return (assets equity-heavy, liabilities long-duration).
  - A: when FS is in the top TRAIN tercile, long TLT − 1.9×IEF (duration-neutral flattener).
  - B: FRED version back to 1993: Δ(DGS20 − DGS10) in month t+1 vs FS from ^GSPC and DGS20 (DGS30 has a 2002-06 gap).
    Reported as yield bp, descriptive pre-2000.
  - C: A only in Q4/Q1 (year-end measurement-date de-risking).
  - D: continuous version, position ∝ z(FS).
  - Cost: 0.5bp/side per leg.
- **Data:** Yahoo SPY/TLT/IEF. FRED DGS10/DGS20.
- **Why new:** 256 was a month-end-day TLT−IEF curve trade on the MTD stock-bond gap. 013/250 are 1-day rebalancing
  effects. Nothing has tested a month-ahead long-end flattener from funded-status changes.

## P8. Risk-parity deleveraging when the stock-bond correlation flips positive
- **Source:** ECB Financial Stability Review May 2020, Box "Volatility-targeting strategies and the market sell-off":
  https://www.ecb.europa.eu/pub/financial-stability/fsr/focus/2020/html/ecb.fsrbox202005_02~f6616db9be.en.html.
  IMF blog (Adrian, Kramer, Malik), "Stock-Bond Diversification Offers Less Protection From Market Selloffs", Feb 2026:
  https://www.imf.org/en/blogs/articles/2026/02/18/stock-bond-diversification-offers-less-protection-from-market-selloffs
- **Mechanism:** Risk-parity books lever a stock-bond mix to a volatility target. When stock-bond correlation turns
  positive, portfolio volatility jumps even if single-asset vols don't. The rule forces sales of both legs over the
  next days. This is a different trigger from equity-vol spikes.
- **Test:** daily, lag-1. Trigger = 21d SPY-TLT daily-return correlation crosses above +0.2, having been < 0 at some
  point in the prior 63 sessions (first crossing in 42 sessions).
  - A: short 50/50 SPY+TLT for 10 sessions.
  - B: short TLT alone for 10 sessions (the levered leg in RP is bonds).
  - C: A only when the 21d vol of the 50/50 mix is above its 1y median.
  - D: placebo: correlation crossing below −0.2 → long 50/50.
  - Report timing value vs exposure-matched buy-and-hold. Cost: 0.5bp/side.
- **Data:** Yahoo SPY/TLT (2002+). FRED DGS10 as a back-cast for the bond leg.
- **Why new:** 018/325 use equity-vol triggers. 122 is a vol-control overlay. 226 is gold month-end. 184 is weekly
  rebalancing. No test has used a correlation-regime trigger.

## P9. Intermediary (broker-dealer) capital shocks → risk premia in dealer-intermediated assets
- **Source:** He, Kelly & Manela, "Intermediary asset pricing: New evidence from many asset classes", *Journal of
  Financial Economics* 126(1), 2017, 1-35: https://bfi.uchicago.edu/wp-content/uploads/jfepublishedversion.pdf
- **Mechanism:** Primary dealers are the marginal investors in credit, Treasuries and options. Negative shocks to
  their equity capital force balance-sheet cuts, and the assets they intermediate then earn higher expected returns as
  capital rebuilds. HKM price this at about 10%/quarter per unit shock.
- **Test:** monthly, signal at month-end. D = 1-month return of an equal-weight dealer-parent basket (GS, MS, JPM, C,
  BAC; flag survivorship: Lehman/Bear/Merrill are missing) minus SPY.
  - A: after D in the bottom TRAIN quintile, long HYG − duration-matched IEF next month.
  - B: same for LQD − IEF.
  - C: same for long SPY (timing value).
  - D: continuous HYG−IEF position ∝ −z(D).
  - Cost: 1bp/side for credit ETFs. Train 2007-14 for HYG (2003-14 for LQD).
- **Data:** Yahoo dealer stocks, SPY, HYG, LQD, IEF.
- **Why new:** 205 used credit-fund outflow proxies (own past HYG returns). 299 used CFTC dealer positions. 304 used
  Fed liquidity. Nothing has used dealer equity capital as the state variable.

## P10. Commercial-bank securities buying (H.8) as weekly Treasury/MBS demand
- **Source:** Drechsler, Savov & Schnabl, "Banking on Deposits: Maturity Transformation without Interest Rate Risk",
  *Journal of Finance* 76(3), 2021 (banks as structural duration buyers funded by deposits):
  https://onlinelibrary.wiley.com/doi/10.1111/jofi.13013. Fed H.8 release (series TASACBW027SBOG):
  https://fred.stlouisfed.org/series/TASACBW027SBOG
- **Mechanism:** Banks are the largest price-insensitive holders of Treasuries and agency MBS. Weeks of heavy bank
  securities buying (deposit inflows) or selling (deposit flight, as in 2022-23) are demand flows that may persist
  across the following week(s).
- **Test:** H.8 is published Friday 16:15 ET for the week ending the prior Wednesday, so trade from the next Monday
  open → Friday close. Signal z = weekly Δ(Treasury+agency securities, all commercial banks, SA) / trailing-52w std,
  using ALFRED first-release vintages, not revised data.
  - A: long IEF when z > +1, short when z < −1.
  - B: same on MBB (2007+; banks are the main MBS buyers), or MBS sub-series if available on FRED.
  - C: 4-week cumulative Δ instead of 1-week.
  - D: control: same rule on SPY (should be ~0).
  - Cost: 0.5bp/side.
- **Data:** FRED/ALFRED H.8 weekly (1973+). Yahoo IEF/MBB/SPY.
- **Why new:** 304 (Fed net liquidity), 233 (POMO), 299/300 (CFTC) and the earlier scout's custody/dealer cards cover
  other holders. Bank portfolio flows haven't been used.

---
**Ranking by expected power and data cleanliness:** P2, P1, P5 and P4 use data already cached (1-min CFDs, VIXY)
and have thousands of events. P6 and P10 are mechanical and cheap to run. P7, P8 and P9 are monthly or episodic with
fewer independent observations, so judge them strictly at N≈450 (luck t ≈ 3.5).
