# Idea scout: 8 options-market and ETF-structure mechanics ideas (2026-11-21, NOT tested)

Scope: mechanics of option markets, option-writing funds, index rebalancing and ETF creation/redemption. No
psychology or sentiment ideas. I checked every card against BACKLOG.md, all 345 LOG.md headings (with the full text of
the related entries: 009, 019, 021, 080-087, 140, 151/154, 178, 180, 186/189, 205, 206, 219, 220, 245, 291/292, 297,
301, 314), `results/ideas_behaviour.md` and `results/ideas_papers.md`.

I dropped these as duplicates or near-duplicates:
- VIX contango/backwardation timing of short VIXY (082b), VIX-ETP rebalancing (154), VIX expiry (032), VIX weekday (219).
- Fear premium (081), SKEW (086), put/call (314) and VIX futures positioning (301).
- MSCI (178), Russell June recon (019), quad witching (180), S&P additions (140), commodity/USO roll (037).
- Dealer gamma from the monthly roll (P5).

Russell's semi-annual reconstitution starts in Dec 2026, so it has no usable history yet.

Protocol for every card: TRAIN 2000-14 (or from data start), VALID 2015-22, and one holdout look only if the card passes
the gate. Signals are known at the prior close (lag-1) unless the card says otherwise. Register exactly the listed
variants.

Data probes done for this note on 2026-11-21:
- The CBOE CSVs for COR3M, COR1M (2006+), VVIX (2006+), VXTH (2006+), WPUT, BXN (2009+) and BXNT (1994+) download.
- KCJ, ICJ and BXMT return AccessDenied.
- Yahoo ex-dividend dates confirm that SPY, IVV and VOO go ex on different days, e.g. 2026-09-18 / 09-15 / 09-28.
- iShares' historical-NAV download returns an HTML site-entry page to scripts. I did not try to get around it.
- FRED's ICE BofA bond total-return indices only go back 3 years. So card 1 is built to need no NAV data.

---

## 1. Twin bond-ETF relative-discount reversion (HYG vs JNK, plus muni and EM twins)
- **Source:**
  - Haddad, Moreira & Muir, "When Selling Becomes Viral: Disruptions in Debt Markets in the COVID-19 Crisis and the
    Fed's Response", NBER WP 27168 (2020): https://www.nber.org/papers/w27168.pdf
  - Pan & Zeng, "ETF Arbitrage under Liquidity Mismatch" (the slow-arbitrage mechanism).
  - ICE note that bond-ETF premium/discount corrections take several days, compared with about one day for equity
    ETFs: https://www.ice.com/insights/market-pulse/exchange-traded-funds-in-volatile-markets
- **Mechanism:** Bond ETFs absorb selling faster than their illiquid baskets. Authorized participants (APs) redeem
  slowly and at a cost, so one fund can trade at a temporary discount. Two funds holding nearly the same bonds (HYG,
  JNK) get different flow shocks and have different AP capacity. The gap between their returns is mostly a
  premium/discount difference, and it should close as creation/redemption arbitrage catches up.
- **Test** (close→close, lag-1, 2bp/side per leg):
  - Residual e_t = r_HYG − β·r_JNK, with β estimated on a trailing 250 days. Signal = z-score of the 3-day sum of e
    (trailing 250-day sd), known at the close.
  - A: when |z| > 2, short the rich leg and long the cheap leg (β-neutral), hold 3 sessions.
  - B: A, only when HYG is in a 5-day drawdown below −2% (stress, when AP capacity binds).
  - C: A on the muni twin MUB vs VTEB (2015+, valid only, descriptive).
  - D: A on the EM-debt twin EMB vs PCY.
  - Report the P&L of the pair and of each leg.
- **Data:** Yahoo HYG (2007), JNK (2007-12), MUB, VTEB, EMB, PCY, using adjusted closes. The ex-dividend dates differ
  between funds, so use adjusted closes or the dividend-corrected raw return.
- **Why new:** 205 traded HYG/LQD against IEF after bad weeks, which is outright credit continuation. 012/167/277 used
  credit as a signal or a month-end bid. None of them tested premium/discount convergence between two funds holding the
  same asset.

## 2. Nasdaq-100 covered-call roll: dealer net-delta flow on the Thursday before OPEX
- **Source:**
  - QYLD index methodology summary (Cboe Nasdaq-100 BuyWrite V2, BXNT): "the option position is liquidated at a VWAP
    price a day prior to its expiration, generally the Thursday preceding the third Friday".
    https://assets.globalxetfs.com/funds/documents/qyld/Index-Methodology-Summary.pdf
  - The original BXN methodology (Friday roll, new call priced at the 11:30-13:30 ET VWAP after the AM SOQ):
    https://indexes.nasdaqomx.com/docs/BXN_Methodology.pdf
- **Mechanism:** QYLD (about $8bn) buys back last month's call and sells a new ATM one-month NDX call on the roll
  Thursday. A dealer who was long the old call is short about Δ_old × notional of NDX as a hedge, and the new ATM call
  makes it short about 0.5 × notional. The net hedge trade is (Δ_old − 0.5) × notional. That means NDX buying when NDX
  is above the old strike and selling when it is below, concentrated in the roll session.
- **Test** (signal known at Wednesday's close: m = NDX close on Wednesday / NDX close on the previous roll Thursday − 1,
  a proxy for the old strike; S = sign(m)):
  - A: S × (QQQ − β·SPY) open→close on roll Thursdays (the SPX leg isolates the NDX-specific flow).
  - B: S × NSX CFD 12:00→16:00 on roll Thursdays (HistData 1-min, NY local time). This assumes the VWAP is weighted
    toward the afternoon; it is descriptive, because the exact window is not published.
  - C: A only when |m| > 2% (Δ_old clearly near 0 or 1).
  - D: dose-response check, A split into 2014-18 (QYLD < $1bn) vs 2019-22 ($1-8bn). Pre-specified expectation: the
    effect grows with AUM. A placebo using the same rule on the Thursday one week earlier should be about 0.
  - Cost: 1bp/side per leg.
- **Data:**
  - Yahoo QQQ, SPY, ^NDX.
  - HistData NSXUSD and SPXUSD 1-min.
  - OPEX calendar from 001/021 (Thursday = the session before the monthly third Friday, holiday-adjusted). QYLD has
    traded since Dec 2013, so TRAIN is almost empty: use 2014-18 as "train" and record that as a deviation.
- **Why new:** 001/021/292/297 study OPEX Friday and OPEX week unconditionally. 245 is Thursday close → Friday open.
  This card uses a different day and a signed, flow-sized prediction (the sign depends on moneyness against the last
  roll), on NDX relative to SPX.

## 3. Nasdaq-100 annual reconstitution (December): additions run-up and post-effective reversal
- **Source:**
  - Nasdaq research on NDX additions (announcement effects are larger than effective-day effects; more than 1%
    market-adjusted from 5 days before announcement to after inclusion, 2010-20):
    https://www.nasdaq.com/docs/2021/04/22/EN.pdf
  - EFMA 2018 event study of NDX reconstitutions 1997-2015:
    https://efmaefm.org/0EFMAMEETINGS/EFMA%20ANNUAL%20MEETINGS/2018-Milan/papers/EFMA2018_0575_fullpaper.pdf
  - Press release format: https://www.nasdaq.com/press-release/annual-changes-nasdaq-100-indexr-2025-12-13
- **Mechanism:** QQQ, QQQM and other NDX trackers (about $400bn+) must buy additions and sell deletions at the close
  before the effective date (the third Friday of December, effective the next Monday). The announcement comes about
  1 week ahead, after the close on the second Friday, so liquidity providers pre-position and then unwind after the
  index trade.
- **Test** (abnormal return vs QQQ, 3bp/side per leg, equal-weight baskets per year):
  - A: long additions / short QQQ from the open after the announcement to the effective-date close (front-run window).
  - B: short additions / long QQQ for 5 sessions after the effective-date close (reversal, mirroring 140).
  - C: long deletions / short QQQ for 5 sessions after the effective-date close (forced-selling rebound).
  - D: A, restricted to the top half of additions by expected index demand / ADV, where expected demand =
    (QQQ AUM proxy × pro-forma weight). Use the announcement-day market cap / NDX market cap, with the 20-day ADV
    known at the announcement.
- **Data:**
  - Additions, deletions and dates from Nasdaq's annual press releases (nasdaq.com / GlobeNewswire, 2001+) and
    Wikipedia's "Nasdaq-100" changes table.
  - Yahoo prices.
  - About 6-10 additions per year, so roughly 100 events in 2001-22. Deleted stocks that were later delisted are missing
    from Yahoo: log the coverage, as 140 did.
- **Why new:** 140 was S&P 500 additions, 178 MSCI, 019 Russell June and 180 the quad-witching close in aggregate.
  Constituent-level NDX reconstitution has not been tested, and its announcement-to-effective schedule (one week) is
  different from S&P's.

## 4. Option-implied correlation (Cboe COR3M) and the correlation risk premium → market and put-write returns
- **Source:**
  - Buss, Schönleber & Vilkov, "Expected Correlation and Future Market Returns", CEPR DP12760 (2018):
    https://repec.cepr.org/repec/cpr/ceprdp/DP12760.pdf
  - Driessen, Maenhout & Vilkov, "The Price of Correlation Risk", JF 2009:
    https://www.netspar.nl/wp-content/uploads/061_Driessen.pdf
- **Mechanism:** Index puts are bid by portfolio hedgers while single-stock calls are sold by overwriters and
  dispersion desks. That makes index-option-implied correlation sit above realized correlation (a correlation risk
  premium). High implied correlation means hedgers pay a lot for diversification-failure insurance, which predicts
  higher market and put-writing returns over 1-12 months.
- **Test** (monthly decision at the last close of the month, terciles fixed on TRAIN 2006-14):
  - A: SPY next-month excess return when COR3M is in the top tercile, against exposure-matched always-long (timing
    value).
  - B: the same rule with CRP = COR3M − realized 63-day average pairwise correlation of the 9 original sector SPDRs.
  - C: CBOE PUT-index excess return over the next month by CRP tercile (long top / flat bottom). This uses real option
    prices: does put-writing earn the correlation premium?
  - D: A at a 3-month horizon (non-overlapping quarterly decisions).
- **Data:**
  - cdn.cboe.com COR3M_History.csv and COR1M_History.csv (2006+, probed OK).
  - Yahoo XLB/XLE/XLF/XLI/XLK/XLP/XLU/XLV/XLY and SPY.
  - data/cboe/PUT.csv (cached).
- **Why new:** 081 tested the variance premium (VIX − RV), 086 SKEW and 314 put/call. Correlation (index vs component
  vol) is a different part of the vol surface and has not been tested.

## 5. Volatility-of-volatility (VVIX) → tail-hedge returns
- **Source:** Park, "Volatility of Volatility and Tail Risk Premiums", Federal Reserve FEDS 2013-54:
  https://www.federalreserve.gov/pubs/feds/2013/201354/index.html
- **Mechanism:** When VVIX is high, tail-hedge buyers bid up VIX calls and deep OTM SPX puts beyond fair value. Dealers
  short vol-of-vol charge more, and subsequent tail-hedge returns are lower for 1 day to about 4 weeks. A 1-sd VVIX
  shock lowers next-day SPX put returns by 1.3-2.2% and VIX call returns by 0.7-1.0%.
- **Test** (lag-1, decisions at the close, terciles fixed on TRAIN 2006-14):
  - A: short VIXY for 5 sessions when VVIX / its 60-day mean is in the top quintile, as timing value against
    exposure-matched always-short (5bp/side).
  - B: protective-put leg return = PPUT − SPX total return (from the cached CBOE PPUT and the S&P TR), next 20 sessions,
    by VVIX tercile. The prediction is that the leg is cheapest to own (lowest return) in the top tercile: underweight
    hedges then.
  - C: B using VVIX/VIX (vol-of-vol per unit of vol) instead of the VVIX level.
  - D: VXTH (Cboe VIX tail-hedge index, SPX + VIX calls) minus SPX TR over the next 20 sessions, by VVIX tercile.
- **Data:**
  - cdn.cboe.com VVIX_History.csv and VXTH_History.csv (2006+).
  - data/cboe/PPUT.csv.
  - Yahoo VIXY (2011+), ^VIX, ^SP500TR.
- **Why new:** 082 conditions on VIX spikes and VIX/VIX3M, 081 on VIX − RV, 086 on SKEW. Nothing so far has used
  vol-of-vol, which is the pricing of the VIX-option layer itself.

## 6. SPY ex-dividend day vs its S&P 500 twins (IVV, VOO): is the ex-day drop smaller than the dividend?
- **Source:**
  - Ruan & Ma, "Ex-Dividend Day Price Behavior of Exchange-Traded Funds", J. Financial Research 35(1), 2012:
    https://ideas.repec.org/a/bla/jfnres/v35y2012i1p29-53.html
  - Boyd & Jagannathan, "Ex-Dividend Price Behavior of Common Stocks" (Minneapolis Fed):
    https://www.minneapolisfed.org/research/staff-reports/exdividend-price-behavior-of-common-stocks
- **Mechanism:** SPY (a UIT) goes ex on the third Friday of Mar/Jun/Sep/Dec. IVV goes ex about 3 days earlier and VOO
  about a week later (confirmed on Yahoo), and all three hold the same basket. On one fund's ex-date, the total-return
  difference against its twin isolates the ex-day price drop relative to the dividend. That drop is set by tax
  clienteles and dividend-capture arbitrage, including deep-ITM call early exercise the day before ex.
- **Test** (spread trades, 1bp/side per leg; raw return = (P_ex + D)/P_prev − 1):
  - A: long SPY / short IVV from the close before SPY's ex-date to the ex-date close.
  - B: mirror, long IVV / short SPY across IVV's ex-dates.
  - C: mirror across VOO's ex-dates (2010+).
  - D: overnight only (close → ex-date open) for A-C pooled, which is where the drop is priced.
  - A pre-specified sign check: the spread is positive if the drop is smaller than the dividend. Also report the drop
    ratio ΔP/D.
- **Data:** Yahoo unadjusted OHLC and the dividends event feed for SPY, IVV and VOO (2000+, 2010+ for VOO). About 80
  SPY events in 2000-22, and over 200 pooled.
- **Why new:** 114 (pay-date reinvestment) is parked and 234 tested SPY dividend reinvestment at month-end. No test has
  looked at ex-date price mechanics, and none has used the same-index twin funds to cancel out market risk.

## 7. Scheduled-event volatility premium in real option prices (FOMC/CPI weeks)
- **Source:**
  - Londono & Samadi, "The Price of Macroeconomic Uncertainty: Evidence from Daily Options", Fed IFDP 1376 (2023).
    Variance risk premia for options spanning FOMC are 44% larger than for control options, and implied vol drops after
    announcements. https://www.federalreserve.gov/econres/ifdp/files/ifdp1376.pdf
  - Wright, NBER WP 28306 (one-day option variance premia, largest on FOMC days):
    https://www.nber.org/system/files/working_papers/w28306/w28306.pdf
- **Mechanism:** Hedgers pay up for options that span scheduled macro releases, so weekly options covering an
  FOMC/CPI date carry extra premium that sellers collect when the event passes. Implied vol mechanically drops once the
  event variance is gone.
- **Test:**
  - A: CBOE WPUT (weekly ATM put-write, Fri→Fri) exposure only in weeks containing a scheduled FOMC announcement,
    against exposure-matched always-on writing (timing value; 2006-22, train 2006-14).
  - B: A for weeks containing a CPI release. Release dates come from ALFRED vintage dates (realtime_start) of CPIAUCSL.
  - C: short VIXY over the FOMC day only (Tuesday close → Wednesday close), 5bp/side, 2011+. This is the implied-vol
    crush on announcement.
  - D: A only when VIX9D/VIX at the Thursday close before the week is in its top half (event premium already loaded).
- **Data:**
  - data/cboe/WPUT.csv (cached).
  - FOMC dates from tools/fomc_dates.csv (024).
  - ALFRED API for CPI release dates.
  - Yahoo VIXY, ^VIX9D, ^VIX.
- **Why new:** 024 traded SPY on FOMC days, 025 SPY on jobs days, and 220 split WPUT by weekday. None measured the
  option premium around scheduled events with real option prices or a VIX-ETP crush.

## 8. Country-ETF premium reversal after home-market holiday closures (creation/redemption suspended)
- **Source:**
  - Franklin Templeton, "When markets are closed, what is your international ETF really worth?" (2026):
    https://www.franklintempleton.com/articles/2026/etf/when-markets-are-closed-what-is-your-international-etf-really-worth
  - Franklin Templeton ETF capital-markets desk on trading through Chinese New Year:
    https://www.franklintempleton.com/articles-us/liberty-shares/etf-capital-markets-desk-on-trading-over-the-chinese-new-year-and-other-celebrations
  - GCC country-ETF mispricing and NAV predictability study: https://mdpi.com/1911-8074/17/10/459
- **Mechanism:** While the home market is shut for multi-day holidays (Lunar New Year, Golden Week, Chuseok, Carnival),
  US-listed country ETFs keep trading. APs cannot create or redeem against a live basket, and market makers hedge with
  proxies. The ETF's closure-period move is unarbitraged price discovery that includes US-flow noise. When the home
  market reopens, the basket either confirms the ETF move or the ETF gives part of it back.
- **Test:**
  - Closure windows are defined automatically as runs of ≥2 consecutive US trading days with no home-index bar.
  - Excess move X = ETF return over the closure window − β·SPY return (β trailing 250 days), known at the last US close
    before the home reopen.
  - Hold the ETF from that close to the next US close, after the home market has reopened overnight (close→close,
    3bp/side, SPY-hedged).
  - A: fade, −sign(X), pooled across ASHR (2013+), FXI/EWH (HK closures), EWT, EWY, EWJ and EWZ, for |X| > 1%.
  - B: A restricted to Lunar New Year closures (China, Taiwan, Korea, HK).
  - C: A with |X| > 2%.
  - D: A held to the second US close after the reopen.
- **Data:** Yahoo ETFs and home indices (000300.SS, ^HSI, ^TWII, ^KS11, ^N225, ^BVSP) to detect closures; SPY.
  Roughly 150-250 pooled events in 2002-22.
- **Why new:** 002 tested daily catch-up on regular days and 028 only Japan's fiscal year-end. The China/HK closure note
  in the 028 backlog line was never run. This card is about premium mechanics when creation/redemption is suspended,
  not ordinary overnight catch-up.

---

**Ranking by expected power and data cleanliness:**
- 6 (clean, market-neutral, many events) and 2 (signed, sized flow; cached 1-min data).
- 4 and 5 (real option-index data, though TRAIN only covers 2006-14).
- 1 (pair trade, no NAV needed).
- 7.
- 3 (needs hand-built event lists).
- 8 (event detection is automatic, but the sample is noisy).

**Caveats:**
- Card 2's exact VWAP window for the V2 roll is not published, so its intraday variant B is descriptive.
- Card 1 would be stronger with real NAV history, but the iShares NAV download needs interactive site entry. Ask the
  user before using any other NAV source.
