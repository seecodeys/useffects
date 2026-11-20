# Backlog (top = next). Mark `[x]` when done, `[~]` in progress.

## PARTICIPANT BEHAVIOR / FLOWS (user focus 2026-11-20, revised: behavior not psychology) — do these first
- [x] 110 Turn-of-month fund inflows (401k/payroll contributions), close→close: long SPY from the close of the 2nd-last
      trading day through the 3rd trading day of the new month (classic TOM window) vs rest; timing value.
- [x] 111 Overnight vs intraday return split (who trades when): always-long SPY close→open vs open→close, by era; and
      a long-overnight / flat-intraday strategy with realistic costs.
- [x] 112 Leveraged-ETF rebalancing: on |day return| > 2% days LETFs buy/sell into the close → overshoot → reversal
      overnight (close→open) and next day. Pre-specified reversal.
- [x] 113 Corporate buyback blackout windows: buybacks pause ~2 weeks before earnings until results; calendar proxy
      (approx. weeks 2-6 after quarter-end = peak blackout) → SPY close→close weaker in blackout vs open window.
- [x] 118 Quarter-end rebalancing anticipation: does the stock-bond rebalance at QUARTER-ends show up on the
      2-3 days BEFORE quarter-end (front-running) rather than the last day? Pre-specified: −sign(gap) on days −3..−2.
- [x] 120 [paper 2405.12768 'Ponzi Funds'] flow-chasing price pressure: sector/country ETFs with high trailing 3-month
      return AND a volume surge (flow proxy) → subsequent under-performance; monthly L/S, survivorship-free.
- [x] 121 [paper 2608.03703 'Preying on Leveraged ETFs'] US analog with Yahoo 1h bars (~2y, descriptive): SPY/QQQ move
      through 15:00 → same-direction last hour (pre-positioning for LETF close rebalancing) → next-open reversal.
- [x] 122 [paper 2609.07946] vol-controlled SPY/TLT/GLD mix vs static — risk overlay (Sharpe/drawdown), not alpha.
- [x] 123 [papers 2504.20116, 2506.19200] LETF trend filter: hold SSO/QLD when trailing 63d index return > 0 else 1x —
      vs static; low priority (trend-following, not a flow).
- [x] 117 LETF rebalancing, sharper: scale the signal by estimated LETF rebalancing demand = (sum of 2x/3x
      SPY/QQQ ETF AUM proxies × leverage×(leverage−1) × day return); test close→open and next-day; 2009+.
- [~] 114 Dividend reinvestment (parked: no free pay-date data) flows (Hartzmark-Solomon): days with large aggregate dividend payments see price
      pressure up; proxy with SPY/large-cap pay-date clustering (quarter's last-business-day pay dates) close→close.
- [x] 115 Equity index futures quarterly roll week (week before 3rd-Friday of Mar/Jun/Sep/Dec): calendar-spread arbitrage
      flows → SPY/IWM returns in roll week.
- [x] 116 Quarter-end vs month-end rebalancing size: does 013's edge scale with fund size proxies (e.g. calendar
      quarter-ends, year-end) — interaction test.

- [x] 127 013 held close→close — no improvement.
- [x] 129 Mid-month rebalancing — dead.
- [x] 133 Treasury auction cycle (TLT) — gate passed but concentrated in 2009-10/2020; no holdout.
- [x] 134 Auction cycle conditional on rate volatility: run 133c only when TLT 20d realized vol > its trailing 1y median
      (dealer stress regime) — pre-specified; also on IEF with 10y auctions only.
- [x] 135 Second paper-sourcing round (nothing new on arXiv)
- [x] 151 CTA MA-cross flows — dead.
- [x] 154 VIX-ETP rebalancing reversal — dead.
- [x] 155 Strategy-catalog sourcing (Quantpedia; thin)
- [x] 165 Mid-month payday — dead.
- [x] 013f AGG bond leg — weaker, same sign.  - [x] 013g SPY−TLT pair — survived holdout.
- [ ] 166 Month-end flow in the closing auction: does 013's edge concentrate at the close? Use Yahoo 1h bars to split
      descriptive 2023-26 only (holdout-era → descriptive) — already seen in 013c (last hour 81% hit); skip unless new data.
- [x] 176 Quarter-end bank balance-sheet (TLT) — dead.
- [x] 177 Tax-deadline flows — weak.
- [x] 178 MSCI index review effective days (last business day of Feb/May/Aug/Nov): EEM/EFA vs SPY open→close and
      close→next open (global index-fund rebalancing); direction from train.
- [x] 180 Quad-witching close reversal — dead.
- [x] 181 Third paper-sourcing round (NBER JS-only; S2/OpenAlex 429; Crossref works) → 182 dead.
- [x] 184 Weekly stock-bond rebalancing — dead.
- [x] 186/189 VIX-futures flow timing — dead.
- [x] 190 Month-end rebalancing in options terms (flows into index puts at month-end?): CBOE PUT index daily returns on
      month-end days by stock-bond gap — does put-writing do better on month-ends when stocks lagged (flow buys equity)?
- [x] 197 FX month-end hedge rebalancing — dead/wrong-signed.
- [x] 205 Credit-fund outflow pressure — dead.
- [x] 206 JPM Hedged Equity collar reset (last business day of each quarter, ~$15-20bn SPX put-spread/call collar,
      2016+): SPY behaviour around quarter-end resets — descriptive split of 013's weak quarter-end months (116), then
      one pre-specified test if a pattern is clear in 2016-22.
- [x] 209 Treasury coupon reinvestment — dead (train-only).
- [x] 210 Dash for cash (pre-month-end selling) — dead.
- [x] 211 FOMC-cycle even weeks — train-only, dead.
- [x] 212 Intraday momentum / hedging demand (Gao-Han-Li-Zhou 2018; Baltussen et al. 2021): prev close→10:30 return predicts
      15:30→close, Yahoo 1h bars 2023-26 → descriptive only (holdout era).
- [x] 213 Target-date-fund contrarian flows (Parker-Schoar-Sun, JF 2023): TDFs rebalance after equity moves; monthly signal =
      −sign(SPY 1-month return − AGG 1-month return) applied over the first 3 sessions of the next month (TDF rebalancing
      lag) — distinct from 013's last-day window.
- [x] 219 VIX weekend mechanics — spot only, not tradable via VIXY.
- [x] 220 Options weekend theta via real prices: CBOE PUT/WPUT index daily returns by weekday — do put-writers earn the
      weekend decay on Fridays (close Thu->close Fri) or Mondays? Weekday-of-exposure timing value vs always-on.
- [x] 225 LETF-heavy sector ETFs overnight reversal — dead (undercuts 112's LETF story).
- [x] 226 Risk-parity month-end in gold — dead.
- [x] 233 Fed POMO purchase days — dead.
- [x] 234 SPY dividend reinvestment month-ends — dead.
- [x] 245 SOQ overnight — dead.  - [x] 250 QTD/YTD rebalancing — dead.
- [x] 252 Month-end high-minus-low beta spread — weak, consistent.
- [x] 253 Threshold rebalancers — dead.
- [x] 255 Month-end flow via VIXY — right sign, noisy.
- [x] 013h Permutation tests — p<0.0001.
- [x] 256 Month-end curve (TLT−IEF) — weak, right sign.
- [x] 257 International month-end replication — positive 2015-22 everywhere; Canada holdout failed.
- [x] 258 Japan month-end — right sign, short data.
- [x] 263 BOJ ETF purchase support — tracks BOJ era, not tradable net.
- [x] 264 Pooled international month-end (257) with the 168 volatility filter (local 20d vol > local train median) —
      robustness on train/valid only.
- [x] 267 International OPEX replication — none.
- [x] 268 Settlement-lag rebalancing day — partial (T+3 era only).
- [x] 274 Country month-end rebalancing — dead.
- [x] 275 Bond turn-of-month (TLT last 3 sessions) — gate passed, holdout weak-positive.
- [x] 276 Bond month-end extension by curve point (IEF/SHY t>3 both periods): IEF/SHY last-3-session longs (extension buying should be largest in
      long duration) — consistency check on train/valid only.
- [x] 277/278 Month-end bond bid in credit & international bonds — right sign everywhere.
- [x] 279 Survivor portfolio v2: 013 (SPY month-end, vol-tilted) + 276 IEF last-3-sessions bid + 062b country reversal;
      construction on 2002-22, report correlation/Sharpe (no new holdout looks).
- [x] 280 Month-end bond bid in MBB/TIP/MUB — strong except munis.
- [x] 281 Full month-end bond cycle — reversal leg faded; keep long-only.
- [x] 284 Month-end bond bid in BWX/EMB — right sign, weaker.
- [x] 285 Bond-bid controls (GLD/SPY/UUP) — no bid outside bonds.
- [x] 286 Mid-month Treasury cash flows — dead.
- [x] 287 Month-end intraday timing (SPX CFD 1-min) — edge mostly in the morning; last-hour variant weak.
- [x] 288-290 Intraday momentum / LETF pre-positioning / EOD reversal on SPX 1-min — tiny or dead.
- [ ] 288b (skip) Same intraday timing study for the bond bid using HistData? (no Treasury series on HistData) — skip unless a source appears.
- [x] 295 Replicate 287 (month-end intraday timing) on Nasdaq-100 CFD (HistData NSXUSD, downloading) — descriptive.
- [ ] 183 Retry OpenAlex/Semantic Scholar later (rate limits) for flow papers; Crossref with targeted titles.: web-fetch NBER working-paper search pages (nber.org/papers?q=...) for 'rebalancing',
      'price pressure', 'flows' — empirical asset-pricing papers with daily-testable rules.
- [x] 179 Month-end rule applied to QQQ-vs-TLT and IWM-vs-TLT pairs with the vol filter (168) — robustness only if cheap.
- [x] 167 Month-end flow in *credit* — dead: HYG−IEF spread on the last day by stock-bond gap (balanced funds' credit sleeve)?
- [x] 168 Month-end signal strength (survived holdout as a vol filter) by equity-vs-bond volatility regime (when bonds are volatile the rebalance is
      bigger in $ terms): interaction on TRAIN+VALID only, then a single pre-specified filter if it's strong.: have a web-fetch agent read free strategy summaries (e.g. Quantpedia's free
      strategy pages, Alpha Architect / CXO-style summaries) for flow-driven anomalies not yet tested; add to backlog.
- [x] 142 Fed net-liquidity weekly timing — dead.
- [x] 140 S&P 500 index additions (flow from index funds at the effective-date close): effective dates from Wikipedia's
      'List of S&P 500 companies' changes table; short added stock vs SPY from the effective-date close for 5 sessions
      (post-inclusion reversal); event study of the run-up into the effective date.
- [x] 139 Quarter-end portfolio pumping — dead.: arXiv queries on 'Treasury auction', 'dealer inventory', 'closing auction',
      'index reconstitution', 'ETF creation redemption', 'month-end' for more flow ideas.

## OPTIONS (user request 2026-11-20) — behavioral angle: people overpay for insurance after fear spikes
- [x] 080 Data: pull CBOE strategy-index CSVs (PUT, BXM, CNDR, BFLY, WPUT, PPUT) into data/cboe/; sanity-check vs ^PUT.
- [x] 081 Fear-premium timing of put-writing: PUT/WPUT index returns when VIX−RV fear premium is high vs low
      (excess over always-on writing), monthly decision at prior close.
- [x] 082 Short-vol ETPs after fear over-reaction: short VIXY / long SVXY (−0.5x era) for N days after VIX spikes,
      and when VIX/VIX3M is in contango; behavioral = VIX over-shoots then mean-reverts.
- [x] 083 Model-priced SPY options for the month-end signal (013): sell the put (when long signal) / call (when short)
      vs buying 1-day ATM options — premium (VIX9D-implied) vs realized edge; is options expression better than stock?
- [x] 085 OPEX-Friday tilt (001) via options: sell a same-day ATM call on monthly OPEX Friday (calibrated model, as 083).
- [x] 086 Crash-fear sentiment via CBOE SKEW: high SKEW (tail insurance demand) → subsequent PUT-index / SPY returns
      (contrarian: over-paid tail insurance), monthly.
- [x] 087 Model validation: monthly ATM put-write priced with VIX (BS) vs the real CBOE PUT index — mean/correlation of
      monthly returns, to bound how wrong the model-pricing in 083/085 could be.
- [ ] 084 VIX1D (2023+, short sample): 0DTE implied vs realized — descriptive only (no train/valid possible).

## BEHAVIORAL (psychology) — DEPRIORITISED per user 2026-11-20 (focus is participant behavior/flows, not mood)
Already known: index-level fades (gap fade, prior-day reversal) work only in crises (009/011); single-stock gap fade
is an opening-print artifact (015). New tests use prior-close information only.
- [x] 050 Over-reaction vs news (volume split): yesterday's extreme stock moves vs SPY — low-volume moves (no news,
      liquidity/emotion) should reverse; high-volume moves (news) should continue. Campbell-Grossman-Wang; Cooper.
- [x] 051 Attention-driven buying (Barber-Odean): stocks with abnormal volume + big up-move yesterday attract retail buy
      orders at the next open → price drifts down open→close. Short attention winners (SPY-hedged).
- [x] 052 Streaks / extrapolation: N≥4 consecutive down (up) closes in a stock or sector ETF → next-day open→close reversal.
- [x] 053 Fear over-reaction at the index level, structured: SPY close-to-close drop > 2σ / VIX jump > 25% / both →
      next-day open→close; by era (retail dip-buying post-2020?).
- [x] 054 Anchoring to the 52-week high (George-Hwang): stocks that closed at a new 52w high yesterday → continuation
      next day (investors anchor and under-react); and 52w lows.
- [x] 055 Lottery demand (MAX effect): stocks with the largest single-day return in the last month → overpriced →
      negative open→close drift; and round-number price psychology (closes just below $X0/$X00).
- [x] 056 Sector herding: sector ETF extreme residual move yesterday → next-day open→close reversal (cross-section of 9 SPDRs + industry ETFs).
- [x] 057 Monday mood / weekend rumination: big Friday drop → Monday open→close; by era.


## Overseas / cross-market signals (core theme)
- [x] 001 Calendar & baseline anomalies for SPY/QQQ/IWM open→close (day-of-week, turn-of-month, pre-holiday, FOMC days, OPEX). Establishes baselines other signals must beat.
- [x] 002 Country/sector ETF universe (EWJ, EWG, EWU, FXI, EWT, EWY, SMH, XLE, XLK, XLF, …): does each ETF's open→close depend on its *home* market's move since the last US close vs. its own gap (catch-up / over-reaction per ETF)? Cross-sectional long/short across ETFs.
- [x] 003 Overnight FX & commodities as signals: USDJPY, AUDUSD, EURUSD, DXY, crude (CL=F), gold (GC=F), copper, US 10y (^TNX), BTC-USD moves since last US close → SPY/sector open→close. NOTE: Yahoo daily FX/futures bars can't give 9:30 ET levels → using HistData 1-min (USDJPY, AUDUSD, XAUUSD, WTIUSD 2001-2025, downloading via tools/bulk_histdata.py; 2026 is per-month on the site).
- [x] 004 ADRs: Asian ADRs (TSM, BABA, SONY, TM, HMC, MUFG, INFY, HDB) — open→close vs home close & ADR gap (ADR premium/discount at the open).
- [x] 005 Europe at 9:30 ET, properly: HistData 1-min CFD bars (tools/histdata.py, DAX/CAC/ESTX50/FTSE, 2010→now, fixed-EST clock, rate-limited ~5 downloads then cool-down). Level at 09:29 NY vs prior US close → SPY/EWG/EZU open→close. Train window only 2010-14 here. Also FTSE daily open via ISF.L (Yahoo, 2009+, unadjusted → mask ex-div gaps).

## Cross-sectional intraday
- [x] 006 Overnight-gap reversal in single stocks (large-cap universe, fresh Yahoo data): rank by gap, long losers/short winners open→close; costs ≥2bp/side. (Lou-Polk-Skouras tug-of-war.)
- [x] 007 Overnight momentum vs intraday reversal at the ETF level (sector ETFs): past N-day overnight returns → today's intraday.
- [x] 008 Lead-lag at the open: large-cap gap vs small-cap gap (SPY vs IWM), QQQ vs SPY — does the lagging one catch up intraday?

## Regime / volatility
- [x] 009 VIX term structure (^VIX/^VIX3M, ^VIX9D) and VIX gap overnight → SPY intraday direction / magnitude.
- [x] 010 Prior-day intraday vs overnight decomposition (yesterday's oc and gap) → today's oc, by regime.
- [x] 011 Extreme overseas sell-off rebound (the one weak pattern from global_open_close) — event study with VIX filter.

## New ideas (added after 001-010)
- [x] 012 Credit/rates gaps as risk signals: HYG, LQD, TLT, IEF opening gaps & prior-day moves → SPY open→close (all US-listed, liquid).
- [x] 013 Month-end / quarter-end rebalancing: last 1-2 trading days of month, SPY vs TLT intraday direction conditioned on month-to-date stock-bond performance gap (pension rebalancing story).
- [x] 013c Month-end follow-ups: same pre-specified rule on IWM/QQQ/DIA, SPY-vs-TLT pair, |gap| sizing, intraday timing via 1h bars (last-hour MOC flows?).
- [~] 014 Earnings-season intensity: SPY/QQQ intraday during heavy mega-cap earnings weeks vs not (needs earnings dates — Yahoo events or skip).
- [x] 015 Execution reality check for 004/006 (first-hour open-print reversals): document what evidence would confirm them (MOO paper fills, TAQ-like data); test robustness to entering at 9:35 using Yahoo 5m bars (60 days) — descriptive only.

- [x] 058 (user OK given to relax open→close) Behavioral signals with close→next-close or close→open holds: short-term
      reversal of extreme residual moves, fear rebounds, attention reversal — where the literature says the effect lives.
- [x] 059 Retail-attention proxies available free: Google-Trends-like? (not free/clean) → instead: abnormal volume in
      leveraged/meme-adjacent ETFs (TQQQ/SQQQ volume share) as a crowd-sentiment gauge → SPY next-day open→close (contrarian).
- [x] 060 Put/call-style sentiment from free data: VIX vs realized vol spread (fear premium), VIX9D/VIX inversion as panic
      gauge → contrarian SPY open→close.

- [x] 061 Behavioral, longer horizon (user OK on holds): post-crash over-reaction at the market level — after SPY
      drawdowns of >10% from a 52w high, 20-day forward returns vs normal (sentiment capitulation); and investor-attention
      'news-driven' vs 'no-news' monthly reversal at ETF level (country ETFs, 1-month reversal, survivorship-free).
- [x] 062 Over-reaction to macro surprises across countries: country ETFs after their worst week vs world (EFA/EEM
      components), hold 1-4 weeks (country-level reversal, Balvers-Wu-Gilliland style mean reversion).

- [x] 063 Disposition effect proxy at ETF level: capital-gains overhang ≈ price vs 252-day volume-weighted average
      price (VWAP of the last year); ETFs far above their 'average cost' face profit-taking → lower next-month returns?
      Cross-section of 26 ETFs, monthly rebalance, survivorship-free.
- [x] 064 Herding across countries during global stress: when VIX > 30, do the countries that fell most in the last
      week rebound more (fear-driven indiscriminate selling)? Conditional version of 062a.
- [x] 065 Extrapolation in sentiment-driven assets: GLD/TLT/USO after 3-month extreme moves (top/bottom 5% of history)
      → next-month reversal (time-series), hold 21 days.

- [x] 070 DST weekend Mondays (lost sleep) — dead.
- [x] 072 Lunar phase — dead.
- [x] 069 Weather mood (Hirshleifer-Shumway): NYC morning cloud cover (free Open-Meteo archive API, daily since 1940) →
      SPY open→close on sunny vs overcast mornings; deseasonalised cloud cover, pre-specified sign (sunny = long).
- [x] 067 Retail mood spillover: BTC weekend return (Fri US close → Mon pre-open, from BTC-USD UTC daily bars) →
      Monday SPY/QQQ open→close, 2015+ (crypto as a retail-sentiment gauge).
- [x] 071 Sports-sentiment style: big national events are hard to source; instead US equity reaction on the day after
      large single-day crypto crashes (BTC < −10% UTC day) → QQQ/ARKK open→close (retail risk appetite shock).

- [x] 075 Record-high attention: the session after SPY closes at an all-time high (headline attention) — retail buys at
      the open, price fades → short SPY open→close; also 'first record high in > 6 months' (bigger headline).
- [x] 077 Under-reaction / conservatism (the other side of over-reaction): country ETF 12-1 month momentum
      (Asness-Moskowitz-Pedersen), monthly rebalance, survivorship-free; and sector-SPDR 12-1 momentum.
- [x] 073 Round-number anchoring: S&P 500 (^GSPC) closing within 0.5% below a multiple of 100 / 500 → next-day
      open→close (resistance) vs just above (breakout).

- [x] 090 Seasonal mood ('Sell in May' / SAD, Kamstra): SPY Nov-Apr vs May-Oct timing value (excess over
      exposure-matched B&H), and the autumn-equinox SAD window; few independent obs → strict.
- [x] 091 Tug-of-war (Lou-Polk-Skouras) at index level: after ≥4 consecutive positive OVERNIGHT returns (retail/attention
      buying) → SPY open→close (institutions lean against) — pre-specified short.
- [x] 095 Retail-heavy ETF over-reaction: ARKK / TQQQ / SMH big prior-day moves (|z|>2) → next-day close→close reversal
      (lag-1) vs the same rule on SPY (institutional) — does over-reaction concentrate where retail is?

- [x] 107 Update README.md: behavioral round (050-095) and options round (080-087) results, revised 'edges found' table.
- [x] 096 Portfolio of survivors: 013 (SPY month-end) + 031 (TLT month-end long) + 062b (monthly country reversal) —
      combined daily P&L, correlation, Sharpe, drawdown by period (construction only; no new signal search).
- [x] 103 Micro-cap over-reaction at ETF level (IWC 2005+, IJR): reverse |z|>2 prior-day moves, lag-1 close→close;
      micro-caps are where retail over-reaction should be strongest and IWC has no survivorship issue.
- [x] 100 Is month-end volatility under-priced? Compare realized |open→close| on month-end days vs other days relative
      to prior-close VIX9D (descriptive; informs whether buying options on month-ends could work despite 083c).

## Flow-driven ideas (the 013 theme is the only robust winner so far)
- [x] 016 Quarter-end window-dressing vs rebalancing: last day of quarter, sector/style winners vs losers MTD (XLK vs XLU etc.) — does the losing sleeve get bought? (cross-sectional sector version of 013).
- [x] 017 Month-end rebalancing in other markets with US-listed vehicles: EFA/EEM vs TLT; small-cap vs large-cap (IWM vs SPY) month-to-date gap on last day.
- [x] 018 Vol-target deleveraging: after large realized-vol jumps (e.g. 5d realized vol doubles vs 60d), risk-parity/vol-target funds sell over following days → SPY open→close drift on days 1-3 after.
- [x] 019 Russell reconstitution (last Friday of June) and S&P quarterly rebalance days: IWM vs SPY open→close.

## Next
- [x] 020 Practical tool: `signals.py` that prints today's/next month-end 013 signal (SPY MTD − TLT MTD, direction, size) and the 001 OPEX-Friday tilt, plus a combined backtest/equity curve of 013(+001) with realistic costs.
- [x] 021 OPEX-week structure (from parking lot): Mon-Thu before monthly OPEX, Monday after, quarterly-only — pre-specify 3 variants on QQQ/SPY.
- [x] 022 Write-up: alpha_research/README.md summarising method, all verdicts, the 013 result and caveats (survivorship, costs, holdout contamination).

## Scheduled-event ideas (added 2026-11-20)
- [x] 024 FOMC announcement days (14:00 ET): SPY/QQQ open→close on FOMC days (long; and conditional on pre-FOMC drift). Needs FOMC dates 2000-2026 (fetch from federalreserve.gov → tools/fomc_dates.csv).
- [x] 025 Jobs-report days (8:30 ET, usually first Friday): does the post-release gap reverse or continue intraday? Rule-based first-Friday dates (approximate; note exceptions).
- [x] 026 Pre-holiday/post-holiday half-days (day after Thanksgiving, Christmas Eve) and the "Santa"/year-end window — few events, descriptive.
- [x] 027 Month-end rebalancing, more evidence: does 013's edge scale with the *bond* leg's size (TLT MTD alone) or the *equity* leg? Which one drives it? Also: month-ends when SPY MTD and gap disagree in sign.
- [x] 028 Japan fiscal-year-end (Mar 31) and China/HK holiday closures: EWJ/FXI open→close around those dates (repatriation flows) — few events.
- [ ] 014 (parked: no reliable free historical earnings calendar) — revisit if a source turns up.

- [x] 031 Treasury month-end index extension (TLT/IEF long last day) — weak-consistent, fails gate narrowly.
- [x] 032 VIX-expiry Wednesday short SPY — dead.

- [x] 037 Commodity roll window (short USO bd 5-9) — dead.
- [x] 038 LETF rebalancing proxy (follow big gaps) — dead.
- [x] 039 Corporate-bond index extension at month-end: HYG/LQD long last trading day (companion to 031).
- [x] 040 Month-end effect robustness: does 013 hold in the pre-2002 period using a bond proxy available earlier (e.g. ^TYX/^TNX yield change as bond-return proxy, SPY 1993-2002)? Fresh out-of-sample *backwards*.

## Ideas parking lot
- 004 follow-up: verify ADR opening-auction fills (paper MOO/MOC orders, or tick/quote data); same idea for European ADRs once 9:30 Europe levels exist (005).
- OPEX-week structure follow-up to 001 (Mon-Thu before OPEX, Monday after, quarterly-only, VIX-expiry Wednesdays). Lower priority: each new test raises N.
- Earnings-season effects on SPY intraday; month-end rebalancing flows; quad witching.
- Treasury ETF (TLT) / credit (HYG) gaps as signals for SPY intraday.
- [x] 298 Bond-bid dose-response on coupon issuance — none; keep flat leg.
- [x] 299 CFTC TFF positioning (lev funds / asset mgrs / dealers; ES, TY, US) — dead (C weakly consistent).
- [x] 300 Bond bid vs asset-manager futures length — no dose-response (descriptive).
