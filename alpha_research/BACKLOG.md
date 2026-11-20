# Backlog (top = next). Mark `[x]` when done, `[~]` in progress.

## OPTIONS (user request 2026-11-20) — behavioral angle: people overpay for insurance after fear spikes
- [x] 080 Data: pull CBOE strategy-index CSVs (PUT, BXM, CNDR, BFLY, WPUT, PPUT) into data/cboe/; sanity-check vs ^PUT.
- [x] 081 Fear-premium timing of put-writing: PUT/WPUT index returns when VIX−RV fear premium is high vs low
      (excess over always-on writing), monthly decision at prior close.
- [x] 082 Short-vol ETPs after fear over-reaction: short VIXY / long SVXY (−0.5x era) for N days after VIX spikes,
      and when VIX/VIX3M is in contango; behavioral = VIX over-shoots then mean-reverts.
- [x] 083 Model-priced SPY options for the month-end signal (013): sell the put (when long signal) / call (when short)
      vs buying 1-day ATM options — premium (VIX9D-implied) vs realized edge; is options expression better than stock?
- [ ] 085 OPEX-Friday tilt (001) via options: sell a same-day ATM call on monthly OPEX Friday (calibrated model, as 083).
- [ ] 086 Crash-fear sentiment via CBOE SKEW: high SKEW (tail insurance demand) → subsequent PUT-index / SPY returns
      (contrarian: over-paid tail insurance), monthly.
- [x] 087 Model validation: monthly ATM put-write priced with VIX (BS) vs the real CBOE PUT index — mean/correlation of
      monthly returns, to bound how wrong the model-pricing in 083/085 could be.
- [ ] 084 VIX1D (2023+, short sample): 0DTE implied vs realized — descriptive only (no train/valid possible).

## BEHAVIORAL FOCUS (user request 2026-11-20) — do these first
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

- [ ] 075 Record-high attention: the session after SPY closes at an all-time high (headline attention) — retail buys at
      the open, price fades → short SPY open→close; also 'first record high in > 6 months' (bigger headline).
- [ ] 077 Under-reaction / conservatism (the other side of over-reaction): country ETF 12-1 month momentum
      (Asness-Moskowitz-Pedersen), monthly rebalance, survivorship-free; and sector-SPDR 12-1 momentum.
- [ ] 073 Round-number anchoring: S&P 500 (^GSPC) closing within 0.5% below a multiple of 100 / 500 → next-day
      open→close (resistance) vs just above (breakout).

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
- [ ] 028 Japan fiscal-year-end (Mar 31) and China/HK holiday closures: EWJ/FXI open→close around those dates (repatriation flows) — few events.
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
