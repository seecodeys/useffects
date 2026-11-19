# Backlog (top = next). Mark `[x]` when done, `[~]` in progress.

## Overseas / cross-market signals (core theme)
- [x] 001 Calendar & baseline anomalies for SPY/QQQ/IWM open→close (day-of-week, turn-of-month, pre-holiday, FOMC days, OPEX). Establishes baselines other signals must beat.
- [ ] 002 Country/sector ETF universe (EWJ, EWG, EWU, FXI, EWT, EWY, SMH, XLE, XLK, XLF, …): does each ETF's open→close depend on its *home* market's move since the last US close vs. its own gap (catch-up / over-reaction per ETF)? Cross-sectional long/short across ETFs.
- [ ] 003 Overnight FX & commodities as signals: USDJPY, AUDUSD, EURUSD, DXY, crude (CL=F), gold (GC=F), copper, US 10y (^TNX), BTC-USD moves since last US close → SPY/sector open→close.
- [ ] 004 ADRs: Asian ADRs (TSM, BABA, SONY, TM, HMC, MUFG, INFY, HDB) — open→close vs home close & ADR gap (ADR premium/discount at the open).
- [ ] 005 Europe at 9:30 ET, properly: HistData 1-min CFD bars (tools/histdata.py, DAX/CAC/ESTX50/FTSE, 2010→now, fixed-EST clock, rate-limited ~5 downloads then cool-down). Level at 09:29 NY vs prior US close → SPY/EWG/EZU open→close. Train window only 2010-14 here. Also FTSE daily open via ISF.L (Yahoo, 2009+, unadjusted → mask ex-div gaps).

## Cross-sectional intraday
- [ ] 006 Overnight-gap reversal in single stocks (large-cap universe, fresh Yahoo data): rank by gap, long losers/short winners open→close; costs ≥2bp/side. (Lou-Polk-Skouras tug-of-war.)
- [ ] 007 Overnight momentum vs intraday reversal at the ETF level (sector ETFs): past N-day overnight returns → today's intraday.
- [ ] 008 Lead-lag at the open: large-cap gap vs small-cap gap (SPY vs IWM), QQQ vs SPY — does the lagging one catch up intraday?

## Regime / volatility
- [ ] 009 VIX term structure (^VIX/^VIX3M, ^VIX9D) and VIX gap overnight → SPY intraday direction / magnitude.
- [ ] 010 Prior-day intraday vs overnight decomposition (yesterday's oc and gap) → today's oc, by regime.
- [ ] 011 Extreme overseas sell-off rebound (the one weak pattern from global_open_close) — event study with VIX filter.

## Ideas parking lot
- OPEX-week structure follow-up to 001 (Mon-Thu before OPEX, Monday after, quarterly-only, VIX-expiry Wednesdays). Lower priority: each new test raises N.
- Earnings-season effects on SPY intraday; month-end rebalancing flows; quad witching.
- Treasury ETF (TLT) / credit (HYG) gaps as signals for SPY intraday.
