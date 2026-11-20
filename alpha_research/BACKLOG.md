# Backlog (top = next). Mark `[x]` when done, `[~]` in progress.

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
- [ ] 011 Extreme overseas sell-off rebound (the one weak pattern from global_open_close) — event study with VIX filter.

## New ideas (added after 001-010)
- [x] 012 Credit/rates gaps as risk signals: HYG, LQD, TLT, IEF opening gaps & prior-day moves → SPY open→close (all US-listed, liquid).
- [x] 013 Month-end / quarter-end rebalancing: last 1-2 trading days of month, SPY vs TLT intraday direction conditioned on month-to-date stock-bond performance gap (pension rebalancing story).
- [x] 013c Month-end follow-ups: same pre-specified rule on IWM/QQQ/DIA, SPY-vs-TLT pair, |gap| sizing, intraday timing via 1h bars (last-hour MOC flows?).
- [ ] 014 Earnings-season intensity: SPY/QQQ intraday during heavy mega-cap earnings weeks vs not (needs earnings dates — Yahoo events or skip).
- [x] 015 Execution reality check for 004/006 (first-hour open-print reversals): document what evidence would confirm them (MOO paper fills, TAQ-like data); test robustness to entering at 9:35 using Yahoo 5m bars (60 days) — descriptive only.

## Flow-driven ideas (the 013 theme is the only robust winner so far)
- [x] 016 Quarter-end window-dressing vs rebalancing: last day of quarter, sector/style winners vs losers MTD (XLK vs XLU etc.) — does the losing sleeve get bought? (cross-sectional sector version of 013).
- [x] 017 Month-end rebalancing in other markets with US-listed vehicles: EFA/EEM vs TLT; small-cap vs large-cap (IWM vs SPY) month-to-date gap on last day.
- [x] 018 Vol-target deleveraging: after large realized-vol jumps (e.g. 5d realized vol doubles vs 60d), risk-parity/vol-target funds sell over following days → SPY open→close drift on days 1-3 after.
- [x] 019 Russell reconstitution (last Friday of June) and S&P quarterly rebalance days: IWM vs SPY open→close.

## Ideas parking lot
- 004 follow-up: verify ADR opening-auction fills (paper MOO/MOC orders, or tick/quote data); same idea for European ADRs once 9:30 Europe levels exist (005).
- OPEX-week structure follow-up to 001 (Mon-Thu before OPEX, Monday after, quarterly-only, VIX-expiry Wednesdays). Lower priority: each new test raises N.
- Earnings-season effects on SPY intraday; month-end rebalancing flows; quad witching.
- Treasury ETF (TLT) / credit (HYG) gaps as signals for SPY intraday.
