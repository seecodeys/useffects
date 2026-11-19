# Research log

## 000 — Global open→close for SPY (pre-protocol)  ·  2026-11-19
See `global_open_close/README.md`. Asia/Europe moves since the US close are priced into
SPY's open; no strategy beat t≈1 out-of-sample (2015+). Best-case country-ETF-open signal
was strong 2005-14 (t=3.5) and vanished after (t=0.3). Weak rebound after the worst 20% of
overseas sessions (+7.5bp/day, t≈1.5, crisis-driven). ~20 variants tested, not in the registry.
Verdict: **dead for SPY on daily data.** Spawned backlog items 002, 004, 011.

## 001 — Calendar baselines (SPY/QQQ/IWM open→close)  ·  2026-11-19
27 variants: weekday longs, turn-of-month, pre-holiday, short OPEX Friday, always-long.
- Weekday / turn-of-month / pre-holiday: nothing (all |t| < 2 in valid; signs flip between periods).
- **Short monthly OPEX Friday (3rd Fri) intraday**: train t = +2.8 / +2.6 / +2.3 (SPY/QQQ/IWM).
  Valid: QQQ t = +2.22, Sharpe 0.78, +26bp/trade → **passed gate**; SPY t = +1.45 (fails); IWM t = −0.2 (fails).
  QQQ & SPY profitable in 19/23 years 2000-22; quarterly expiries stronger (QQQ 34bp vs 21bp). Story: AM-settled index
  options settle at Friday's open, dealer hedge unwind / pin effects through the day.
  Luck benchmark: with N=27 tests, best-of-noise t ≈ 2.6, so the valid t of 2.2 alone isn't conclusive.
- **Holdout (2023→Nov 2026, one look):** QQQ short OPEX Friday: +12.6bp/trade, hit 55%, Sharpe 0.42, t = +0.83
  (44 trades). At 2bp/side: +9.6bp/trade. Same sign, about half the size, not significant.
Verdict: **weak-but-alive.** A real-looking, persistent tilt but tiny: ~12 trades/yr, ~1-1.5%/yr at 1× notional.
Usable as a filter/overlay rather than a standalone strategy. Follow-ups: OPEX-week structure (Mon-Thu before,
Monday after), quarterly-only, VIX-expiry Wednesdays — but these are new tests and count against N.

## 002 — Asian country ETFs vs home-market move (catch-up / over-reaction)  ·  2026-11-19
8 ETFs (EWJ, EWH, FXI, EWY, EWT, EWA, EWS, INDA) vs home index since last US close; implied gap fitted on TRAIN;
surprise = implied − actual ETF gap. Costs 2bp/side. 20 registry rows (10 from a first run whose cross-sectional
row had its sign flipped vs its label — kept for an honest count, annotated).
- Per-ETF catch-up: strong in train for EWT (t=+7.5), EWS, INDA, but **every ETF negative in valid** (t −2.2…−3.7).
  Those valid losses are mostly costs: gross edge ≈ −0.6bp/trade, so fading doesn't work either.
- Cross-sectional catch-up L/S (long 2 cheapest-vs-home, short 2 richest): train t=+25 (!), gross 22bp/day;
  valid gross 2.7bp/day < 4bp/day costs → t=−2.8 net. Gross edge by year decays monotonically:
  45bp (2001-03) → 10bp (2009-13) → 3bp (2017-19) → ~0 (2020-22).
Verdict: **dead.** Classic arbitraged-away (and/or stale-opening-print) effect; mirrors the 000 finding that
country-ETF opens stopped predicting SPY after ~2014. Lesson for later experiments: in pre-2010 data, opening prints
of thinner ETFs are unreliable — be suspicious of huge train-period t-stats from open-based signals.
