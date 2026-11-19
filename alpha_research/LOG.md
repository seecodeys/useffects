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

## 004 — Asian ADRs vs home-listed shares  ·  2026-11-19
17 ADRs (TSM, UMC, CHT, SONY, TM, HMC, MUFG, SMFG, MFG, NMR, INFY, WIT, HDB, IBN, KB, PKX, SHG). Home share move since
last US close (known pre-open) → implied ADR gap (fitted on TRAIN); surprise = implied − actual gap. Costs 3bp/side.
- Per-ADR time-series catch-up and pooled: nothing in valid (mixed signs).
- **Cross-sectional L/S (long 3 most under-priced vs home, short 3 most over-priced):** valid t=+12, Sharpe 4.3 — too
  good. Edge correlates 0.8 with tick size in bp (UMC/MFG/WIT at $2-10 dominate) → opening-print bid/ask artifact.
- 004b: restricted to ADRs with prior close ≥ $20 (tick ≤ ~5bp), top2/bottom2: train t=+13.3, valid t=+4.9,
  Sharpe 1.73, +4.7bp/day net; positive every year 2004-22. Passed gate ($20 cut chosen from the tick diagnostic,
  not tuned on returns).
- **Holdout (2023→Nov 2026, one look):** 3bp/side: Sharpe 1.30, t=+2.56, +4.4bp/day net, hit 56%.
  5bp/side: Sharpe 0.11, t=+0.2. Gross ≈ 10bp/day, so it lives or dies on execution cost.
- 004c (bounce check, Yahoo 1h bars, Dec 2023→Nov 2026): gross open→10:30 = +8.5bp/day (t=5.3),
  10:30→close = +1.9bp/day (t=1.4). ~80% of the edge is in the first hour; delayed entry leaves less than costs.
Verdict: **promising but unverifiable with free data.** Real only if you can actually transact at the printed open
(NYSE opening auction via MOO orders) at ~≤3bp all-in. Could equally be bid/ask bounce in the open print.
Next step needs execution evidence: paper-trade MOO/MOC fills, or quote-level data at 9:30.
Total registry: 70 tests (luck benchmark t ≈ 2.9).

## 009/010 — VIX regime & prior-day decomposition → SPY/QQQ open→close  ·  2026-11-19
12 variants (6 pre-specified hypotheses × SPY, QQQ), 0.5bp/side. VIX/VIX3M from prior close only.
- H1 fade yesterday's open→close: **QQQ passed gate** (train t=+2.21, valid t=+2.44, +6bp/trade); SPY weak (t≈1).
  But QQQ P&L is crisis-concentrated (2000, 2001, 2008, 2020 ≈ 80% of total; ex-best-3-years +133% vs +345%).
  **Holdout (one look): t=+0.15, +0.5bp/trade @0.5bp; negative @1bp.** Dead.
- H2 (H1 only when VIX>VIX3M): QQQ valid t=+2.96, +45bp/trade on 8.5% of days — but train t only 1.58 (fails gate).
  Same crisis-reversal effect; few independent episodes.
- H6 (follow yesterday's intraday when its gap disagreed) is reliably *negative* (QQQ valid t=−2.7) — the same
  reversal seen from another angle, not independent evidence.
- H3 gap-fade|VIX>25, H4 long after VIX spike, H5 long in backwardation: nothing.
Verdict: **dead** as standalone. Index intraday reversal exists only in high-vol crisis regimes; too few episodes to trust.
Registry: 82 tests (luck benchmark t ≈ 3.0).

## 008 — Relative opening gaps (IWM/QQQ/DIA vs SPY; sector SPDR cross-section)  ·  2026-11-19
Fade the part of an ETF's gap not explained by SPY's gap (beta from TRAIN), hedged with SPY; 1bp/side per leg. 4 variants.
- IWM: train t=+2.9 → valid t=−1.1. QQQ/DIA: valid t≈−6, but that's ~4bp/day of two-leg costs on a ~1-2bp gross
  edge — momentum direction doesn't clear costs either.
- Sector XS fade (long 2 most-negative residual gap, short 2 most-positive): train t=+15, gross 20-30bp/day 2002-09,
  then ~2-3bp/day 2011-22 → valid t=−1.3 net.
Verdict: **dead.** Third instance of the same pattern (000, 002, 008): open-based relative-value signals among ETFs were
large before ~2010 and have been arbitraged to below costs. Deprioritise further ETF-open relative-value ideas.

## 003 (part 1) — Overnight USDJPY (HistData 1-min, real 15:59→09:29 NY levels)  ·  2026-11-19
6 variants (3 hypotheses × SPY, QQQ), 2001-2025, 0.5bp/side. USDJPY's overnight move correlates +0.30 with SPY's
gap but −0.015 with the subsequent open→close. Follow-overnight-FX, catch-up-to-FX-implied-gap and follow-last-hour
(08:29→09:29) all |valid t| < 0.7. **Dead** — overnight FX is priced into the open, same as overseas equities.
Remaining for 003: AUDUSD, XAUUSD (downloading; run `python experiments/003_overnight_fx.py AUDUSD XAUUSD`).

## 005 — Europe at 9:30 ET, measured properly (HistData DAX/CAC 1-min CFDs, Nov 2010-2025)  ·  2026-11-19
12 variants: DAX→SPY/EWG, CAC→SPY/EZU × {follow Europe's move since the US close (15:59→09:29 NY), catch-up to
Europe-implied gap, follow Europe's last hour 08:29→09:29}. Train is only 2010-14 here.
Europe's true 9:30 move correlates 0.79-0.88 with the US/ETF opening gap and 0.00-0.03 with the open→close.
No variant has train or valid t > 1.4 in the right direction. **Dead.**
This closes the original thesis: with correct timing (no look-ahead, real 9:30 levels), neither Asian closes (000),
overnight FX (003) nor European levels at the US open (005) predict US index open→close returns — it's all in the gap.

## 003 (part 2) — Overnight AUDUSD  ·  2026-11-19
6 variants, 2001-2025. corr(AUDUSD overnight, SPY gap) = +0.46, with open→close +0.01. All |valid t| < 1.5,
mostly wrong sign. **Dead.** Gold (XAUUSD) still downloading — low prior given USDJPY/AUDUSD; run it when cached,
then close 003. Registry: 110 tests (luck benchmark t ≈ 3.1).
