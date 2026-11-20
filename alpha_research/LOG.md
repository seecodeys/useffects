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

## 006/007 — Cross-sectional intraday signals, ~95 US large caps (price ≥ $20)  ·  2026-11-19
Hand-picked universe of names listed since ~2000 → survivorship-biased (flagged). Dollar-neutral decile L/S, open→close.
- 007a tug-of-war (long low past-20d overnight return): valid t=−2.5 @2bp. Dead.
- 007b intraday momentum (past-20d intraday): valid t=−1.6. Dead.
- **006 overnight-gap reversal** (long bottom-decile gap vs SPY, short top): gross 15-30bp/day 2003-10, ~5bp/day
  2015-22. @2bp/side valid t=+0.6 (fail); **@1bp/side valid t=+2.10, Sharpe 0.74 → passed gate** (cost level is the
  only difference; MOO/MOC on large caps can plausibly reach ~1bp all-in).
  **Holdout (one look):** @1bp: Sharpe 0.51, t=+1.0, +2.1bp/day; @2bp: t≈0. Gross ~5bp/day in 2023-25.
  Delayed-entry check (Yahoo 1h, Dec 2023→Nov 2026): open→10:30 +4.9bp/day (t=2.7), 10:30→close −0.9bp (t=−0.6).
Verdict: **weak / execution-dependent** — same signature as 004: a few bp of reversal that lives entirely in the first
hour after the opening print, so it requires real opening-auction fills at ≤1bp. Not standalone-tradable on this evidence.
Pattern across 004 and 006: the only surviving intraday "edges" are open-print reversals concentrated in the first hour.
Registry: 114 tests.

## 003 (part 3, closed) — Overnight gold (XAUUSD 1-min, 2009-2025)  ·  2026-11-19
6 variants. Gold's overnight move barely relates to SPY's gap (corr 0.07) or its open→close (+0.03). All |valid t| < 1.1.
**Dead.** 003 closed: USDJPY, AUDUSD, gold overnight moves have no open→close predictive power for SPY/QQQ.
(WTI crude not downloaded — low prior after three dead FX/commodity tests; parked.) Registry: 120 tests (luck t ≈ 3.1).

## 012 — Credit & rates ETFs (HYG, TLT) as risk signals → SPY/IWM open→close  ·  2026-11-19
8 variants. H1 "follow yesterday's credit-equity divergence" (HYG_cc − 0.4·SPY_cc) looked consistent:
SPY train/valid t = +1.4/+1.8, IWM +1.7/+2.8 (fails gate on train). Decomposition (HAC regression of open→close on
HYG prior-day and SPY prior-day returns): the SPY prior-day **reversal** term carries it (t ≈ −1.6…−2.7 in both periods);
the HYG term flips sign (train −0.9/−0.4 → valid +2.0/+1.6). So H1 is the index day-to-day reversal from 009 in disguise,
which already failed its holdout. H2 (fade TLT prior day) wrong-signed in valid; best-case simultaneous HYG/TLT gaps: nothing.
Verdict: **dead.** Registry: 128 tests.

## 013 — Month-end rebalancing: fade the stock-bond month-to-date gap on the last trading day  ·  2026-11-19
gap_mtd = SPY MTD − TLT MTD through the prior close (known pre-open). 4 variants (2002-08 →, TLT start).
- **H1 SPY, last trading day of month, pos = −sign(gap_mtd), open→close: train t=+3.34, valid t=+2.01, Sharpe 0.71,
  +17bp/trade → passed gate.** 16/21 years positive 2002-22 (ex-best-3-years total +23% vs +49%).
  Dose-response: mean edge by |gap_mtd| tercile = +5bp / +18bp / +39bp — the bigger the imbalance, the bigger the
  move, as a rebalancing-flow story predicts (consistent with published work on pension/balanced-fund rebalancing).
- H2 (last 2 days): valid t=+2.6 but train t=1.1. H3 quarter-ends only: weak (few obs). H1 mirror on TLT: right sign, t<2.
- **Holdout (2023→Nov 2026):** @0.5bp: **Sharpe 1.63, t=+3.20, +35.8bp/trade, hit 66%**; @2bp: t=+2.98.
  Bug found right after: the unfinished current month's latest day (2026-11-18) was treated as a month-end (one
  −43bp trade). Fixed (`complete` mask) and the holdout re-run, logged as such: @0.5bp Sharpe 1.68, t=+3.30,
  +37.5bp/trade, hit 67% (46 trades); @2bp t=+3.09. Train/valid rows unaffected.
Verdict: **SURVIVED HOLDOUT — first robust finding.** Small in time (12 trades/yr, ~4-5%/yr at 1× notional on the days
traded, ~1.5% max drawdown), but large per trade and cost-insensitive. Registry: 132 tests (luck t ≈ 3.1); holdout t=3.3
on a pre-specified rule is meaningful evidence.
Follow-ups: same rule on IWM/QQQ/DIA and on equity-vs-bond pairs (SPY long/short vs TLT) as confirmation; sizing by
|gap_mtd|; check whether the effect sits in the last hour (MOC flows) using 1h data.

## 013c — Month-end rebalancing: confirmation on IWM/QQQ/DIA + intraday timing  ·  2026-11-19
Same pre-specified rule (signal = −sign(SPY MTD − TLT MTD), last trading day, open→close, 1bp/side):
| ETF | train t | valid t | holdout Sharpe | holdout t | holdout bp/trade | hit |
|---|---|---|---|---|---|---|
| IWM | +3.61 | +2.38 | 1.41 | +2.77 | +37.7 | 74% |
| QQQ | +2.04 | +2.07 | 1.80 | +3.55 | +48.3 | 70% |
| DIA | +2.94 | +2.03 | 1.22 | +2.39 | +26.2 | 70% |
All gate-passed and all positive in holdout. (Highly correlated with SPY, so confirmation of consistency, not
independent evidence.) Timing on SPY (Yahoo 1h, 32 month-ends 2023-26): signed bp open→12:30 +20.7 (75% hit),
12:30→15:30 +10.3 (53%), 15:30→close +17.8 (81% hit) — the move is spread through the day with a strong last hour,
consistent with MOC rebalancing flows. Registry: 135 tests.
Verdict: **013 confirmed as the project's main finding.** Ideas: higher-beta/small-cap ETF for larger moves, gap-size
sizing, combining with the 001 OPEX tilt when month-end and OPEX don't coincide (they rarely do).

## 013d — Month-end extensions (SPY)  ·  2026-11-20
4 variants. E1 first-day-of-month unwind (follow prior month's gap): train t=+1.3, valid t=−0.8 — no reversal. E3
second-to-last day alone: train t=−1.1 — effect is a last-day phenomenon. E4 IEF instead of TLT: train t=+3.2 but valid
+1.0 — TLT (long duration, what balanced funds' bond sleeves move most) is the better proxy.
E2 sizing by |gap| (clip at 2× train median): passed gate (valid t=+2.21); **holdout t=+2.52, Sharpe 1.28 — lower than
the plain sign rule (t=3.30)**, so sizing adds nothing out-of-sample; keep the binary rule. Registry: 139.

## 015 — Execution reality check for 004/006: delayed entry with 5-min bars  ·  2026-11-20
Descriptive only (Yahoo 5m, 59 sessions Aug-Nov 2026; not registered). Gross L/S bp/day by entry time → close:
| Signal | open | 9:35 | 9:40 | 10:00 |
|---|---|---|---|---|
| 004 ADR XS catch-up (≥$20) | +21.7 (t=2.2) | +3.2 | +0.8 | +4.9 |
| 006 large-cap gap reversal | +13.2 (t=1.3) | +5.0 | −2.1 | −6.7 |
The edge is gone within 5-10 minutes of the open. Verdict: **004 and 006 downgraded to "opening-print artifact unless
proven otherwise".** They could only be real if MOO orders actually fill at the printed opening-auction price with
negligible impact; a 5-minute delay already erases them. Not worth more research without auction-fill evidence.

## 016/017 — Other rebalancing flows on the last trading day of the month  ·  2026-11-20
6 variants, 1bp/side per leg, MTD through prior close (helper verified identical to 013's SPY−TLT gap on all last days).
- 017a IWM−SPY size spread fade: train t=−0.8, valid +1.9. 017b EFA vs TLT: +1.3/+1.0. 017c EEM vs TLT: +1.6/+1.7.
  017d SPY−EFA spread fade: +1.9/+1.4. 016a sector XS (long 3 worst MTD, short 3 best): +0.6/+1.2. 016b quarter-ends: 0.
- None pass the gate. The international stock-bond versions (EFA, EEM) have the right sign in both periods but are
  roughly half as strong as the US stock-bond effect; sector/size rotation flows show nothing.
Verdict: **dead/weak.** The month-end effect is specific to the broad US equity-vs-Treasury rebalance (013).
Registry: 145 tests.

## 018 — Vol-target (de)leveraging; 019 — Russell reconstitution day  ·  2026-11-20
018 (SPY, 0.5bp): short when 5d/60d realized vol > 2: train t=+1.0, valid +0.2. Long when < 0.5: −0.5/+0.7.
Combined: +0.2/+0.6. **Dead** — no detectable open→close footprint of vol-target selling on daily data.
019 IWM−SPY spread on the last Friday of June (27 events; direction +1 chosen on train, mean +38bp train, +12bp valid):
t=+1.6/+0.6. **Dead/untestable** (too few events). Registry: 149 tests (luck t ≈ 3.2).

## 011 — Rebound after extreme overseas sell-offs  ·  2026-11-20
Long SPY open→close after the bottom 5% of overseas sessions (global_avg ≤ −1.13%, train-defined): train t=+1.5,
valid t=+1.2, +20bp/trade, 305 events. With prior VIX > 25: +1.9/+1.0, +25bp/trade. Right sign and sizeable per
trade in both periods, but never significant — volatile crisis days. **Weak, fails gate.** Same crisis-rebound family
as 009 H2/H4; parked. Registry: 151.

## 021 — OPEX-week structure (SPY/QQQ)  ·  2026-11-20
6 variants. Long Mon-Thu of OPEX week: t<1.2 both. Short day after OPEX: nothing (wrong sign in valid).
Short quarterly OPEX Friday: SPY train t=+2.48, valid +2.03, +45bp/trade → gate passed (a subset of 001's rule, so
not independent). **Holdout: Sharpe 0.17, t=+0.34, +6.7bp/trade (14 trades)** — weak, consistent with 001's
half-strength holdout. Verdict: **OPEX effects are a small, fading tilt; nothing new.** Registry: 157.

## 020 — signals.py (live helper + combined backtest)  ·  2026-11-20
`python signals.py` prints SPY/TLT month-to-date, the stock-bond gap, the next last-trading-day (NYSE month-end
holidays handled: Good Friday, Memorial Day, fixed dates) and the next monthly OPEX. `--backtest` writes
results/combined_yearly_pct.csv and combined_equity.png. Combined 2002-08→2026-11 at 1bp/side (in-sample years included):
013 SPY month-end: +2.6%/yr, Sharpe 0.93, maxDD −3.9%, t=4.6; 001 QQQ OPEX: +2.1%/yr, Sharpe 0.59; combined +4.7%/yr,
Sharpe 1.04, maxDD −5.3% (at 1× notional only on ~24 trade days/yr — capital is idle the rest of the time).
Fix: lib.get dropped a *completed* same-day bar when run after the close; now only drops it before 16:30 ET (earlier
experiments ran intraday and are unaffected).

## 022 — README.md write-up  ·  2026-11-20
Summary of method, all verdicts, the 013 finding and caveats.

## 025 — Jobs-report days (first Friday)  ·  2026-11-20
4 variants. Gaps are ~20% larger on jobs days (news lands pre-open), but neither fading the gap (SPY t=−0.7/−0.3;
QQQ −0.9/−1.6, i.e. slight continuation, not significant) nor a blanket long (−0.3/+1.3) works. **Dead.** Registry: 161.

## 027 — What drives the month-end effect? Equity leg vs bond leg (train+valid only)  ·  2026-11-20
2 registered variants + diagnostics (holdout untouched). L1 −sign(SPY MTD) alone: train t=+2.5, valid +0.6.
L2 +sign(TLT MTD) alone: train +1.5, valid +2.3. HAC regression on 245 last days 2002-22:
oc = −0.023·SPY_MTD (t=−1.3) + 0.057·TLT_MTD (t=+2.9). On the 56 days where SPY MTD and the stock-bond gap disagree
in sign, the gap rule still earns +18bp/trade (59% hit).
Interpretation: the **bond leg carries more of the signal** — when Treasuries rallied over the month, balanced funds
sell bonds / buy equities into month-end. The combined gap (013) remains the best single specification.
Registry: 163.

## 026 — Holiday-adjacent sessions (SPY long)  ·  2026-11-20
4 variants, 15-27 events each: day after Thanksgiving, Dec 24, last and first trading day of the year — all |t| < 1.
**Dead** (and underpowered). Registry: 167.

## 031 — Treasury month-end index extension; 032 — VIX-expiry Wednesday  ·  2026-11-20
031 long TLT / IEF open→close on the last trading day: TLT train t=+2.07, valid +1.62 (+9bp/trade); IEF +1.87/+1.81.
Consistent right sign in both periods (index-extension buying is a documented effect) but just short of the gate →
**weak-but-consistent**, not holdout-tested. Note it complements 013 (bonds bid at month-end while the bond-leg rebalance
also pushes equities).
032 short SPY on VIX-expiration Wednesday (251 events): +0.4 / −1.0. **Dead.** Registry: 170.

## 024 — FOMC announcement days (scheduled meetings, dates from federalreserve.gov)  ·  2026-11-20
Dates fetched by a web-fetch agent from the Fed's per-year historical pages + current calendar → tools/fomc_dates.csv
(scheduled + unscheduled flagged; spot-checked). 4 variants, 214 scheduled FOMC days 2000-2026:
long SPY on FOMC day train t=+1.7 → valid −0.4; QQQ +1.0/−0.2; day-before (pre-FOMC drift) +1.0/−0.3; long TLT on
FOMC day −0.7/+2.0. **Dead** — the documented pre-FOMC/FOMC-day equity premium has not survived in open→close terms
since 2015 (consistent with the literature's post-publication decay). Registry: 174 (luck t ≈ 3.2).

## 037 — Commodity index roll; 038 — Leveraged-ETF rebalancing proxy  ·  2026-11-20
037 short USO open→close on business days 5-9 (GSCI-style roll window), 2bp/side: train t=−0.1, valid +1.2. Dead.
038 follow the gap on |gap| > 1% days (LETF rebalancing in the move's direction): SPY +1.3/+0.0, QQQ −0.8/+0.3. Dead —
whatever LETF flow exists at the close doesn't show up as open→close continuation on daily data. Registry: 177.

## 040 — Month-end rule back-cast to 1993-2002 (fresh pre-sample, never examined)  ·  2026-11-20
Bond leg proxied by the 30y yield (bond MTD ≈ −17 × Δyield); proxy validated on 2002-26: 94% sign agreement with the
TLT-based gap, corr 1.00. Exact 013 rule on Feb 1993 → Jul 2002 (114 month-ends), SPY open→close @0.5bp:
**Sharpe 0.20, t=+0.60, +6.6bp/trade, hit 55%** — right sign, but weak; volatile years (1994 −381bp, 1998 −305bp).
Logged in holdout_log as a one-look out-of-sample test.
Interpretation: the effect is **era-dependent** — much weaker before 2002 than in 2002-2026 (train t=3.3, valid 2.0,
holdout 3.3). A plausible (post-hoc) reason is the growth of rebalancing-driven assets (balanced/target-date funds,
especially after the 2006 Pension Protection Act); but it also means the edge depends on a flow regime that can change.
This lowers confidence somewhat: still the best finding, but not a timeless law.

## 039 — Corporate-bond month-end; 046 — Tax-loss selling / January effect  ·  2026-11-20
039 long HYG / LQD on the last trading day: HYG −0.2/+0.4, LQD +1.9/+1.3 (same weak-consistent pattern as Treasuries in
031, smaller). Dead/weak.
046 IWM−SPY spread: short last 5 days of December −0.3/+0.6 (dead); long first 5 days of January **−2.6/−1.4** — the
classic January small-cap effect is *reversed* intraday (small caps lag SPY open→close in early January). Not
pre-specified in that direction, so only noted. Registry: 181.

## 013e — Month-end signal stability through time (descriptive)  ·  2026-11-20
Per-trade P&L by 5-year block (TYX proxy pre-2002-08, TLT after): 1993-94 −16bp (t=−1.7, 23 trades), 1995-99 +14 (0.8),
2000-04 +22 (1.9), 2005-09 +37 (3.3), 2010-14 −1 (−0.1), 2015-19 +13 (1.8), 2020-24 +26 (2.0), 2025-26 +47 (3.2).
Positive in 6 of 8 blocks, strongest post-2005, with a dead 2010-14 stretch. results/013_stability.png.

## 050/051 — Over-reaction vs news (volume split) and attention buying, ~95 large caps  ·  2026-11-20
Behavioral focus begins (user request). Signals from the prior close only (yesterday's return vs SPY, volume vs its
20-day average); trade next open→close; 2bp/side/leg. 6 registry rows (050b first run barely traded because of a
≥20-names filter — redone as 050b' with thresholds).
- 050a reversal of LOW-volume extreme moves: −0.2 / −1.3. 050c plain reversal: −2.3 / −1.4 (losing after costs).
- 050b' continuation of HIGH-volume moves (>2× volume, |move|>2%): +1.3 / −1.9 — flips sign after 2015.
- 051 short attention winners (high volume + top-decile move): **−3.1 / −1.8** — wrong way: big-volume winners keep
  rising a little (high-volume return premium), but 051r (long them) is only +0.6 / −1.1 after costs.
Verdict: **dead in large caps.** Next-day over-reaction/attention effects among liquid large caps are a few bp at most —
below 2bp/side costs. These effects are documented mainly in small, retail-heavy stocks, which need a survivorship-free
universe we don't have for free. Moving to ETF-based behavioral tests (no survivorship issue). Registry: 187.

## 052/053 — Streaks (26 ETFs) and fear over-reaction (SPY)  ·  2026-11-20
052 long after ≥4 down closes: +1.8 / +0.1; short after ≥4 up closes: +1.2 / −1.5. 053 long SPY next open→close after
a < −2σ day: +1.5 / +0.1 (219 events); after a VIX +25% day: +1.7 / −0.1 (65); both: +1.6 / −0.1 (61).
By decade the fear rebound was large in the 2000s (+74…+83bp/trade after VIX spikes), ~0 in the 2010s, +26bp in the
2020s — positive overall but erratic and never significant. **Dead** as tradable rules; same "worked before ~2010" shape.
Registry: 192 (luck t ≈ 3.2).

## 054-057 — Anchoring (52w highs/lows), lottery (MAX), sector herding, Monday mood  ·  2026-11-20
054 long new-52w-high / short new-52w-low large caps (SPY-hedged): −2.2 / −3.0 net — gross ≈ 0, costs (~4-5bp/day of
daily round trips) produce the negative t. 055 MAX/lottery L/S: −2.0 / −4.1 net, same story. 056 sector herding
reversal: +1.3 / −0.3. 057 Monday after a Friday drop: +0.1 / +0.4. **All dead.** Registry: 196.

### Behavioral round summary (050-057)
Thirteen behavioral variants (over-reaction vs news, attention, streaks, fear, anchoring, lottery, herding, mood): none
pass. The recurring shape: effects that are visible in the 2000s (fear rebounds of +70-80bp/trade, streak reversals)
are ~0 after 2010 in liquid US stocks/ETFs held open→close. The open→close constraint matters: much of documented
over-reaction correction happens **overnight** or over **several days**, and in **small, retail-heavy stocks** —
neither of which this setup captures. Logical next steps: (a) allow overnight/multi-day holds for behavioral signals;
(b) a survivorship-free small-cap universe (not freely available).

## 059/060 — Crowd-sentiment gauges, contrarian  ·  2026-11-20
060a long SPY when the fear premium (VIX − 20d realized vol) is in its top 20%: +1.4 / +0.8. 060b long SPY when
VIX9D > VIX (short-term panic): +0.8 / +0.2. 059 long QQQ when the bearish 3× ETF (SQQQ) takes an unusually large
share of TQQQ+SQQQ dollar volume (z>2): +0.3 / +0.8, +13bp/trade. All right-signed (contrarian works a little), none
significant. **Weak/dead.** Registry: 199.

## 058 — Behavioral signals with overnight / multi-day holds (close→close)  ·  2026-11-20
First tests under the relaxed holding rule (user OK). 9 variants; lag-1 = realistic (avoids trading on the signal's
own closing print).
- A 1-day residual reversal: ETFs train t=+5.2 (best-case lag0) / +2.2 (lag1) → valid +0.3 / −0.7. Large caps: −0.9 /
  +1.5 → −1.5 / −2.4.
- B 5-day residual reversal, overlapping 5-day holds (lag1): ETFs +3.1 → +0.4; large caps +3.3 → +0.4. Gross edge by
  5-year block: 6-7bp/day (2000-04) → 3-5 (2005-09) → ~1-2bp/day since 2010, vs ~0.35-0.7bp/day turnover costs.
- C SPY fear rebound (after a −2σ day) held 1 day: +1.7 / +0.7; held 5 days: 0 / −0.3.
- D ETF streak reversal held 1 day: +1.5 / −0.7.
Verdict: **dead/weak.** Allowing overnight/multi-day holds doesn't rescue the classic short-term over-reaction
(reversal) effects in liquid US assets: they were large in 2000-09 and have decayed to ~1-2bp/day gross. Registry: 208.

## 062 — Country-level over-reaction (20 single-country iShares ETFs, survivorship-free)  ·  2026-11-20
Long 3 worst / short 3 best countries by past-N-day return vs the cross-section, overlapping H-day holds, lag-1, 2bp/side.
- a) weekly (N=5,H=5): train t=+4.0 → valid −0.9; gross 6bp/day (2000-04) decaying to ≤0 after 2015. Dead (same decay).
- **b) monthly (N=21,H=21): train +1.1, valid +1.5; gross bp/day by 5-yr block 1.0 / 0.1 / 1.7 / 1.5 / 1.1 / 1.1 —
  small but unusually stable across eras (≈2.5-4%/yr gross on a half-long/half-short book, low turnover).** Fails gate.
- c) quarterly losers vs winners (N=63,H=21): +0.2 / +1.6; erratic.
Verdict: weekly over-reaction dead; **monthly country reversal = weak-but-stable** (doesn't decay like the short-horizon
effects; plausible story: country-level herding/over-reaction to macro news over weeks). Candidate for refinement with
pre-specified variants (e.g. skip-week, vol-scaling) — but each adds to N. Registry: 211.

## 062b — Country reversal refinements (pre-specified)  ·  2026-11-20
d) skip-week (t−21..t−5): +0.5 / +1.7. e) vol-scaled: +0.6 / +1.8. f) 30-country universe: +1.1 / +1.2.
All share the same profile: ~0 gross in 2000-09, ~1-1.5bp/day gross since 2010, never significant. **Weak-but-stable;
no variant passes the gate.** Not pursued further for now (registry 214, luck t ≈ 3.3).

## 061 — Market capitulation / euphoria timing (SPY, excess over exposure-matched buy-and-hold)  ·  2026-11-20
a) long only when SPY is >15% below its 252-day high (17% of days): timing excess −0.2 / +1.3. b) step aside at euphoric
highs (new high + 21d return > 8%; rare): −0.6 / −1.2. **Dead** — no timing value beyond the equity premium.
Registry: 216.

## 063/064/065 — Disposition effect, panic-driven country reversal, extrapolation reversal  ·  2026-11-20
063 long low / short high capital-gains-overhang ETFs (price vs 1-year VWAP), monthly: −0.1 / +0.05. Dead.
064 country 1-week reversal only when VIX > 30 (12% of days): train +3.0 (2008-09 crisis) → valid +0.4. Dead.
065 pooled reversal after 63-day extremes (top/bottom 5% of train history) in GLD/TLT/USO/EEM/QQQ, 21-day holds: -0.4 / -0.1. Dead. Registry: 219.

## 070/072 — Mood anomalies: DST "lost sleep" Mondays, lunar phases  ·  2026-11-20
070 short SPY open→close on the first session after each DST change (54 events): +0.4 / −1.6. 072 long the new-moon
week / short the full-moon week: −0.5 / −0.1. **Both dead** — classic mood anomalies don't show up in SPY open→close.
Registry: 221.

## 069 — Weather mood: sunny vs cloudy New York mornings (Hirshleifer-Shumway)  ·  2026-11-20
NYC morning (06-09h) cloud cover from the free Open-Meteo ERA5 archive
(archive-api.open-meteo.com/v1/archive?latitude=40.71&longitude=-74.01&hourly=cloud_cover&timezone=America/New_York),
deseasonalised by calendar month (train). H1 long sunniest 20% / short cloudiest 20%: −0.4 / −1.1. H2 −sign(cloud):
−0.2 / −0.5. **Dead** (if anything wrong-signed). Registry: 223.

## 067/071 — Crypto as a retail-mood gauge  ·  2026-11-20
BTC-USD starts 2014-09, so TRAIN is essentially empty; judged on VALID (2015-22).
067 follow BTC's weekend move (|move|>3%) on Monday open→close: QQQ valid t=+0.3, ARKK +0.1. Dead.
071 short the next US session after a BTC day < −10% (44 events): QQQ −1.5, ARKK −0.8 — i.e. tech/ARKK tend to *bounce*
(~+28bp) after crypto crashes, the opposite of the pre-specified shock direction; not significant, noted only.
Registry: 227.

## 080/081 — CBOE option-strategy data; fear-premium timing of put-writing  ·  2026-11-20
080: CBOE strategy-index histories downloaded to data/cboe/ (PUT 1991+, BXM, CNDR, BFLY, WPUT 2006+, PPUT, BXMD, CLL)
from cdn.cboe.com/api/global/us_indices/daily_prices/<NAME>_History.csv (follow redirects). Real option prices.
081: write SPX puts only when the fear premium (VIX − 20d realized vol) is above its train median; measured as timing
value over exposure-matched always-on writing, net of switch costs.
- Monthly (decide at each roll): PUT −1.0 / +1.6; weekly WPUT +0.3 / +0.0.
- Descriptive: always-on PUT excess return by prior-day fear-premium tercile 2000-22 = +10% / −3% / +30% annualised —
  high-fear days do pay most, but they're also the most volatile, so timing doesn't produce a reliable edge.
- 081b daily exposure switch (hypothesis formed from that table): 10bp/switch −0.4 / +1.3; 25bp/switch −1.2 / −0.4.
Verdict: **dead/weak** — the insurance premium is real (put-writing earns ~equity-like returns) but timing it by the
fear premium adds no robust value. Registry: 231.

## 082 — VIX futures ETP (VIXY) and fear over-shoot  ·  2026-11-20
Timing value over exposure-matched always-short VIXY (close→close, lag-1, 5bp/side; train only 2011-14):
a) short VIXY for 5 days after VIX > 1.25× its 20-day mean (fear over-shoot): +0.8 / −0.5. Dead.
b) short VIXY only in contango (VIX < VIX3M): timing value ≈ 0 (−0.0 / +0.2), but raw Sharpe 2011-22 0.66 vs 0.25 for
always-short — the rule's benefit is **risk reduction** (it sits out backwardation crashes like Feb-2018/Mar-2020), not
extra return. Known, not behavioral alpha; noted as a risk overlay. Registry: 233.

## 083 — Month-end signal expressed with same-day SPY options (MODEL-PRICED)  ·  2026-11-20
Black-Scholes, IV = VIX9D (else VIX) at prior close, ATM struck at the open, expiring at the close, 5% premium haircut.
- 083a/b raw model: buying looked great (valid t=+2.9) — **a pricing artifact**: calendar-time VIX underprices intraday
  options (modelled ATM premium 20bp vs 58bp average |open→close| move on month-ends). Rows kept, flagged.
- Calibration: premiums scaled ×1.69 so a same-day straddle costs 110% of the average realized |open→close| in TRAIN
  (sanity: a straddle bought every non-month-end day loses −10.8bp/day, i.e. a ~10% vol premium, as intended).
- 083c BUY the ATM option in the signal's direction (calibrated): +0.2 / +0.7, ~+1-4bp/trade. Not worth it.
- **083d SELL the ATM option against the signal** (sell the put when the signal says long, the call when short):
  train t=+5.57, valid t=+2.71, +18 / +11bp per trade → passes the gate on per-trade bp (registry's synthetic-series
  bp field is per day). Directional edge dominates: the built-in vol premium contributes only ~1.4bp/trade.
  **Holdout (2023→Nov 2026, one look): Sharpe 1.80, t=+3.53, +15.7bp/trade, 80% hit (46 trades), worst −78bp**
  vs the stock version on the same days: +37.5bp/trade, 67% hit, worst −109bp.
Verdict: **survived holdout (model-priced).** Selling a same-day option against the month-end signal is a higher-hit-
rate, lower-drawdown way to harvest the same flow, at the cost of capped upside. Caveats: model prices (no skew, real
0DTE IV/spreads differ; VIX1D/VIX9D median 0.86 in 2023+), same-day SPX options broadly listed only since ~2016-2022,
and assignment/settlement mechanics (use cash-settled SPX/XSP, PM-settled). Registry: 237.

## 087 — Model validation: BS-with-VIX monthly ATM put-write vs the real CBOE PUT index  ·  2026-11-20
Monthly excess returns over T-bills, roll-to-roll (months available in CBOE's file; it has gaps pre-2015):
1997-2014 model +0.72%/mo vs real +0.50% (corr 0.97); 2015-22 +0.71 vs +0.47 (0.98); 2023+ +1.16 vs +0.75 (0.90).
The model tracks reality closely but **overstates the seller's edge by ~0.26%/month (~35%)** — VIX embeds OTM skew, so
it sits above ATM implied vol. Implication for 083 (model-priced short same-day options): real-world results are
likely lower than modelled — e.g. a 15% lower premium would cut the +15.7bp/trade holdout figure to roughly +10bp.
Still positive, but treat 083's numbers as an upper bound.

## 085/086 — OPEX tilt via same-day options; SKEW (tail-insurance demand) as sentiment  ·  2026-11-20
085 sell a same-day ATM SPY call on monthly OPEX Friday (083's calibrated model): train t=+5.15, valid +2.82,
+17 / +12bp per trade → gate passed (per-trade). **Holdout: +3.0bp/trade, t=+0.52, 73% hit (44 trades), worst −118bp.**
Weak — mirrors the stock version's fade (001 holdout +12.6bp). Model-priced and likely optimistic (087).
086 SKEW top-20% (tail insurance over-bought) → contrarian put-writing: PUT timing value **−2.95 / −1.15**; SPY −1.6 /
−0.7. Wrong-signed: high SKEW precedes *worse* put-writing returns (tail demand is informed, not over-reaction). Dead.
Registry: 240.

## 073/075/077 — Round numbers, record-high attention, under-reaction momentum  ·  2026-11-20
075 short SPY the session after an all-time-high close (621 events): −0.2 / −1.4 (records are followed by slight
*gains*, not attention fades); first ATH in >126 sessions (6 events): untestable. 077 12-1 momentum: countries
top4/bottom4 −0.2 / −0.9, sectors +0.1 / +0.3, gross ≈ 0 in every 5-year block. 073 S&P near round 100s (short just
below / long just above): −0.2 / −0.4. **All dead.** Registry: 245 (luck t ≈ 3.3).

## 090/091/095 — Seasonal mood, tug-of-war, retail-heavy over-reaction  ·  2026-11-20
090 Sell-in-May (long Nov-Apr only, timing value): +0.9 / −0.0. 091 short SPY open→close after ≥4 positive overnight
returns: −2.0 / +0.5 (sign flips). 095 reversal of |z|>2 days in retail-heavy ETFs (ARKK/TQQQ/SMH, pooled, lag-1):
+0.1 / −1.2; SPY control +0.5 / −0.3 — no extra over-reaction where retail concentrates. **All dead.** Registry: 249.

## 110-113 — Participant-behavior / flow tests (SPY)  ·  2026-11-20
110 turn-of-month inflow window (last day + first 3), close→close timing value: +1.0 / +0.1. Dead.
111 overnight vs intraday: mean bp/day overnight vs intraday by 5-yr block 2000: 0.6/−1.2, 2005: 2.0/−1.6,
2010: 2.7/2.6, 2015: 2.6/1.3, 2020: 3.2/2.4, 2025: 3.8/1.1 — overnight consistently ≥ intraday, but long-overnight-only
(t=+0.7/+0.8) and long-overnight/short-intraday (−0.1/−0.4) aren't significant after costs. Dead as a strategy.
**112 leveraged-ETF rebalancing reversal** (fade |day|>2% from the close to the next open): train −0.1, valid **+2.02**
(+2.2bp/day, 523 events). Mechanism predicts it should only exist once LETFs were large (2009+): 112b (post-hoc sample
start 2009, logged as such): 2009-14 t=+0.8 (+7bp/trade), valid +2.0 (+32bp/trade) → fails gate. **Weak lead**, growing
over time; revisit with intraday data or a cleaner LETF-AUM-weighted signal.
113 buyback-blackout proxy (flat days 14-45 after quarter-end): −0.3 / −0.9. Dead. Registry: 255.

## 114/115/116 — Dividend flows (parked), futures roll week, month-end size interaction  ·  2026-11-20
114 parked: aggregate dividend *payment* dates aren't available from free data (Yahoo gives ex-dates only).
115 long SPY / IWM in the index-futures roll week (timing value): +0.1 / −1.1 and 0.0 / −1.1. Dead.
116 (descriptive, 2002-22 only): 013's per-trade edge by month type — ordinary month-ends +27bp (t=4.1, 163),
quarter-ends +5.6bp (t=0.7, 61), year-ends +18.6bp (t=1.2, 21). The effect is concentrated in ORDINARY month-ends,
not the biggest rebalancing dates — consistent with large quarter-end flows being anticipated / spread over prior days
(or diluted by quad-witching on 3rd Fridays of quarter months). Not a new rule (would be a post-hoc filter). Registry: 257.

## 118 — Quarter-end rebalancing anticipation  ·  2026-11-20
013 rule applied on days −3 and −2 of quarter-end months: −0.9 / +0.7. No front-running signature. Dead. Registry: 258.

## Paper sourcing (arXiv, via web-fetch agent)  ·  2026-11-20
Six arXiv API queries (price pressure, rebalancing, leveraged ETFs, overnight/intraday, gamma hedging, fund flows);
~11 relevant papers screened from abstracts. Testable-with-free-data ideas added to BACKLOG as 120-123 (Ponzi-fund
flow chasing; LETF pre-positioning/reversal; vol-controlled stock-bond-gold; LETF trend filters). Context notes: an
end-of-day reversal paper (2611.09286) reports ~3bp/day in 2023-26 for large stocks, below costs; a futures intraday
momentum study (2605.04004) found nothing beats friction — consistent with our open-print findings.

## 120/121 — Paper-sourced flow tests  ·  2026-11-20
120 [2405.12768 'Ponzi Funds'] short sector/country ETFs with high 3-month return + volume surge (flow proxy), long the
rest, monthly: −0.1 / +0.8; gross ≈ 0 in every 5-year block. Dead.
121 [2608.03703 'Preying on LETFs'] US analog, Yahoo 1h, Dec 2023-Nov 2026 (descriptive): on |open→15:30| > 1% days, the
last half-hour does NOT continue the move (SPY −0.6bp, QQQ −5.1bp); next-open reversal SPY +11.4bp (64% hit, t=1.0),
QQQ +6.8bp (t=0.9) — same direction as our weak 112 lead (overnight reversal after big days), not significant.
No pre-positioning signature in US ETFs. Registry: 259.

## 100/103/096 — Month-end option pricing, micro-cap reversal, portfolio of survivors  ·  2026-11-20
100 realized/implied |open→close| ratio (VIX9D|VIX-implied 6.5h move): month-ends 1.35 (median 1.03) vs other days 1.46
(1.15) — month-end volatility is NOT under-priced; no case for buying options on month-ends (agrees with 083c).
103 reverse |z|>2 days in IWC/IJR (micro/small-cap ETFs), lag-1: +0.2 / **−2.3** — small caps *continue* after big days
if anything. Dead.
096 (construction, not registered): 013 SPY month-end, 031 TLT month-end long, 062b monthly country reversal are
uncorrelated (|ρ| ≤ 0.11). Sharpe train/valid/holdout: 013 0.91/0.71/1.68; 031 0.57/0.57/**−0.72**; 062b 0.30/0.52/0.30;
combined 0.81/0.89/0.64. The TLT month-end leg failed in 2023+ (rate regime), so adding it hurt the holdout; 013 alone
remains best. Registry: 260.

## 117/122/123 — Sharper LETF signal; paper-sourced risk overlays  ·  2026-11-20
117 fade |SPY day|>2% close→next open only when LETF dollar-volume share (SSO/SDS/UPRO/SPXU/QLD/QID/TQQQ/SQQQ vs
SPY+QQQ) is above its 1-year median, 2009+: train +0.1, valid +1.8 (175 events). Conditioning on LETF activity doesn't
sharpen it; the 112 family stays a weak, valid-period-only lead.
122 [2609.07946] vol-controlled 60/40 (10% target, monthly): Sharpe 1.15 vs 0.91 static (2003-14), 0.64 vs 0.62
(2015-22); max drawdown −20%/−22% vs −30%/−27%. Excess return t +1.7 / +0.4. Risk-reduction overlay, not alpha (as the
paper frames it).
123 [2504.20116/2506.19200] SSO when SPY's 63d trend > 0, else SPY: Sharpe 0.48 vs 0.36 always-SSO (2007-14) but 0.45 vs
0.54 (2015-22); excess t −0.2 / −1.5; cuts drawdowns (−65% vs −85%). Not alpha. Registry: 263.
