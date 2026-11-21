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

## 127/129/133 — Month-end close→close, mid-month rebalancing, Treasury auction supply cycle  ·  2026-11-20
127 013 held close→close (adds the overnight before the last day): +2.7 / +1.9 — no better than open→close (3.3/2.0).
129 013 rule on the first trading day ≥ the 15th: +0.5 / −1.1. Dead — the rebalancing effect is a month-END effect.
133 Treasury auctions (Lou-Yan-Zhang; 750 long-duration auction dates 2002-26 from api.fiscaldata.treasury.gov saved in
data/auctions.json): (a) short TLT d−3..d−1: +0.8 / +2.3; (b) long TLT d+1..d+2: +2.7 / +0.8;
**(c) full cycle: train t=+2.46, valid +2.51, Sharpe 0.89 → gate passed** — but highly concentrated: valid ex-2020 t=+1.0,
train ex-2009/10 t=+0.8 (13/21 positive years). Fails the "not driven by one year" rule → **no holdout look**; the
effect appears only in high-volatility bond regimes (dealer balance-sheet stress). Candidate refinement: scale by rate
volatility (MOVE-like proxy from TLT realized vol) — pre-specify before testing. Registry: 268.

## 134 — Auction cycle refinements  ·  2026-11-20
a) TLT cycle only in high rate-vol regimes: +0.7 / +1.6 (ex-2020 +0.8). b) IEF with 10y auctions: +1.0 / +1.6 (ex-2020
+0.9). Right sign everywhere but weaker than 133c — the auction cycle is a real but small, episodic effect.
**Weak-but-consistent; parked.** Registry: 270.

## 139 — Quarter-end portfolio pumping (IWM−SPY)  ·  2026-11-20
a) long IWM−SPY open→close on quarter-end day: −1.9 / +1.1. b) short IWM−SPY quarter-end close→next open (pumping
reversal): **−3.2 / −1.5** — small caps keep outperforming overnight into the new quarter (gross +16bp/trade 2000s,
+7 2010s, +3 2020s, decaying). 139d (direction from train, long overnight): +1.9 / +0.05 — gone in valid. c) control at
ordinary month-ends: −0.7 / −2.2. Dead. Registry: 274.

## Paper sourcing round 2 (arXiv)  ·  2026-11-20
Seven queries (Treasury auctions, closing auction, index reconstitution, month-end, dealer inventory, ETF creation/
redemption, calendar anomalies): no new daily-testable ETF ideas — arXiv is thin on empirical asset pricing (mostly
theory/microstructure). Partial fits: index-addition liquidity provision (2506.21775, single stocks) → spawned 140 below.
Next sourcing rounds should target NBER/SSRN-style abstracts if fetchable.

## 142 — Fed/Treasury 'net liquidity' (WALCL − TGA − RRP, FRED)  ·  2026-11-20
Long SPY the week after a weekly net-liquidity increase (position from the Friday close after Thursday's release),
flat after decreases; timing value: −1.1 / +1.1. **Dead** — the popular liquidity narrative has no weekly timing value.
FRED CSVs (no key) at fred.stlouisfed.org/graph/fredgraph.csv?id=<SERIES> saved to data/. Registry: 275.

## 140 — S&P 500 additions (index-fund buying at the effective-date close)  ·  2026-11-20
300 additions of current constituents since 2001 (Wikipedia 'Date added'; removed stocks missing → survivorship biases
against the short). Abnormal return vs SPY:
| period | n | 10d run-up | effective day | next 5 sessions |
|---|---|---|---|---|
| 2001-14 | 130 | +330bp | −13bp | **−87bp (t=−2.10)** |
| 2015-22 | 113 | +103bp | +25bp | **−79bp (t=−2.18)** |
| 2023+ | 57 | +704bp | +11bp | −4bp (t=−0.06) |
Portfolio version (short added vs SPY for 5 sessions, 3bp/side/leg, ~25% capital per basket): train t=+1.57, valid
+2.05 → fails gate. **Protocol slip:** the descriptive table printed 2023+ stats, so this idea's holdout is spent
(logged here; no holdout_eval call). In 2023+ the post-inclusion reversal is gone while run-ups got much larger
(momentum names). Verdict: **real through 2022, absent since** — consistent with heavy arbitrage of index events.
Registry: 276.

## 151/154 — CTA moving-average flows; VIX-ETP rebalancing  ·  2026-11-20
151 follow SPY 50d / 200d moving-average crosses for 3 sessions: MA50 −0.5/−0.8 (lag0), +0.9/−0.8 (lag1); MA200
−2.0/+0.6, −0.7/+0.1. No CTA-flow continuation. 154 fade |VIXY day| > 10% next day (106 events): −0.5/+0.6 (lag0),
−1.0/+0.1 (lag1). Both **dead**. Protocol amended (rule 4b: descriptive tables stop at 2022). Registry: 282.

## 163 — Pre-holiday sessions held close→close  ·  2026-11-20
Long TLT over the pre-holiday session: −2.1 / +0.2. SPY: +0.8 / +0.9. Dead. Registry: 284.

## 013f — Month-end rule with AGG as the bond leg  ·  2026-11-20
gap = SPY MTD − AGG MTD: train t=+2.9, valid +1.4 (+12bp/trade); 82% sign agreement with the TLT version. Weaker than
TLT (consistent with 027/013d: the long-duration bond move carries the signal). Robustness: same direction, smaller.
Registry: 285.

## 013g — Month-end rebalancing as a SPY−TLT pair  ·  2026-11-20
Last trading day, pos = −sign(SPY MTD − TLT MTD) in the SPY−TLT spread (both sides of the rebalancing flow), open→close,
0.5bp SPY + 1bp TLT per side: train t=+3.37 (+35.5bp/trade), valid +2.17 (+25.0bp) → gate passed (per-trade).
**Holdout: Sharpe 1.36, t=+2.67, +39.0bp/trade, 76% hit (46 trades), max DD −2.4%.** Survived — same edge as SPY-only
(t=3.3, +37.5bp, 67% hit) with a higher hit rate; TLT leg adds little in 2023+ (consistent with 096's TLT weakness).

## Strategy-catalog sourcing (Quantpedia free pages; Alpha Architect 403)  ·  2026-11-20
Usable new flow candidates: mid-month payday (16th; 401k contributions) → 165; option-expiration Monday (already 021B,
dead). Others need non-free data (13F holdings, fund returns, commodity futures curves) or aren't flows (Ramadan).

## 165 — Mid-month payday (16th)  ·  2026-11-20
Long SPY close→close on the first session ≥ the 16th (timing value): +0.8 / +0.4. Dead (as the source itself notes it
weakened after 2010). Registry: 287.

## 167/168 — Month-end flow in credit; month-end edge vs volatility  ·  2026-11-20
167 HYG−IEF spread on the last day by the stock-bond gap: +0.9 / +0.6. Dead (no credit-sleeve footprint).
168 descriptive (2002-22): 013 per-trade bp by prior-day 20d vol tercile — SPY vol: train 13/21/34, valid −2/8/49;
TLT vol: train 14/13/40, valid 11/20/25. Monotone in both periods → one pre-specified filter registered:
**trade 013 only when SPY 20d vol > its train median**: train t=+2.64 (Sharpe 0.75, +30bp/trade), valid +2.53 (0.90,
+44bp), ~6 trades/yr → gate passed. **Holdout: Sharpe 1.47, t=+2.90, +61.5bp/trade, 78% hit, 18 trades, max DD −0.5%.**
Survived — the rebalancing price impact is larger in volatile markets (bigger $ imbalances, thinner liquidity).
Use as a sizing tilt (bigger on high-vol month-ends) rather than a hard filter, since the unfiltered rule also works.
Registry: 289.

## 176 — Quarter-end bank balance-sheet window dressing (TLT)  ·  2026-11-20
Short TLT last 3 sessions of the quarter / long first 3 of the next: **−3.7** / −0.6 (Treasuries actually rally into
quarter-ends — the month-end index-extension/rebalancing bid of 031 dominates); year-end only: −2.8 / +0.4. Dead.
Registry: 291.

## 177 — Tax-deadline flows (short SPY 3 sessions into Jan/Apr/Jun/Sep/Dec 15)  ·  2026-11-20
+0.5 / +1.2 — right sign, not significant. Weak/dead. Registry: 292.

## 178 — MSCI index-review effective days (EEM/EFA vs SPY)  ·  2026-11-20
4 variants (direction from train): EEM open→close +1.5/−0.4; EEM close→next open +0.6/+1.3; EFA open→close +0.4/+1.0;
EFA close→next open +0.4/−0.4. Dead (78 events each). Registry: 296.

## 180 — Quad-witching / S&P quarterly rebalance close → next open  ·  2026-11-20
Fade the quad-witching day's move from the close to the next open: SPY +0.4 / −1.2, IWM +0.1 / −0.3. Dead.
Registry: 298 (luck t ≈ 3.4).

## 179 — Robustness: month-end pair + vol filter on QQQ-TLT and IWM-TLT (train/valid only, no holdout look)  ·  2026-11-20
QQQ−TLT: train t=+2.14 (+42bp/trade), valid +2.51 (+58bp). IWM−TLT: +3.02 (+65bp), +2.74 (+59bp). The high-vol
month-end rebalancing effect is consistent across equity sleeves; higher-beta sleeves move more. Registry: 300.

## Paper sourcing round 3 + 182 — Earnings extrapolation (RFS 2025)  ·  2026-11-20
NBER search is JS-rendered (unreadable); Semantic Scholar and OpenAlex APIs rate-limited (429) from this proxy;
Crossref API works (api.crossref.org/works?query.bibliographic=…) but abstracts are sparse. One usable hit:
"Earnings Extrapolation and Predictable Stock Market Returns" (RFS 2025): month-1-of-quarter market return predicts
months 2-3 positively and next quarter's month 1 negatively. 182a (follow month-1 sign in months 2-3): +1.1 / +0.3;
182b (fade it in next quarter's month 1): −0.3 / −0.3. Doesn't replicate in SPY 2000-2022. Dead. Registry: 302.

## 184 — Weekly stock-bond rebalancing (Fridays)  ·  2026-11-20
Week-end version of 013 (−sign(SPY WTD − TLT WTD) on the last session of the week): −1.3 / −0.1; high-vol weeks −0.7 /
+0.5. Dead — the rebalancing effect is specific to month-ends (where balanced/pension funds actually rebalance).
Registry: 304.

## 186/189 — VIX-futures flow timing (VIXY, timing value over always-short)  ·  2026-11-20
186 short VIXY the week after monthly OPEX (vol sellers re-strike): +1.4 / +0.1. 189 short VIXY first 3 sessions of the
month: +0.9 / +0.2. Dead (train only 2011-14). Registry: 306.

## 190 — Month-end signal via the CBOE PUT index (real option prices)  ·  2026-11-20
Exposure ±1 to PUT-index excess return on the last trading day by the 013 signal: +9.2bp/trade (train, 96 obs — CBOE file
gaps) / +9.7bp (valid, 96), t=+1.2 / +1.5. About half the SPY edge, consistent with the index's ~0.5 delta: real option
prices confirm the direction; no extra edge from expressing it via written puts at monthly strikes. Registry: 307.

## 197 — Month-end FX hedge rebalancing (WM/Reuters 4pm fix)  ·  2026-11-20
Signal: short USD on the last day if SPY beat EFA month-to-date (foreign holders re-hedge grown USD exposure).
a) UUP open→close: −2.5 / −2.9 net — wrong-signed (USD tends to *rise*), but gross is only ~−4.7bp/trade, so the flipped
rule would barely clear 2bp/side costs. b) USDJPY 09:29→11:05 NY across the fix (HistData 1-min, 2001-25): −0.6 / −0.7,
~0bp. Dead as specified; no clean FX month-end hedge flow in daily data. Registry: 309.

## 205 — Credit-fund outflow pressure (HYG/LQD vs IEF after bad weeks)  ·  2026-11-20
Short HYG (LQD) / long IEF for 5 sessions after a bottom-decile 5-day return: HYG −0.9 / −0.3; LQD −0.3 / +0.2. Dead —
no outflow-driven continuation in credit ETFs. Registry: 311.

## 206 — Quarter-end damping / JPM collar (descriptive, ≤2022, not registered)  ·  2026-11-20
|open→close| relative to recent 20d vol: quarter-end days 0.74 (2005-15) / 0.80 (2016-22) vs other month-ends 0.85 / 0.92
and other days 0.87 / 0.84. Quarter-end sessions are systematically calmer in BOTH eras, so it isn't specific to the JPM
Hedged Equity collar's growth (2016+); more likely general quarter-end pinning (large quarterly option OI). Explains why
013's edge is smaller at quarter-ends (116). No trade.

## 209 — Treasury coupon/maturity reinvestment (refunding 15ths)  ·  2026-11-20
Long TLT over the 2 sessions into the 15th of Feb/May/Aug/Nov: train **+2.7** → valid −0.2; control months −0.5 / +0.3.
Worked in 2002-14, gone since. Dead. Registry: 313.

## Paper APIs blocked → literature from memory; 210/211  ·  2026-11-20
OpenAlex still 429; Crossref relevance collapses when sorted by citations. Switched to well-known flow papers directly.
210 'Dash for cash' (Etula et al., RFS 2020): short SPY over sessions −3,−2 before month-end: −1.3 / −1.4 — returns are
not low there in SPY (if anything slightly positive). Dead.
211 FOMC cycle (Cieslak-Morse-Vissing-Jorgensen, JF 2019): long SPY in even FOMC-cycle weeks, timing value: train **+2.6**
→ valid +0.4. Strong in-sample (the paper's era), faded after 2015 — consistent with post-publication decay. Dead.
Registry: 315.

## 212/213 — Intraday momentum (hedging demand); target-date-fund rebalancing lag  ·  2026-11-20
212 (descriptive, Yahoo 1h Dec 2023-Nov 2026): last half-hour in the direction of prev close→10:30: SPY −0.4bp (48% hit),
QQQ −0.4bp — the documented intraday momentum is absent recently (consistent with the 2611.09286 end-of-day-reversal
finding and our 121).
213 −sign(prior month SPY − AGG) over the first 3 sessions of the new month: −1.7 / +1.7 (sign flips). Dead — the
rebalancing footprint is on the month's LAST day (013), not the first days after. Registry: 316.

## 219 — Weekend option-decay mechanics in VIX  ·  2026-11-20
Spot ^VIX mean change by weekday 2000-22: Mon +2.10%, Tue +0.13, Wed −0.28, Thu +0.18, Fri −0.72 — strong calendar
mechanics in the (non-tradable) index. Tradable VIXY: short Fridays +1.2 / +0.4; long Mondays −0.7 / −0.7. Futures
already price the weekend pattern. Dead. Registry: 318.

## 220 — Put-writing returns by weekday (real CBOE prices)  ·  2026-11-20
Mean excess bp by weekday 2006-22: WPUT Mon +2.6, Tue +6.5, Wed +3.4, Thu +0.9, **Fri −6.9** (weekly expiry/roll day);
PUT Mon +2.9, Tue +8.0, Wed +1.1, Thu −0.4, Fri −0.1. Exposure only Fri+Mon: −2.2 / −2.2 (timing value). 220b post-hoc
'WPUT except Fridays': +0.0 / −0.3, Sharpe 0.48 vs 0.45 always-on (2006-14), 0.04 vs 0.06 (2015-22). The Friday loss is
visible on average but not a robust timing rule. Dead. Registry: 320.

## 225 — LETF rebalancing where leveraged products are large (GDX, GDXJ, SMH, XBI, TLT)  ·  2026-11-20
Fade |z|>2 days from the close to the next open, 2010+: GDX −0.1/−0.0, GDXJ −0.6/+0.5, SMH −0.4/+1.2, XBI −0.2/−0.0,
TLT +0.5/+0.3; pooled −0.7/+0.4. Even where LETFs are big relative to the underlying, no overnight reversal — this
undercuts the LETF-rebalancing reading of 112 (whose SPY result may be generic big-day overnight reversal, not LETF flow).
Dead. Registry: 326.

## 226 — Risk-parity month-end rebalancing in gold  ·  2026-11-20
GLD on the last day, −sign(GLD MTD − avg(SPY, TLT) MTD): +0.3 / −1.2. Dead — no multi-asset rebalancing footprint in gold.
Registry: 327.

## 233 — Fed QE purchase (POMO) days  ·  2026-11-20
NY Fed operation results (markets.newyorkfed.org/api/tsy/all/results/summary/search.json; saved data/nyfed_tsy.json):
1,612 outright Treasury purchases 2008-12 → 2022-12 (QE1-3 and 2019-22). Long SPY open→close on purchase days: train
+1.5 (2009-14 QE era) → valid −0.2; long TLT on long-maturity purchase days: −0.5 / +0.9. Dead — the POMO-day equity
premium reported for QE2/QE3 doesn't hold up out of sample. Registry: 329.

## 234 — SPY dividend reinvestment on pay-date month-ends  ·  2026-11-20
Long SPY open→close on the last day of Jan/Apr/Jul/Oct: −0.6 / −1.0; other month-ends −0.7 / −0.6. No unconditional
month-end buying (direction comes from the stock-bond gap, as in 013). Dead. Registry: 331.

## 245 — Overnight into the monthly SPX AM settlement (Thu close → OPEX Fri open)  ·  2026-11-20
Direction from train: SPY −0.1 / +1.6; QQQ −0.1 / +0.1. Dead. Registry: 333.

## 250 — Quarterly / annual rebalancers (QTD and YTD stock-bond gaps)  ·  2026-11-20
a) quarter-end, −sign(SPY QTD − TLT QTD): +1.2 / +0.5 (+13 / +7bp/trade). b) year-end, YTD gap: −0.6 / +1.3 (20 events).
Weaker than the MTD version even at quarter-ends — the flow keys on month-to-date drift (monthly rebalancing schedules
dominate), consistent with 116/206. Dead. Registry: 335.

## 252 — Month-end flow in a high-minus-low-beta large-cap spread  ·  2026-11-20
d × (top-quintile beta − bottom-quintile beta), last day, open→close: train +0.4 (gross +12bp/trade), valid **+2.1**
(+30bp). Right direction in both periods (high-beta names move more with the month-end flow) but fails train t; costs
of a 4-leg book (8bp/day) eat much of it. Weak, consistent with the index-level effect; not a separate edge.
Registry: 336.

## 253 — Threshold rebalancers (first |MTD stock-bond gap| > 5% / 4% in a month → next day)  ·  2026-11-20
5%: −0.1 / +1.0 (154 events); 4%: −0.4 / +0.2. No threshold-rebalancing footprint intra-month — the observable flow
is calendar (month-end) rebalancing. Dead. Registry: 338.

## 013h — Permutation tests of the month-end rule (2002-22, holdout untouched)  ·  2026-11-20
Actual +21.0bp/trade over 245 month-ends. Sign permutation (10,000 shuffles, long/short counts preserved): p < 0.0001.
Same rule on 245 random NON-month-end days (2,000 draws): mean +0.8bp, 95th pct +10.2bp, share ≥ actual 0.0005 —
the stock-bond-gap signal only works ON the month's last day. Strong, non-parametric support for 013 as a calendar flow.

## 255 — Month-end flow through volatility (VIXY)  ·  2026-11-20
−d in VIXY open→close on the last day: +20bp/trade (2011-14), +37bp (2015-22), t=+0.2 / +0.9 — right sign, too noisy
(VIXY is ~4-5× SPY's volatility, so a 20-37bp edge is small relative to its swings). Consistent, not tradable alone.
Registry: 339.

## 256 — Bond side of the month-end rebalance in curve terms (TLT−IEF)  ·  2026-11-20
+sign(stock-bond MTD gap) in TLT−IEF on the last day: +6.9bp/trade gross in both periods, t=+0.9 / +0.8. Right sign,
small. Weak. Registry: 340.

## Paper scan (arXiv API direct, https)  ·  2026-11-20
Note: arXiv API works over https from this container (http returns empty). Queries: seasonality (q-fin.TR), order flow +
ETF, predictable + flows, end of month, institutional trading + returns. ~45 new abstracts: mostly microstructure/LOB,
crypto, ML alpha-mining, electricity — no new daily-testable flow mechanisms for US ETFs. OpenAlex still 429.
Flow-idea sources look close to exhausted for free daily data; remaining avenues need intraday/auction or holdings data.

## 257 — Out-of-country replication of the month-end rebalancing rule  ·  2026-11-20
Local equity vs local government-bond ETFs, local last trading day, equity open→close, 2bp/side:
| market | train t (n) | valid t (n) | per-trade train / valid |
|---|---|---|---|
| Canada XIU/XBB | **+2.97** (169) | **+2.42** (96) | +24.8 / +21.5bp |
| UK ISF/IGLT | +1.29 (71) | +1.10 (96) | +19.7 / +13.1 |
| Australia STW/IAF | −1.61 (33) | +1.67 (96) | −14.2 / +15.8 |
| Euro EXS1/EUNH | +0.12 (68) | +1.97 (96) | +5.6 / +20.8 |
| pooled | +1.96 | **+3.35** | — |
2015-22: positive in all four markets (pooled t=3.35) — independent support for the rebalancing mechanism.
Canada passed the gate → **holdout (2023+): −2.1bp/trade net (+1.9 gross), 39% hit, t=−0.19 (46 trades). Failed.**
Pooled missed the gate on train (short histories) → no holdout look. Verdict: the month-end rebalancing effect is
international in 2015-22, but its 2023+ persistence is shown only for the US so far — a caution against over-trusting
the US holdout. Registry: 345.

## 258/263 — Japan: month-end stock-bond rule; BOJ ETF purchase support  ·  2026-11-20
258 1306.T vs 2510.T month-end rule (2018-22 only): +12.7bp/trade, t=+0.5 (60 trades) — right sign, like the other
markets in 257; too short to judge. 263 long 1306.T open→close after a >0.5% gap-down open: mean +2.9bp (2009-12),
+7.7bp (2013-20, heavy BOJ buying), −7.6bp (2021-22, BOJ scaled back) — the pattern tracks BOJ activity, but net of
3bp/side it's −0.3 / −0.5 t. Dead as a trade; interesting as evidence of central-bank flow support. Registry: 347.

## 264 — International month-end + volatility filter (train/valid only)  ·  2026-11-20
Pooled 4-country rule, trading only when local equity 20d vol > its pre-2015 median: train +1.0, valid **+2.9**
(vs +2.0 / +3.35 unfiltered). The vol filter doesn't sharpen it abroad. (Importing 257 accidentally re-ran its 5
registrations; those rows are marked DUPLICATE in the registry.) Registry: 353 rows (348 unique tests).

## 267 — International option-expiry replication of 001  ·  2026-11-20
Short on local monthly expiry, open→close: Eurex 3rd Fri (EXS1.DE) +0.5 / +0.6; Japan SQ 2nd Fri (1306.T) −0.3 / −2.2
(Japanese ETF *rises* on SQ days); Canada 3rd Fri (XIU.TO) −0.5 / +0.1. No international support for the OPEX-short tilt
— 001 looks US-specific (or noise). Registry: 356 rows (351 unique).

## 268 — Settlement-lag rebalancing day (T+3 / T+2 / T+1 eras)  ·  2026-11-20
013 signal on the session that is the settlement lag before month-end: T+3 era (pre-Sep-2017) day −3: +23.0bp/trade
(train t=+2.78); T+2 era (Sep-2017..2022) day −2: +3.2bp (valid t=+0.4). Partial support in the old regime only; under
T+2 the flow concentrates on the last day itself (013), not the settlement-lag day. Not a rule. Registry: 357 rows.

## 274 — Month-end country rebalancing by global funds (20 country ETFs)  ·  2026-11-20
Last day: long 4 MTD-loser countries / short 4 winners, open→close: +1.0 / −0.6 (+2.4 / −1.6bp per trade net). Dead —
like sectors (016a), cross-sectional rebalancing within equities leaves no footprint; only the stock-bond leg does.
Registry: 358 rows.

## 275 — Bond turn-of-month: long TLT over the last 3 sessions of each month  ·  2026-11-20
a) last 3 sessions: train t=+3.31 (Sharpe 0.94), valid +2.32 (0.82) → gate passed; **positive in 21/21 years 2002-22**
(~+4.4%/yr at full exposure, ex-best-3-years +65% of +92%). Mechanism: Treasury index month-end duration extension and
balanced-fund bond buying. b) first 3 sessions: −3.2 / −0.9 (the month-end bid partly reverses early next month).
**Holdout (2023→Nov 2026): Sharpe 0.31, t=+0.60 @1bp; years +2.2% / +4.8% / +3.8% / −5.3% (2026 YTD).** Weak-but-alive:
same sign 3 of 4 years, much smaller than 2002-22. (Distinct from 031's last-day open→close version, which failed 096's
holdout check.) Registry: 361 rows.

## 276 — Month-end Treasury bid along the curve (train/valid consistency; no holdout look, as pre-specified)  ·  2026-11-20
Long over the last 3 sessions of each month: IEF train t=+3.39 / valid +3.24 (Sharpe ~1.0-1.1), gross +2.8%/yr;
SHY +4.34 / +3.21 (Sharpe ~1.1-1.2), gross +0.9%/yr; vs TLT +3.31 / +2.32, ~+4.4%/yr. Present at every curve point,
size scaling with duration — the classic month-end Treasury effect (index extension + month-end portfolio buying).
Robust in 2002-22; its 2023+ strength is only weakly confirmed (275 holdout t=0.6). Registry: 363 rows.

## 277/278 — Month-end bond bid in credit and abroad (train/valid consistency; no holdout looks)  ·  2026-11-20
Long over the last 3 sessions of each month: LQD +1.75 / +2.67; HYG +1.16 / +1.68; UK gilts IGLT.L +0.5 / +1.0;
Canada XBB.TO +1.95 / +2.64; euro govts EUNH.DE +0.9 / +0.8. Right sign in all 10 period-instrument pairs; strongest in
US Treasuries/IG credit and Canadian bonds. Supports a broad month-end bond-index-extension bid. Registry: 368 rows.

## 279 — Survivor portfolio v2 (construction, 2002-22 only)  ·  2026-11-20
Legs: A 013 SPY month-end (vol-tilted: 1.0 high-vol / 0.5 otherwise), B IEF last-3-sessions month-end bid (276),
C monthly country reversal (062b). Pairwise correlations ≈ 0 (|ρ| ≤ 0.03). Sharpe (max DD):
2003-14: A 0.84 (−3.3%), B 0.87 (−6.1%), C 0.30 (−15.9%), equal-risk combo **1.14 (−2.0%)**;
2015-22: A 0.82 (−2.3%), B 1.14 (−2.6%), C 0.52 (−7.8%), equal-risk combo **1.38 (−1.7%)**.
Diversified month-end flow book. Caveats: B and C were never holdout-confirmed (B's TLT sibling 275 was weak in 2023+);
returns are small in absolute terms (legs active on a few days a month) unless levered.

## 276b — Holdout look for the IEF month-end bid (2nd look in the bond-bid family)  ·  2026-11-20
276 was pre-specified as consistency-only; this deliberate look is logged as the family's 2nd holdout look (after 275/TLT).
IEF long over the last 3 sessions of each month, 2023→Nov 2026: **Sharpe 0.83, t=+1.63 @1bp (0.72 / 1.42 @2bp),
+1.9%/yr, max DD −2.9%; yearly +1.2 / +3.1 / +3.3 / −0.3%.** Weak-but-alive, better than TLT's holdout (0.31).

## 276c — Anatomy of the month-end bond bid (descriptive, 2002-22, not registered)  ·  2026-11-20
IEF mean bp by session from month-end: last day +11.2, day −2 +6.8, day −3 +5.1, day −4 −0.8, day −5 +3.3, day −6 +1.2
(all-day mean 1.5). Builds into the final session. Independent of the stock-bond gap: +6.7bp/session when stocks led vs
+8.8 when they lagged — i.e. a mechanical index-extension / month-end portfolio bid, distinct from 013's equity
rebalancing (which is why the two legs are uncorrelated in 279).

## 276d — Bond bid in refunding vs other months (descriptive, 2002-22)  ·  2026-11-20
Mean last-3-session sum: IEF +23.4bp (refunding months Feb/May/Aug/Nov, t=3.0, n=82) vs +23.1bp (others, t=4.4, n=163);
TLT +43.7 vs +37.7bp. No meaningful difference — the bid is present every month, so no refunding filter.

## 280 — Month-end bond bid in other index segments (consistency, no holdout looks)  ·  2026-11-20
Long over the last 3 sessions: MBB (agency MBS) +2.61 / +3.23; TIP (TIPS) +3.31 / +2.91; MUB (munis) +0.87 / +0.98.
Strong in index-tracked taxable bonds (Treasuries, TIPS, MBS, IG credit), weak in munis (not in the main taxable bond
indices' monthly extension) — a nice cross-check of the index-extension mechanism. Registry: 371 rows.

## 281 — Full month-end bond cycle (long last 3 / short first 3 sessions; consistency)  ·  2026-11-20
IEF +3.52 / +2.27 (vs long-only +3.39 / +3.24); TLT +4.34 / +2.03 (vs +3.31 / +2.32). The early-month reversal leg helped
in 2002-14 but not 2015-22 → keep the long-only month-end bid. Registry: 373 rows.

## 284 — Month-end bond bid in global bond ETFs (consistency)  ·  2026-11-20
BWX (intl govt, unhedged) +1.2 / +0.6; EMB (EM USD sovereigns) +2.65 / +1.38. Right sign, weaker than US Treasuries/MBS/
TIPS (FX noise in BWX; EMB partly tracks the US-dollar index-extension bid). Registry: 375 rows.

## 285 — Controls for the month-end bond bid (assets without bond-index extension)  ·  2026-11-20
Last-3-session timing value: GLD −0.1 / −1.7; SPY +0.2 / −0.5; UUP +0.1 / −1.1. No month-end bid outside bonds — the
effect is specific to index-tracked bonds, as the index-extension mechanism predicts. Registry: 378 rows.

## 028 — Japanese fiscal year/half-year end (EWJ)  ·  2026-11-20
Long EWJ on the last day of Mar/Sep: +0.5 / +1.1 (+14.5bp/trade valid, 46 events); short first 3 sessions of Apr/Oct:
−0.1 / +0.3. Right-signed but underpowered. Dead/weak. Registry: 380 rows.

## 276e — Permutation test of the month-end bond bid (2002-22, descriptive)  ·  2026-11-20
Mean return over the last 3 sessions vs a random 3-session window in each month (5,000 draws): IEF +23.2bp vs +2.7bp
(99th pct +12.2; p<0.0002); TLT +39.7bp vs +2.6bp (99th pct +23.4; p<0.0002). The month-end window is extreme relative
to any other 3-day window — strong non-parametric support.

## 286 — Mid-month Treasury cash flows (IEF, 3 sessions into the 15th)  ·  2026-11-20
+1.1 / +0.5. Dead — the bond bid is a month-END (index rebalancing) effect, not a coupon-date effect. Registry: 381 rows.

## Status note — idea saturation  ·  2026-11-20 14:35 UTC
~375 unique tests. Robust findings: (1) month-end stock-bond rebalancing in SPY (holdout t=3.3, permutation p<0.0001,
international 2015-22 support, vol-scaled); (2) month-end bond bid in index-tracked bonds (21/21 years 2002-22, t>3 on
IEF/SHY/TIP/MBB, permutation p<0.0002, controls null; holdout IEF Sharpe 0.83). Free daily-data flow ideas, arXiv, and
strategy catalogs are largely exhausted; OpenAlex/Semantic Scholar rate-limited. Highest-value next steps need data we
don't have for free: intraday/auction data (opening/closing imbalance, MOC flows), fund holdings/flows, options open
interest. Rounds continue per the user's instruction, prioritising robustness work on the two findings.

## 276f — Bond-bid stability by 5-year block (2002-22, descriptive)  ·  2026-11-20
Last-3-session bp per month (mean, t): IEF 17.6 (1.3) / 22.5 (2.2) / 25.5 (3.4) / 22.7 (3.3) / 25.7 (2.0) for
2002-04 / 05-09 / 10-14 / 15-19 / 20-22; TLT 23.8 / 37.7 / 54.7 / 43.0 / 25.4; SHY 7.0 / 10.4 / 5.3 / 6.0 / 6.8 (t 2.4-5.7).
Positive in every block for every maturity, no decay in IEF/SHY (TLT smaller recently) — unlike most anomalies tested,
this one hasn't been arbitraged away (consistent with its mechanical, size-driven nature).

## 279b — Month-end flow book scaled to 10% volatility (construction, 2003-22)  ·  2026-11-20
Raw legs run at ~2-3% vol (active only a few days a month). Scaled to 10% annual vol (≈4-4.6× notional on trade days,
e.g. via ES/ZN futures): SPY month-end leg ≈ +8.4% / +8.2% per year (2003-14 / 2015-22), IEF bond-bid leg ≈ +8.7% /
+11.4%, equal-risk combo ≈ +11.4% / +13.8% — before financing, slippage on larger size, and with the caveat that only
the SPY leg is solidly holdout-confirmed (IEF holdout Sharpe 0.83). Not advice; illustrates the economic size.

## 276g — Bond bid: overnight vs intraday split (descriptive, 2002-22)  ·  2026-11-20
Last-3-session mean bp per session: IEF overnight +3.7 (t=3.7), intraday +4.0 (t=3.8); TLT +6.2 / +7.0 (t=3.1 each) vs
all-day ~0.7-1.3. The bid is split evenly across overnight and trading hours → hold through the window (enter at the
close 4 sessions before month-end, exit at the month-end close), as specified in 275/276.

## 276h — Bond-bid window length (descriptive, 2002-22)  ·  2026-11-20
Gross Sharpe of the daily series by window (last k sessions): IEF k=2 1.11, k=3 1.12, k=4 0.95, k=5 0.99, k=7 0.87;
TLT 0.93 / 0.94 / 0.72 / 0.79 / 0.75. Mean bp/session falls as the window widens (IEF 9.0 → 3.8). The pre-specified
3-session window sits at the plateau — no evidence the choice was lucky; 2-3 sessions is where the bid concentrates.

## 276i — Bond bid vs month-to-date rate direction (descriptive, 2002-22)  ·  2026-11-20
IEF last-3-session sum when 10y yields ROSE month-to-date (to the window start): +22.0bp (t=3.5, n=128); when they FELL:
+24.5bp (t=4.1, n=117). Regime-independent — the bid doesn't depend on recent rate moves, again pointing to a
mechanical calendar flow. (So the weaker 2023+ TLT result isn't explained by the rate direction alone.)

## 287 — Intraday timing of the month-end equity flow (HistData SPX CFD 1-min, 2010-22)  ·  2026-11-20
141 month-ends; signed bp by segment: 09:30-12:00 +8.3 (t=2.2), 12:00-15:00 +4.9 (t=1.6), 15:00-15:50 +1.2 (t=0.5),
15:50-16:00 +2.6 (t=1.2). Over 2010-22 the edge accrues mostly in the MORNING, not at the close (the 2023-26 hourly
snapshot in 013c suggested a strong last hour — that was a short-sample impression). Registered variant 'last hour only
(15:00→16:00)': +0.2 / +0.9 — weak. Execution conclusion: hold the full session from the open (as 013 specifies).
Registry: 382 rows.

## 288-290 — Intraday flow patterns on 15 years of 1-min S&P data (HistData SPX CFD)  ·  2026-11-20
Gross bp/day 2010-14 / 2015-22; net t (0.5bp/side) train / valid:
- 288 intraday momentum (prev close→10:00 predicts 15:30→16:00): +0.2 / +0.3bp gross; −1.3 / −1.0 net. Dead after costs.
- 289 LETF pre-positioning (prev close→15:00 predicts 15:00→16:00): +1.6 / +1.7bp gross, consistent in both periods;
  +0.7 / +0.8 net. Small and below significance after 1bp round-trip costs — the documented effect exists but is tiny.
- 290 end-of-day reversal (09:30→15:30 reversed in the last half-hour): +0.3 / −0.3 gross; dead.
Registry: 385 rows.

## 291/292 — FOMC post-announcement drift; OPEX-Friday intraday timing (SPX CFD 1-min)  ·  2026-11-20
291 FOMC days 2010-25: fade the 14:00→14:30 reaction until the close (direction from train): +0.3 / +0.7. Dead.
292 (descriptive 2010-22, 153 OPEX Fridays) short-signed bp: 09:30-10:30 −1.2 (t=−0.5), 10:30-12:00 **+8.7 (t=2.5)**,
12:00-15:00 +5.9 (t=1.5), 15:00-16:00 −0.8. The OPEX-Friday weakness happens mid-session (after the AM settlement hour,
10:30-15:00) — consistent with dealer hedge unwinds once the SOQ is fixed. Not a new rule (it's 001's edge, located).
Registry: 386 rows.

## 293 — Month-end equity trade: morning-only vs full session (SPX CFD, post-hoc, no holdout)  ·  2026-11-20
Morning only (09:30-12:00): +6.9bp/trade, per-trade mean/sd 0.158; full session: +16.1bp, 0.223 → full session is
better; don't truncate. CFD replication also re-confirms 013e: ~0 in 2010-14 (t −0.0) and strong in 2015-22 (t +2.9).
Registry: 388 rows.

## 294 — Overnight unwind after the month-end equity flow  ·  2026-11-20
−d from the last day's close to the next open: +4.9bp/trade gross (2002-14), +11.3bp (2015-22); t=+0.7 / +1.3. Some
partial unwind overnight (right sign, growing), not significant. Weak. Registry: 389 rows.

## 295 — Month-end intraday timing replicated on the Nasdaq-100 CFD (2010-22, descriptive)  ·  2026-11-20
141 month-ends, signed bp: 09:30-12:00 +13.6 (t=2.7), 12:00-15:00 +5.1 (t=1.3), 15:00-15:50 +1.4, 15:50-16:00 +1.9.
Same profile as the S&P (287): the month-end rebalancing move is front-loaded into the morning, larger for the
higher-beta index. Confirms 'enter at the open'.

## 296 — Does the month-end flow start overnight? (SPX CFD, 2010-22, descriptive)  ·  2026-11-20
Signed by 013's signal: 16:00(d−1)→20:00 +3.7bp (t=1.1), 20:00→04:00 −5.5bp (t=−1.3), 04:00→09:30 +1.1bp (t=0.3).
No consistent pre-open move — the flow starts at the cash open (287/295), so entering at the prior close (127) only adds
noise. Execution: enter at the open, as specified.

## 297 — OPEX-Friday short, mid-session only (post-hoc refinement of 001; SPX CFD, no holdout)  ·  2026-11-20
Short 10:30→15:00 on monthly OPEX Fridays: train t=+2.39, valid +1.46, +13.6bp/trade, mean/sd 0.202 vs the full-session
CFD baseline +1.42 / +1.27, +11.5bp, 0.143. Concentrating on the post-settlement window improves risk-adjusted edge
~40%, but it's post-hoc and still not significant in 2015-22. Practical note for anyone using the 001 tilt: avoid the
first hour (settlement noise) and the last hour. Registry: 391 rows.

## 298 — Does the month-end bond bid scale with Treasury coupon issuance? (dose-response, 2002-22)  ·  2026-11-20
Monthly note/bond/TIPS issuance (fiscaldata, by issue month) relative to its trailing 12-month mean, terciles cut on
2002-14. IEF last-3-session bp, low/mid/high issuance: 2002-14 +26.0 / +18.3 / +24.1; 2015-22 +33.0 / +17.9 / +21.2;
correlation with the window return 0.006 (TLT 0.019). **No dose-response** — the bid is not proportional to new supply,
so it looks more like a generic month-end portfolio/benchmark bid (or extension driven by aging/maturities, which this
proxy misses) than a pure "new issues enter the index" effect. Sized variant (0.5/1/1.5 by tercile) is no better than
flat (IEF +3.15 / +2.88 vs +3.39 / +3.24). Keep the flat bond-bid leg. Registry: 392 rows.

## 299 — CFTC futures positioning (TFF report; new data source)  ·  2026-11-20
Source: CFTC public reporting API (publicreporting.cftc.gov/resource/gpe5-46if.json, filter cftc_contract_market_code;
ES 13874A, 10y 043602, bond 020601; weekly 2006-2026). Signal applied from the first close after the Friday release,
held one week; 2009-14 train (needs 2y of z-score history). Six pre-specified variants (train t / valid t):
A SPY contrarian to leveraged-fund z>|1| +1.0 / −0.8; B SPY follow asset-manager 4w change −1.8 / +0.7;
C TLT contrarian to leveraged funds in 10y notes +1.2 / +1.5 (timing value t=1.8, 2009-22); D TLT follow asset managers
+0.8 / −0.1; E TLT contrarian to lev funds in bonds −0.8 / +0.8; F SPY follow dealers +0.6 / +1.2.
Dead — weekly positioning doesn't time SPY/TLT. C (fade crowded leveraged 10y positioning) is the only same-sign pair,
far from the gate. Registry: 398 rows.

## 300 — Month-end bond bid vs asset managers' Treasury-futures length (mechanism check, 2009-22, descriptive)  ·  2026-11-20
Terciles of asset managers' net long in 10y futures (share of OI, z vs 3 years; last release before the window):
IEF +18.5 / +32.2 / +17.6bp, TLT +38.3 / +57.8 / +26.6bp; correlation ≈ +0.05. No dose-response. Together with 298
(no link to new issuance), the bid looks like a broad month-end benchmark/cash-deployment effect that can't be pinned
on one measurable flow with free data. Doesn't change the trade (flat, every month). Not registered (descriptive).

## 301 — Crowded short-vol positioning (CFTC TFF, VIX futures)  ·  2026-11-20
Same timing as 299. A SPY sign of leveraged-fund VIX z (|z|>1): −1.5 / −0.6 (wrong sign); B asset-manager VIX z: +0.1 / +0.8;
C long SPY but flat when lev funds are unusually short VIX: Sharpe 0.64 / 0.38 vs buy-and-hold, timing value t=−1.2.
Dead — crowded short-vol positioning didn't foreshadow sell-offs at a weekly horizon. Registry: 401 rows.

## 302 — Month-end bond bid by month type (descriptive, 2002-22)  ·  2026-11-20
Last-3-session bp, ordinary / Mar-Jun-Sep / December months: IEF +22.0 (t=4.1) / +25.4 (3.1) / +25.5 (1.6);
TLT +37.5 / +45.0 / +41.1; SHY +6.6 / +7.6 / +9.9. Slightly larger at quarter- and year-ends but essentially uniform —
unlike the equity rebalancing trade (116: concentrated in ordinary months). No quarter-end filter needed. Not registered.

## 303 — Month-end equity trade (013) vs asset managers' equity-futures length (descriptive, 2009-22)  ·  2026-11-20
013 signed open→close bp by tercile of asset-manager net ES length (z, last release before the day), low / mid / high:
all trades +20.0 / +17.2 / +4.7; short-side (stocks led) +25.3 / +22.0 / +19.2; long-side (stocks lagged)
+13.3 / +8.4 / **−19.4** (n=21). The buy-side month-ends fail when real money is already heavily long equity futures —
plausible (less under-weight to refill), but n≈20 per cell and post-hoc. Not adopted as a filter; noted as a watch item
(would need a pre-registered test on new data). Not registered.

## 304 — "Fed net liquidity" (WALCL − TGA − RRP) as a weekly SPY signal  ·  2026-11-20
FRED weekly data, available from the Friday after the Wednesday level (first used at Monday's close). A sign of 4-week
change +1.5 / +0.9 (timing value t=1.2, 2003-22); B 13-week −/+ +1.3 / +0.2; C long only when 4w change > 0:
Sharpe 0.59 / 0.61, t +2.0 / +1.7 — but its timing value over an exposure-matched long is only t=1.2. Fails the gate.
The popular liquidity narrative has the right sign but is mostly just "be long more often". Weak. Registry: 404 rows.

## 305 — Tax-date reserve drain (TGA jumps on quarterly tax dates)  ·  2026-11-20
DTS daily cash (fiscaldata operating_cash_balance; 2005+): the TGA rises a median $56bn on tax dates vs −$0.5bn otherwise,
and 45% of the top-2% TGA-jump days are tax dates — the drain is real and calendar-predictable. But SPY on the top-2%
drain days: +5.2bp vs +4.2bp average. Registered calendar rule, short SPY close→close: A tax day −0.1 / +0.4;
B tax day + next −1.9 / −0.7 (wrong sign: the day after a tax date averaged strongly positive, which is post-hoc and
overlaps the generally firm OPEX week — not pursued). Dead: reserve drains don't move equities day-to-day.
Registry: 406 rows.

## 306 — Weekend crypto moves → Monday US equities (arXiv 2610.35702)  ·  2026-11-20
BTC-USD Friday→Sunday (UTC closes), known before Monday's open. Correlation with Monday's SPY gap +0.09, with Monday
open→close −0.01 (2015-22). Follow the weekend crypto move on Monday open→close: SPY valid t −0.7, QQQ −0.9; QQQ only
when |BTC weekend| > 5%: +0.6. Dead (and no 2000-14 sample exists). Registry: 409 rows.

## 307 — Bond-proxy control for the month-end bond bid  ·  2026-11-20
Last-3-session timing value over always-long: XLU +0.9 / +0.1, VNQ +1.5 / +0.3, IYR +1.5 / +0.4. Rate-sensitive equities
don't share the bid in 2015-22 (where IEF has t=3.2) → it's specific to index-tracked bonds, not a general month-end
rates move. Consistent with 285 (GLD/SPY/UUP controls). Registry: 412 rows.
(Also tried: S&P 500 deletion-rebound test — blocked, Wikipedia no longer carries the historical changes table.)

## 308 — Commodity hedging pressure / speculator crowding (CFTC disaggregated COT)  ·  2026-11-20
Paper idea (de Roon-Nijman-Veld 2000; Bessembinder 1992). WTI→USO, gold→GLD, nat gas→UNG, silver→SLV; weekly, first used
after the Friday release; equal-weight, 2bp/side. A fade managed-money crowding (|z|>1): −0.2 / −0.2 (timing t −0.5);
B follow producer hedging pressure: +0.5 / −1.6 (timing t −0.7). No leg consistent across periods. Dead — the classic
hedging-pressure premium doesn't show up at a weekly horizon in liquid commodity ETFs since 2009. Registry: 414 rows.

## 309 — Month-end trade (013): delayed-entry execution check (SPX/NSX CFD, 2010-22, descriptive)  ·  2026-11-20
Signed bp to 16:00 by entry time, S&P: 09:30 +17.1 (t=2.8) | 09:35 +15.8 (2.7) | 09:45 +15.4 (2.7) | 10:00 +14.2 (2.4);
Nasdaq-100: +22.1 | +20.0 | +17.9 | +16.5. Entering 5-30 minutes late keeps ~85-90% of the edge — **not an opening-print
artifact** (unlike 004/006, which vanished at 09:35). Execution needn't rely on the opening auction. Not registered.

## 310 — Month-end bond bid: overnight vs intraday (IEF/TLT daily opens, 2002-22, descriptive)  ·  2026-11-20
IEF bp (t): 3rd-last session overnight +5.3 (2.9) / intraday −0.1; 2nd-last +1.6 / +5.2 (3.3); last +4.3 (2.6) / +6.9 (3.6)
(all-day averages +0.7 / +0.8). TLT same shape (+9.4 / −0.8; +1.6 / +9.0; +7.7 / +12.7). The first chunk of the bid
arrives overnight into the 3rd-last session, so **enter at the close of the 4th-last session (MOC), not the next open**
— waiting for the open gives up ~20% of the window. Exit at the last day's close. Not registered.

## 311 — Does the OPEX-Friday weakness extend to weekly expiries? (SPX CFD, 2011-22)  ·  2026-11-20
Short on non-monthly Fridays: 10:30→15:00 −1.8 / +0.6 (−1.1bp/trade); 09:30→16:00 −0.5 / −0.2; Thursday control 10:30→15:00
−1.0 / −1.3. No weekly-expiry effect. If 001/297's tilt is real it is tied to the large monthly open interest, not to
expiry per se — no support from this out-of-sample angle. Registry: 417 rows.

## 312 — Off-exchange short volume ("DIX"-style) as a flow signal (FINRA daily files, 2019-22)  ·  2026-11-20
FINRA consolidated short-volume files (cdn.finra.org/equity/regsho/daily, free only from 2019; tools/finra_shvol.py keeps
S&P 500 / SPY / QQQ aggregates). Ratio z vs 60 sessions, used from the next day's close. Timing value t (2019-20 / 2021-22):
A long 5d after high S&P short ratio +0.1 / +0.3; B sign of z, 1-day +1.3 / −0.0; C long 5d after high SPY short ratio
+0.4 / −1.2. Dead — the popular dark-pool/short-volume signal adds nothing beyond being long (2023+ left untouched).
Registry: 420 rows.

## 313 — Treasury leveraged-ETF rebalancing reversal (TLT after big days)  ·  2026-11-20
Fade TLT after |close→close| > 1.5% / 2% (TMF/TMV rebalance in the move's direction into the close): next-day
open→close +1.1 / −0.8 and +1.1 / −0.4; overnight close→open −0.4 / +0.9 (2bp/side). Dead — Treasury LETFs are too small
relative to the bond market to leave a footprint. Registry: 423 rows.

## 314 — Option hedging-demand spikes (CBOE put/call ratios, 2006-2019)  ·  2026-11-20
CBOE equity/index P/C CSVs (free history ends Oct 2019). z vs 60 sessions, used from the next close (lag=1).
A long SPY 5 sessions after z(equity P/C) > 1.5: train +1.62 / valid +2.18 (Sharpe 0.57 / 1.00), timing value over
exposure-matched long t=+1.38 / +1.88 — **weak but consistent**, fails the gate on train t. B index P/C: +0.5 / +1.5 (timing
−0.2 / +0.9). C 1-day contrarian sign: −1.9 / −1.1 — consistently wrong-signed, i.e. the day after a hedging spike tends to
be weak and the rebound comes over the following week (dealer hedge unwind). Post-hoc shape, not chased. No post-2019 free
data → no holdout possible. Registry: 426 rows.

## 314b — Put/call hedging-spike rule extended through 2022 (CBOE daily JSON, Oct 2019+)  ·  2026-11-20
Spliced cdn.cboe.com market-statistics JSON (tools/cboe_pc_daily.py) onto the CSV history; the JSON equity ratio runs
lower (0.58 vs 0.68 around the splice — different product scope), which only affects the 60-day z for ~3 months.
Same rule as 314A: timing value t 2007-14 +1.36 | 2015-Sep 2019 +1.86 | **Oct 2019-2022 −0.30**; valid 2015-22 overall
t=+1.23 (was +2.18 to 2019). The lead fades in the new data → downgrade 314A from weak-consistent to dead. Registry: 427.

## 315 — Back-cast of the month-end bond bid to 1962 (FRED constant-maturity yields; never-used data)  ·  2026-11-20
Yield decline over the last 3 sessions of each month (bp, t):
DGS10 1962-72 −0.1 (−0.2) | 1973-82 −0.1 (−0.1) | 1983-92 +1.8 (1.6) | 1993-01 +1.9 (1.8) | 2002-22 +2.5 (4.3)
DGS2  1973-82 +2.1 (0.8) | 1983-92 +3.0 (2.7) | 1993-01 +2.3 (2.0) | 2002-22 +2.8 (5.7)
DGS30 1973-82 −0.9 (−0.4) | 1983-92 +1.7 (1.6) | 1993-01 +2.3 (2.3) | 2002-22 +2.4 (4.5)
Permutation vs random 3-day windows, pre-2002 only: p = 0.03 (10y), 0.006 (2y), 0.03 (30y).
**The bid is absent before bond indexing (1962-82) and appears from 1983 on, at a stable ~2-3bp of yield per month-end,
in every decade since** — exactly the timeline the index-extension / benchmarked-manager mechanism predicts (Lehman
indices launched 1973; index-tracked bond assets grew from the mid-1980s). Unlike 013's equity trade (weak pre-2002, 040),
the bond bid replicates on ~20 years of never-used data. Strongest robustness evidence for finding 2 so far. Not registered.

## 316 — Back-cast of the month-end equity rebalancing trade (013) to 1962-92 (never-used data, descriptive)  ·  2026-11-20
^GSPC last-day close→close (no reliable opens pre-1990s), bond leg = −7 × ΔDGS10. bp/trade (t):
1962-72 +3.4 (0.6) | 1973-82 −9.8 (−1.2) | 1983-92 −1.0 (−0.1) | same proxy 2002-14 +24.1 (3.0) | 2015-22 +6.7 (0.7).
No equity month-end rebalancing effect before 2002 (with 040's weak 1993-2002 result) — unlike the bond bid (315), which
is present from 1983. 013 is a modern effect, consistent with the post-2000s growth of target-date/balanced/risk-parity
assets that rebalance mechanically; it could also fade if those flows change. The cruder proxy (close→close, yield-based
bond leg) is noisier than the real 013 in 2015-22. Not registered.

## 317 — Month-end bid back-cast in corporate yields (Moody's Aaa/Baa daily, FRED, 1983+; descriptive)  ·  2026-11-20
Yield decline over the last 3 sessions (bp, t): Aaa 1983-92 +0.5 (0.7) | 1993-01 +1.8 (2.0) | 2002-22 +2.3 (4.1);
Baa +1.8 (2.5) | +1.8 (2.1) | +2.4 (4.5). Pre-2002 permutation p = 0.03 (Aaa), <0.001 (Baa). The month-end bid shows up
in investment-grade corporate yields too, from the 1980s-90s on, matching LQD (277) — consistent with a broad
bond-index extension bid (part of it is the Treasury move these yields embed). Not registered.

## Forward paper ledger started  ·  2026-11-21
tools/paper_ledger.py + results/paper_trades.csv: pre-committed trades for each month-end, written before the event.
November 2026 (planned with data through 2026-11-20): EQ LONG SPY 2026-11-30 open→close, full size (high vol;
gap −8.66%, provisional until the 11-27 close); BOND long IEF 2026-11-24 close → 2026-11-30 close (final).

## 318 — Index-eligibility control for the bond bid: bills vs notes (FRED, 1973-2022, descriptive)  ·  2026-11-21
Yield decline over the last 3 sessions (bp, t), 1983-92 / 1993-01 / 2002-22, and pre-2002 permutation p:
3m bills (outside bond indices) +1.6 (1.5) / +0.3 (0.4) / +0.75 (1.9), p=0.44; 1y (edge of eligibility) +2.4 (2.1) /
+0.8 (0.8) / +1.2 (3.2), p=0.39; 2y (inside) +3.0 (2.7) / +2.3 (2.0) / +2.8 (5.7), p=0.006.
The bid fades sharply below the 1-year index cut-off — bills show little or nothing (small 2002-22 drift plausibly from
money-fund month-end flows). Another mechanism check consistent with bond-index extension. Not registered.

## 319 — Does the month-end bond bid reverse early next month? (FRED back-cast, descriptive)  ·  2026-11-21
Yield decline over the first 3 sessions of the month (bp, t), 1973-82 / 83-92 / 93-01 / 02-14 / 15-22:
10y −0.4 (−0.2) / +1.9 (1.6) / +0.9 (0.8) / **−2.4 (−2.7)** / −1.3 (−1.4); 2y +0.3 / +2.6 (2.1) / +1.3 / 0.0 / −1.1.
No reversal before 2002 (early-month yields kept drifting down, i.e. turn-of-month buying spread into the new month);
a partial reversal in 10y appears only after 2002, when the bid became more concentrated/anticipated. The month-end
window itself (315) is the stable part — keep the trade long-only into month-end, as specified. Not registered.

## 320 — Japanese fiscal-year-end repatriation in FX (USDJPY, 2000-22)  ·  2026-11-21
Short USDJPY over the last 10 sessions of March: −1.3 / −0.4 (−84 / −26bp per window); March+September: −1.0 / −1.2;
long USDJPY first 10 sessions of April: +1.1 / −0.6. Wrong-signed: the yen if anything WEAKENS into fiscal period-ends
(repatriation is presumably hedged/spread out, or swamped by other flows). Dead. Registry: 430 rows.

## 321-323 — Market-behaviour round (user request 2026-11-21)  ·  2026-11-21
321 "Vanna/charm" OPEX-week drift (long SPY Mon-Thu of monthly OPEX week): unconditional +0.7 / +0.2; only when VIX was
elevated at the prior Friday −0.5 / +0.3; elevated and falling −1.4 / +0.3. Dead — no dealer-hedge-unwind drift on daily data.
322 OPEX strike pinning (SPX CFD 1-min, bet on moving toward the nearest 25/50-point strike 13:00→15:55): OPEX Fridays
K=25 −0.7 / −1.2 (−6.4bp/trade, 42% hit); K=50 −1.8 / −1.2; other Fridays −/+ ~0. Wrong-signed: the index tends to move
AWAY from round strikes on expiry afternoons (caveat: the CFD level can be offset from cash SPX by a few points). Dead.
323 "Spot up, vol up" (SPY > +0.5% and VIX > +5% same day; lag-1): short 1d −1.1 / +0.4; short 5d +1.0 / +0.8;
mirror "spot down, vol down" long 5d +0.6 / +2.7 — but only ~20 events per side in 2000-22: right-signed, underpowered,
not tradeable as specified. Registry: 439 rows.

## 324 — "Spot up / vol up" with looser pre-set thresholds (power check of 323)  ·  2026-11-21
Timing value t (2000-14 / 2015-22): short 5d after SPY up & VIX up (581 events) +0.6 / +0.4; with SPY>+0.25% & VIX>+2%
(101) −0.4 / +0.1; long 5d after SPY down & VIX down (552) +1.0 / +1.1; with thresholds (99) −0.1 / +2.3. With power, the
effect mostly disappears; the 'down/down' long is mostly just being long. Dead/weak. Registry: 443.

## 325 — Re-leveraging after a volatility spike fades (vol-target funds adding back)  ·  2026-11-21
Trigger: VIX ≥25% below its 20d high, high ≥25 (first in 20 sessions; 37/28 events). Long SPY (lag-1): 10d timing t
−0.9 / +1.1; 20d +0.1 / +2.0 (valid Sharpe 0.85, t 2.40); spike ≥30, 10d −1.1 / +1.3. Works only in 2015-22 (the
vol-target era?) — fails train; weak. Registry: 446.

## 326 — Next-session reversal after the biggest days (agent-run; write-up results/agent_326.md)  ·  2026-11-21
Fade top-5% |moves| (vs trailing 252d) on a 50/50 SPY+QQQ book; 427 train / 271 valid events. Train t / valid t:
A close→next open −0.9 / +1.5; B next-day open→close +1.3 / +0.6; C close→close lag-1 +0.3 / −0.6; D close→close
lag-0 +0.9 / +1.7. Valid strength is mostly 2020 (79-99% of A/D's valid P&L). Dead.

## 327 — Holiday-shortened weeks / long-weekend flows (agent-run; results/agent_327.md)  ·  2026-11-21
Calendar-known (lag 0). A long SPY through holiday weeks +1.4 / +2.5 (timing t 1.0 / 1.9; 2020 dominates); B SPY
open→close after a holiday +0.7 / +0.1; C short TLT after a holiday −0.0 / −1.5; D short SPY Friday before a long
weekend +0.5 / −1.6. Dead (A weak, mostly market exposure).

## 328 — Mega-cap earnings clusters / buyback blackout (agent-run; results/agent_328.md)  ·  2026-11-21
New data: SEC EDGAR 8-K Item 2.02 filing dates for 15 long-lived mega-caps (tools/edgar_earnings.py), 72 cluster
quarters 2005-22. A long SPY 5 sessions after the cluster week (lag-1): +1.3 / +2.1 (timing t 0.9 / 1.6; 2008/2020
driven); B after-minus-before contrast +0.4 / +0.3; C QQQ−SPY after +0.6 / −1.0; D contrast −/+ ~0. Dead (agrees with 113).

## 333 — Buyback-desk dip support: open window vs blackout (agent-run; results/agent_333.md)  ·  2026-11-21
Blackout = ≥8 of 15 mega-caps in pre-earnings blackout (EDGAR dates from 328). Long SPY t+1→t+4 after a −1.5% day:
open-window dips +0.7 / −0.5; blackout dips −0.5 / +1.5; open−blackout contrast +0.9 / −1.4 (−1.0% trigger +0.7 / −0.9).
Sign flips between periods (2008 vs 2020). Dead — agrees with 113/328.

## 332 — Mortgage convexity-hedging continuation (agent-run; results/agent_332.md)  ·  2026-11-21
Trigger: 5-day |Δ10y| > 21bp (train 90th pct); follow the move 5 sessions (lag-1), 1bp/side; "near the money" = Freddie
30y within ±75bp of its 3-7y-earlier average. A TLT all triggers +0.5 / +0.2; B TLT in-zone +0.7 / −0.4; C IEF in-zone
+0.1 / +0.1; D short MBB / long IEF in-zone +1.1 / +0.4 (2008-09 driven). Dead.

## 329 — Gotobi (Tokyo 09:55 JST fix) USD buying (agent-run; results/agent_329.md)  ·  2026-11-21
USDJPY 1-min (HistData), hand-built Japanese holiday calendar, 0.5bp/side. Long 08:00→09:55 JST on gotobi days:
train t **+3.68** / valid +0.62 (+0.4bp/trade net); post-fix fade 09:55→11:00 +0.4 / −1.7; 25th/month-end only +1.6 / −0.9;
non-gotobi control −1.6 / −2.2. Gotobi beat non-gotobi days by +2.4bp gross (t 4.0) in 2001-14 and +1.1bp (t 1.6) in
2015-22, positive in 19/22 years — a real, well-known flow, but it halved after 2014 and the net edge is ~0 after costs.
Weak — documented, not tradeable at retail costs. Registry: 470.

## 331 — Treasury auction dealer takedown (agent-run; results/agent_331.md)  ·  2026-11-21
Dealer share (primary-dealer / competitive accepted) vs the tenor's last 6 auctions; 10y→IEF, 30y→TLT; entry at the
auction-day close, 1bp/side; ~70 events per period. A ±5pp, hold d+1..3 +0.2 / −1.1; B hold 5d −0.0 / −1.8;
C bid-to-cover ±0.2 +0.9 / +0.7; D one-day-later entry +0.4 / −1.2. Dealer share wrong-signed in valid. Dead.

## 330 — London 4pm WM/R fix pre-hedge reversal (agent-run; results/agent_330.md)  ·  2026-11-21
USDJPY+AUDUSD pooled, 0.5bp/side: fade the 15:30→16:00 London run over 16:00→16:30, non-month-end −3.5 / −2.4;
|run|>1σ −1.0 / +0.5; month-end only +2.0 / +0.9; 15-min version −3.4 / −1.2. Gross reversal +0.4-0.8bp/day, below
the 1bp round trip. Dead. **The agent discovered a clock bug in the HistData pipeline (next entry).**

## CORRECTION — HistData 1-minute timestamps are New York LOCAL time, not fixed EST  ·  2026-11-21
Verified independently: the Sunday FX open is at 17:00 in both January and July, and the US 08:30/10:00 data-release
volatility spikes sit at the same clock time in winter and summer (SPXUSD and USDJPY, 2012/2019/2022). The old
`ny_minute_close` converted EST→New York, shifting every DST-season bar +1h, so from March to November a "09:30" query
returned 08:30 data (and "16:00" returned 15:00). Fixed in experiments/003_overnight_fx.py and tools/histdata.py.
(Agent 330 also reports FX ~2019+ may be London−5h, which differs from NY local only ~3 weeks a year.)
Affected: 003, 005, 197, 287-297, 309, 311, 322 (329/330 handled time themselves). Daily-data results — including
013 and the bond bid — are unaffected. Corrected reruns (registry rows tagged CLOCK-FIX; old rows SUPERSEDED):
- 309 month-end delayed entry (SPX CFD): 09:30 +10.4bp (t 1.6) | 09:35 +8.9 | 09:45 +10.1 | 10:00 +10.1 (NSX +12.4 …
  +11.8). Still not an opening-print artifact, but the CFD-measured month-end effect is smaller than reported (+10 vs +17bp).
- 287/295 intraday profile: S&P 09:30-12:00 +4.1 (t 1.1), 12:00-15:00 +6.4 (1.8), 15:00-15:50 −1.4, 15:50-16:00 +1.4;
  NSX similar. **The "front-loaded into the morning" conclusion is withdrawn** — the move is spread over the session.
- 293 morning-only 2.7bp vs full session 9.4bp → hold the full session (conclusion unchanged).
- 297 OPEX mid-session short: +1.2 / +0.7 (was +2.4 / +1.5); full-session CFD baseline +1.9 / +1.6. Weaker.
- 311 weekly-expiry Fridays: still dead. 322 strike pinning: −/+ ~0 (no longer wrong-signed) — dead.
- 288 sign(prev close→10:00) on 15:30→16:00: +0.3 / −6.0; 289 +0.5 / −1.9; **290 fade 09:30→15:30 on the last half hour:
  −3.9 / −2.3, i.e. intraday MOMENTUM (continuation into the close) is strong in both periods on the correct clock.**
  Followed up in 334. 291 FOMC fade ~0; 292 OPEX segments now front-loaded in the first hour (+8.5bp, t 2.4).

## 334 — Intraday momentum into the close, clock-corrected (single pre-set rule)  ·  2026-11-21
Follow sign(09:30→15:30) over 15:30→16:00 on the SPX CFD, direction taken from 2010-14: train +1.52 / valid −1.04
(gross +2.2bp/day 2010-14, +0.4bp 2015-22; net of the 1bp round trip ~0 or negative). Fails the gate; no holdout look.
Note: the large negative t of the clock-fixed 290 'fade' was mostly the cost drag on a ~zero gross edge — neither
momentum nor reversal survives costs. Dead. Registry: 494.

## 339 — Broker-dealer capital shocks → credit/equity premia (He-Kelly-Manela; agent-run, results/agent_339.md)  ·  2026-11-21
Shock month = dealer basket (GS/MS/JPM/C/BAC) minus SPY in the bottom quintile (cut from 2000-14); 36 train / 20 valid
months; lag-1, credit duration-hedged with IEF. HYG−0.5 IEF +0.0 / +0.4; LQD−1.1 IEF −0.8 / +0.1; long SPY −0.1 / +0.8;
size-scaled HYG +/− ~0. 2008-09 shocks were followed by further losses. Dead.

## 338 — Risk-parity deleveraging when the stock-bond correlation flips positive (agent-run; results/agent_338.md)  ·  2026-11-21
Trigger: 21d SPY-TLT correlation crosses above +0.2 after being <0 within 63 days (12 train / 11 valid events); hold
10d, lag-1. Short 50/50 SPY+TLT −0.9 / −0.1; short TLT −0.7 / −0.9; high-vol only −0.8 / −0.1; placebo −0.3 / +0.4.
Wrong-signed and underpowered. Dead.

## 336 — Volatility day/night pricing in VIXY (Muravyev-Ni; agent-run, results/agent_336.md)  ·  2026-11-21
VIXY 2011-22, 2bp/side per leg, 8 rows (raw + timing value). Short overnight only +1.0 / −0.1 raw (timing −1.4 / −1.9);
long intraday −1.4 / −1.9; both legs −0.6 / −1.5; overnight short in contango only +0.0 / +0.2. VIXY decays as much
intraday as overnight, so the day/night split doesn't translate to the ETP. Dead.

## 335 — European-open overnight drift, 02:00-03:00 ET (Boyarchenko-Larsen-Whelan; agent-run, results/agent_335.md)  ·  2026-11-21
SPX CFD (clock-corrected), 0.5bp/side: long every day −0.0 / −1.2; after a negative 15:00-16:00 hour +1.0 / +0.3;
−sign(last hour) −0.4 / −1.5; NSX after a weak last hour +0.6 / +0.5. The drift exists (02-03h ≈ +0.9 / +0.6bp vs
~0.1bp other hours) but can't pay a 1bp round trip. Dead.

## 337 — TIPS seasonal inflation accrual (Gapen 2003; agent-run, results/agent_337.md)  ·  2026-11-21
Known excess accrual NSA CPI(M-2)/CPI(M-3) minus its 12m trend (only CPI released by the decision date); monthly,
1bp/side; spread TIP − β·IEF. A sign(excess accrual) +2.25 / +1.10; B sized +1.6 / +0.4; C VTIP − β·SHY +1.7 / +0.4;
D IEF-only control −0.7 / −1.0. Spread-on-accrual slope 0.33 / 0.21 (not significant): the market prices most of the
known carry. Weak (A train leans on 2008-09); fails gate.

## Agent round summary (321-339)  ·  2026-11-21
Two idea scouts (results/ideas_behaviour.md, results/ideas_papers.md) + 14 tester agents. Nothing passed the gate.
Real-but-sub-cost flows: Gotobi fix (329), WM/R fix reversal (330), European-open drift (335). Weak: TIPS seasonal (337),
vol-spike re-leveraging (325). Bonus: agent 330 found the HistData clock bug (corrected above). Registry: 518.

## 341 — Foreign-official custody drawdowns → Treasuries (agent-run; results/agent_341.md)  ·  2026-11-21
FRED WMTSECL1 4-week % change (from 2007), known Friday, lag-1, 1bp/side, thresholds from train. Drop <10th pct → short
TLT 20d −0.4 / +0.7; with UUP rising −0.5 / +1.4; short IEF 10d −0.1 / +0.8; rise >90th → long IEF +0.1 / no events.
Signs flip between periods (valid gains = 2022); holdings trend down after 2014. Dead.

## 342 — Mutual-fund Oct-31 year-end tax-loss selling / November rebound (agent-run; results/agent_342.md)  ·  2026-11-21
Sector SPDRs + IWM + 20 country ETFs; long YTD bottom fifth / short top fifth (rank at Oct 20), lag-1, 2bp/side.
Oct-end→Nov-end −1.0 / +1.8; selling leg Oct 20→31 −0.8 / +0.6; first 10 Nov sessions −1.1 / +2.2; June placebo +0.2 / −0.3.
Wrong sign in train (losers lagged in 11/15 Novembers); valid gain is 2020+2022. Dead.

## 344 — Same-half-hour return persistence (Heston-Korajczyk-Sadka; agent-run, results/agent_344.md)  ·  2026-11-21
SPX/NSX CFD 2010-22, trade a half-hour when the same slot's lag-1 and lag-5 returns agree, 0.5bp/side per trade.
SPX all 13 slots −3.2 / −4.9; 3 best train slots +1.6 / −0.3; NSX all −2.1 / −4.5; 3 best +1.1 / −0.4. Gross ≈ 0 for
all slots in valid; the chosen midday slots keep ~0.85bp gross, below the 1bp round trip. Dead.

## 343 — Closing-auction price pressure reversal (Bogousslavsky-Muravyev; agent-run, results/agent_343.md)  ·  2026-11-21
Fade the 15:50→16:00 move from the 16:01 bar, 0.5bp/side: SPX to 16:14 −6.3 / −4.0; SPX to next 09:29 −0.6 / +0.2;
big moves only −3.4 / −2.2; NSX to 16:14 −2.3 / −2.7. No index-level post-close reversal (train even shows continuation;
valid gross ≈ 0, so net = cost). Implementable via ES/NQ, but no edge. Dead.

## 340 — Primary-dealer Treasury inventory build-ups → IEF/TLT (agent-run; results/agent_340.md)  ·  2026-11-21
NY Fed PD statistics (weekly net positions ex-TIPS, series breaks stitched, 2001+), released Thursday W+8, lag-1, 1bp/side.
4-week-change z>+1 → long IEF 4w +2.04 / +1.75 (+62 / +54bp/trade); symmetric +0.4 / +1.6; TLT 1w +1.3 / +1.5; coupons
only +1.8 / +1.7. Right sign in both periods but timing value over exposure-matched holdings is only t 0.2-0.5 / ~1.6 —
mostly duration premium; train leans on 2008. Weak; fails gate.

## 345 — Commercial-bank securities buying (Fed H.8) → IEF/MBB (agent-run; results/agent_345.md)  ·  2026-11-21
FRED revised H.8 data (first-release vintages need an ALFRED API key — results are, if anything, flattered), entered the
Monday after the Friday release, ~1 week. IEF 1w change +0.5 / +0.1; MBB on MBS change −1.8 / +0.0; IEF 4w change
+1.7 / +0.6 (2008/2022-driven); SPY control −1.0 / −1.2. Dead.

## 349 — BCOM/GSCI January reweight (agent-run; results/agent_349.md)  ·  2026-11-21
10 commodity ETFs ranked on prior-year return; long bottom 3 / short top 3, close of January BD4 → BD10, lag-1, 2bp/side.
Main −1.6 / −0.1 (−150 / +19bp per event); Feb-Dec roll-window placebo −1.0 / +0.2; BD10→20 reversal +1.8 / +0.5
(2009/2014-driven). January looks like the placebo months. Dead. (6 rows incl. 3 superseded by a short-January fix.)

## 346 — London gold PM fix (agent-run; results/agent_346.md)  ·  2026-11-21
XAUUSD 1-min (clock: NY local ≤2018, London−5h 2019+ — checked on DST-mismatch days), 1bp/side, ~1,500/2,060 events.
Short 14:30→15:00 London +1.5 / −1.9; long 15:05→16:00 +0.4 / −2.0; combined +1.2 / −2.8; NY-clock placebo ~0.
Real before the 2015 LBMA auction reform (pre-fix +3.0bp gross, t 4.6) but ~0.9bp gross since — below costs. Dead.

## 347 — Norges Bank pre-announced NOK purchases (agent-run; results/agent_347.md)  ·  2026-11-21
Monthly announced daily amounts 2000-2026 (mid-month revisions and Decembers excluded), lag-1, 2bp/side.
EURNOK on ≥200m/day change +0.6 / +1.3 (20 valid trades, 2020-driven); USDNOK +0.2 / +1.3; sign of amount ~0;
NORW−EFA −1.0 / +0.1. Dead.

## 348 — Japan MOF weekly cross-border securities flows (agent-run; results/agent_348.md)  ·  2026-11-21
MOF week.csv (revised history, 2005+), entered at the first US close after release, ~5-day hold, |z|>1.5.
USDJPY +0.5 / +0.3; TLT −/+ ~0 / −0.6; EWJ −0.7 / −0.7; DXJ −0.7 / −1.2. Flows persist week to week but prices don't
follow. Dead.

## 353 — SPY ex-dividend day vs IVV/VOO twins (agent-run; results/agent_353.md)  ·  2026-11-21
Raw prices + Yahoo dividends, 0.5bp/side per leg. Long SPY/short IVV on SPY ex-dates −1.3 / +0.8; long IVV/short SPY
on IVV ex-dates +0.4 / −2.3; long VOO/short SPY on VOO ex-dates −1.8 / −1.4; pooled overnight −0.0 / −2.8. Median
ex-day drop = 0.96-1.02× the dividend — fully priced; spreads ±1-4bp gross with flipping sign. Dead.

## 350 — Twin bond-ETF residual reversion (agent-run; results/agent_350.md)  ·  2026-11-21
Fade |z|>2 of the 3-day beta-residual between twins, beta-neutral, 3 sessions, 1bp/side per leg (lag-1; lag-0 best case
also registered, 8 rows). Lag-1: HYG/JNK +1.2 / −0.1; stress-only +1.4 / +0.1; MUB/VTEB valid +0.9 (2020); EMB/PCY
+1.7 / −0.9. Lag-0: HYG/JNK +2.8 / +0.3, EMB/PCY +2.0 / +2.0 — closing-print noise, faded after 2011. Dead.

## 352 — Implied-correlation premium (COR3M; agent-run, results/agent_352.md)  ·  2026-11-21
COR3M from cdn.cboe.com (2006+, back-filled before its 2022 launch; KCJ/ICJ files 403). Month-end decisions, lag-1,
timing value vs exposure-matched long. High COR3M → SPY +1.1 / +2.0 (only 3 valid events, April 2020); high premium
(COR3M − realized sector corr) → SPY +1.4 / +0.4; → PUT index +1.1 / +0.5; 3-month hold +1.2 / +1.5 (1 valid event).
Thresholds rarely fire after 2014. Dead/weak.

## 351 — Nasdaq-100 covered-call (QYLD) roll flow (agent-run; results/agent_351.md)  ·  2026-11-21
Signal: sign of the NDX move since the last roll (strike proxy), traded the session before monthly OPEX.
QQQ−β·SPY open→close −0.5 / +0.8 (+3bp/trade, rising with fund AUM but t<1 per sub-period; ~0 at 1bp/side/leg);
|move|>2% −0.2 / +1.3; placebo a week earlier ~0; NSX CFD 12:00-15:59 directional +1.3 / +2.1 (2019-22 t 3.5, 2022 ≈45%)
— fails train and is likely trend rather than roll flow. Dead/weak; worth re-checking later if QYLD-type AUM keeps growing.

## Agent round 3 summary (340-353)  ·  2026-11-21
14 more agent tests from 3 more idea scouts (cross-asset, options/ETF, papers). Nothing passed the gate. Real but sub-cost
or decayed: gold PM fix pre-2015 (346). Weak: dealer inventory (340, duration premium), covered-call roll (351).
Registry: 580. The two findings (013 month-end rebalancing; month-end bond bid) remain the only robust results.

## 354 — VVIX and tail-hedge returns (agent-run; results/agent_354.md)  ·  2026-11-21
Timing value vs always-on hedge legs, lag-1, cut-offs from 2006-14. Short VIXY 5d after high VVIX/60d +0.7 / −0.2;
short PPUT−SPXTR in top VVIX tercile −0.2 / −0.1; same on VVIX/VIX −/+ 0.1 / −1.9; short VXTH−SPXTR −0.4 / −0.8.
No monotonic pattern; VVIX's post-2014 level shift leaves B/D on ~96% of valid days. Dead.

## 357 — Dealer gamma regime from distance to the last OPEX (agent-run; results/agent_357.md)  ·  2026-11-21
SPX CFD (clock-corrected), 15:30→16:00, 0.5bp/side. Momentum when SPX >2% below last OPEX close +1.7 / −1.1 net;
reversal when >2% above −1.0 / −1.7; combined +1.0 / −1.9; momentum when >4% below +0.7 / −1.0. Gross also flips sign
between periods (A positive 2011-16, negative 2017-22). Dead.

## 356 — Country-ETF holiday-closure fade (agent-run; results/agent_356.md)  ·  2026-11-21
FXI/EWH/ASHR/EWT/EWY/EWJ/EWZ; closure = 2-8 US sessions without a home-index bar; fade ETF−β·SPY from the last
closure close to the close after reopening; 229 windows 2002-22. lag-1 |X|>1% +1.0 / +0.4; Lunar New Year only
+0.1 / −0.1; |X|>2% +1.2 / −0.8; same-close best case +0.7 / +1.6. Partial reversion, too noisy. Dead.

## 355 — Event-vol premium around FOMC and CPI (agent-run; results/agent_355.md)  ·  2026-11-21
CPI release dates from ALFRED CPIAUCSL vintages (data/cpi_release_dates.csv). Hold close(t−1)→close(t), lag-1, 2bp/side;
timing value vs an always-on position. Short VIXY on FOMC day +1.4 / +1.7 (+96bp/event valid, right sign 10/12 years,
only 32 train events); short VIXY on CPI day −0.5 / +0.6; WPUT in FOMC weeks +0.6 / −1.7; WPUT in CPI weeks ~0.
Dead except V1 = weak-but-consistent (fails train t on few events). Follow-up: longer train sample via VIX futures index.

## 355b — FOMC-day vol crush in spot VIX, 2000-22 (descriptive, supports 355 V1)  ·  2026-11-21
No free VIX-futures series before 2009 (Yahoo lacks SPVXSTR), so spot VIX (not tradable) as a check: log change on FOMC
days 2000-07 −2.2% (t −2.7, falls 71%), 2008-14 −2.9% (t −2.7, 63%), 2015-22 −1.6% (t −1.4, 58%) vs ~0 on all days.
The event-vol premium into FOMC is a long-standing pattern, weaker recently. 355 V1 stays weak-consistent (VIXY history
too short to pass the train gate). Not registered.

## 358 — SNB sight deposits as an intervention gauge → EURCHF (agent-run; results/agent_358.md)  ·  2026-11-21
SNB data portal (cube snbgwdchfsgw, domestic banks' sight deposits, weekly 2009+), released Monday, entered Tuesday close
(lag-1), 5-day hold, 2bp/side, 2011-15 floor excluded (short train). ΔGI>+2bn long EURCHF −0.5 / +0.3; ΔGI<−2bn short
+0.6 / +0.0; z-score signal −0.2 / −0.4; USDCHF +/− ~0. Dead.

## 360 — Pension funded-status LDI flattener (agent-run; results/agent_360.md)  ·  2026-11-21
Month-end FS proxy = 3m SPY − 3m TLT; position TLT − 1.9×IEF next month, lag-1, 1bp/side (raw + timing value, 8 rows).
Top-tercile FS −0.3 / −0.6; FRED 20s−10s proxy (1994+) +2.0 / +0.1 (fading 1994-99 → valid); Oct-Mar only +0.1 / −0.7;
continuous z +0.4 / +0.4. Dead.
