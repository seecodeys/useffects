# 369 — Cboe VX futures open interest -> VIXY / SPY  (data source 6, results/data_sources_new.md, H6a/H6b)

Script: `experiments/369_vx_open_interest.py` · output: `results/369_output.txt` · registry: `results/registry_agent_369.csv` (4 rows).
Data: 209 monthly VX contract files from cdn.cboe.com (archive path for 2005-12 expiries, `VX_{expiry}.csv` for 2013+),
cached in `data/cfe_vx/` (3 early-2005 archive files 403; irrelevant, series used from 2006). Weeklies not included.
Timing: OI for day t is published next morning -> feature indexed at t, position decided at close t+1
(`lib.backtest_weights(..., lag=1)`). Costs 2bp/side VIXY, 0.5bp/side SPY. Registered series = timing value vs
exposure-matched always-on leg (per period, as 354). Not a duplicate of 082 (VIXY contango/overshoot, no OI),
154 (VIXY big-day fade), 301 (weekly CFTC trader categories), 354 (VVIX), 032 (VIX-expiry Wednesday).

Signal z = 20-session log change of total VX OI, standardized over a trailing 252-session window; contango = front settle < second settle.

| Variant | Episodes train / valid | Train t | Valid t | Valid Sharpe | Net bp/round-trip train / valid | Timing value bp/yr train / valid | Gate |
|---|---|---|---|---|---|---|---|
| A long VIXY 5d after z>2 in contango (TRAIN 2011-14) | 5 / 14 | −1.65 | −0.52 | −0.18 | −3030 / −394 | −1200 / −244 | no |
| B short SPY 5d after z>2 in contango | 9 / 14 | −1.63 | +0.03 | +0.01 | −370 / −52 | −134 / +3 | no |
| C long VIXY in final week when front OI share > train p67 (slow roll) | 7 (2011-14) / 0 | +1.12 | n/a | n/a | +413 / — | +914 / 0 | no |
| D long SPY 10d after z>2 in backwardation | 1 / 0 | −0.43 | n/a | n/a | −189 / — | −27 / 0 | no |

Descriptive (≤2022, lag-1 forward 5-session return): after z>2 in contango VIXY fell −559bp (train, n=43 days) and
−46bp (valid, n=65) vs −102 / −69bp unconditional-contango; SPY +77 / +2bp. So OI surges, if anything, precede
*more* VIXY decay (opposite of H6a), but not reliably out of sample.

Structural caveats: (1) C never fires in VALID — the final-week front OI share fell from ~0.29-0.45 (2006-14 medians by
sessions-to-expiry) to ~0.14-0.34 in 2015-22 as ETP-driven daily rolling took over, so the train-percentile threshold
(dominated by pre-ETP years) is never crossed. That is a flaw in the pre-specified rule; not re-tuned. (2) D has
essentially no events: OI does not surge in backwardation (stress periods see OI fall). Both C and D are untested in
practice rather than refuted.

Verdict: **dead.** No variant passes lib.passes_gate; A/B have the wrong sign in train and ~0 in valid. Luck t for best
of N=4 ≈ 1.7. No holdout look. A possible follow-up (new pre-registration needed) would define C's slow-roll threshold
relative to a trailing window rather than fixed 2006-14 percentiles.
