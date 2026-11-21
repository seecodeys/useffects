# 366 — SPDR primary-market flows (shares outstanding) → SPY / sector SPDRs (data_sources_new.md item 1, H1a/H1b)

Script: `experiments/366_spdr_shares_flow.py`. Registry: `results/registry_agent_366.csv` (4 rows). Summary: `results/366_summary.csv`.

**Data.** State Street public NAV-history XLSX (`ssga.com/.../navhist-us-en-{ticker}.xlsx`, follows a 301 redirect; plain public
download, nothing bypassed). Shares outstanding are daily from 2006-05-31 for SPY and the 9 original Select Sector SPDRs
(XLK XLF XLE XLV XLY XLP XLI XLB XLU). The raw files are cached in `data/ssga/` (git-ignored, State Street content, not committed).
Days with |Δlog shares| > 40% count as zero flow (this removes the Dec-2025 XLK/XLE split jump). Prices: Yahoo adjusted cc returns.

**Timing.** The file fetched on Sat 2026-11-21 ends on Thu 11-19, so it is published with a lag. The value dated t is assumed
known by the close of t+2. Signals are shifted 2 sessions and then run through `lib.backtest_weights(lag=1)`, so a flow dated t
first sets a position at the close of t+3. A descriptive check (≤2022) supports the 1-day booking lag: corr(flow dated t, SPY
return at t−1) = +0.144, versus −0.02 at t and ≈0 at t+1 and t+2. The flow dated t is therefore mostly day t−1's flow chasing day
t−1's return, with no same-day price pressure visible afterwards.

**Variants** (all four pre-specified, all reversal, costs inside; bp = net bp per unit-day of gross exposure):

| Variant | events tr/va | train t | valid t | valid Sharpe | valid bp | net bp/event tr / va | timing-value t tr / va | gate |
|---|---|---|---|---|---|---|---|---|
| A SPY 1d flow ≤q05 / ≥q95 (expanding) → reverse, hold 5d, 0.5bp | 102/22 | +1.65 | −0.02 | −0.01 | −0.3 | +55 / −2 | +1.63 / −0.02 | no |
| B sectors 5d flow, long min / short max, 5d tranches, 2bp | daily | +0.34 | −1.19 | −0.42 | −1.3 | – | +0.35 / −1.18 | no |
| C sectors 20d flow, long bottom 3 / short top 3, 20d tranches, 2bp | daily | +0.25 | +0.09 | +0.03 | +0.1 | – | +0.29 / +0.09 | no |
| D sector 1d flow ≤q02 / ≥q98 → reverse vs SPY, hold 5d | 336/150 | −0.25 | −1.66 | −0.59 | −3.0 | −4 / −24 | −0.35 / −1.78 | no |

**Notes.**
- A's train result comes from 2008 and 2011 (+631 and +518 bp): buying SPY after big redemptions works in crisis
  rebounds. Valid has only 22 events (the expanding thresholds are inflated by 2008) and is flat.
- B and C are noise with large year-to-year swings (B: 2009 +2990, 2021 −3916). Sector flow reversal does not show up
  cross-sectionally.
- D leans the wrong way in valid (t −1.66): idiosyncratic sector flow shocks, if anything, continue mildly against the reversal bet.
- All are well below the luck threshold sqrt(2 ln 632) ≈ 3.6.

**Verdict: DEAD.** No variant passes `lib.passes_gate`. No holdout look was taken.
