# 359 — GLD bullion-tonnage flows → GLD / GDX (card X8, ideas_crossasset.md)

Script: `experiments/359_gld_tonnes_flow.py`. Registry: `results/registry_agent_359.csv` (4 rows). Summary: `results/359_summary.csv`.

**Data.** Real holdings, not a proxy: SPDR Gold Shares public historical archive XLSX (linked from spdrgoldshares.com/usa/gld/,
endpoint `api.spdrgoldshares.com/api/v1/historical-archive?product=gld&exchange=NYSE&lang=en`; plain public download, nothing
bypassed). "Tonnes of Gold", daily from 2004-11-18. Cached in `data/` (git-ignored; the file's disclaimer prohibits
redistribution, so do not commit it). The old `assets/dynamic/GLD/GLD_US_archive_EN.csv` URL now redirects to the bar list PDF.
Prices: Yahoo adjusted GLD, GDX.

**Setup.** s_t = 5-day % change in tonnes. Decile thresholds come from an expanding window (min 500 obs), so the first signal
is 2006-11 and train is effectively 2006-11 to 2014. Tonnes for day t are published after t's close, so all weights go through
`lib.backtest_weights(lag=1)` (position from close t+1). Costs: 1bp/side GLD, 2bp/side GDX, charged on turnover.
Four variants were pre-specified; there is no lag-0 version because the day-t tonnage is not known at t's close.
Registered as pos = gross exposure, ret = net/exposure, so the bp column is net bp per unit-day.
"Events" = signal days (top/bottom-decile days for A–C, |ΔT|>1% days for D).

| Variant | events tr/va (long) | train t | valid t | valid Sharpe | valid bp/unit-day | net bp/event tr / va | timing-value t tr / va | gate |
|---|---|---|---|---|---|---|---|---|
| A GLD continuation 5d | 328/391 (65/155) | −0.35 | −1.09 | −0.38 | −4.8 | −13 / −24 | +0.00 / −0.97 | no |
| B GDX−GLD 5d | 328/391 | −1.81 | −0.49 | −0.17 | −2.3 | −107 / −23 | −2.06 / −0.42 | no |
| C GLD reversal, sessions 6–20 | 328/391 | +1.33 | +0.10 | +0.04 | +0.3 | +102 / +4 | +0.84 / −0.09 | no |
| D GLD 1d, |ΔT|>1%, next session | 211/106 (150/70) | +0.83 | −2.45 | −0.87 | −25.9 | +7 / −27 | +0.55 / −2.61 | no |

Yearly net bp (≤2022): A has a single big positive year (2016 +1276) and losses in most others. C's train is mostly 2008
(+1947). D's 2008 is −1984, and 2015–22 is mostly negative.

**Notes.**
- There is no continuation. Flows do not predict GLD over the next 5 sessions (A) and do not lead miners (B; train is
  negative, driven by 2008).
- C (reversal, sessions 6–20) has the right sign in train (t +1.3), but it is concentrated in 2008 and gone in valid (t +0.1).
- D flips sign: weak continuation in train, then t −2.45 *reversal* in valid. The sign is inconsistent across periods and
  1 of 4 tests, against a luck threshold of sqrt(2 ln 612) ≈ 3.6. Do not promote it.
- Descriptive: corr(5d tonnes change, prior GLD 5d return) is only +0.05, so tonnage flows are mostly not price-chasing at the
  5-day horizon. The AP/bullion leg leaves no tradable multi-day footprint in GLD after publication.

**Verdict: DEAD.** No variant passes `lib.passes_gate`. No holdout look was taken.
