# 372: month-end bond-index extension bid in hedged and local-market bonds (idea card E1)

Script: `experiments/372_monthend_bond_hedged_local.py`. Registry: `results/registry_agent_372.csv` (4 rows).
Signal: calendar only. Long over the last 3 sessions of each month, using each exchange's own calendar (Yahoo bars
carry local exchange dates, so the ASX and TSE month-ends are local). Entry at the close of the 4th-to-last session,
exit at the month-end close. Close->close adjusted returns, 2bp/side. No holdout look.

| Variant | Train start | Events tr/va | Train t | Valid t | Valid Sharpe | Valid net bp/event | Timing value t (tr/va) | Gate |
|---|---|---|---|---|---|---|---|---|
| V1 BNDX (USD-hedged) | 2013-06 | 19/95 | +1.10 | +1.04 | +0.37 | +6.4 | +0.15 / +0.83 | no |
| V2 IAF.AX (AUD, ASX) | 2012-03 | 34/95 | +2.38 | +1.74 | +0.61 | +10.3 | +1.89 / +1.50 | no |
| V3 2510.T (JPY, TSE) | 2017-12 (no train) | 0/60 | n/a | -1.06 | -0.47 | -2.3 | n/a / -0.83 | no |
| V4 EMLC - 0.98*CEW | 2010-07 | 54/95 | -1.24 | -1.92 | -0.68 | -9.6 | -1.25 / -1.93 | no |

- Pooled Stouffer t across V1-V3: train +2.47 (V1 and V2 only), valid +1.00. Pooled timing-value t: train +1.44,
  valid +0.87.
- Timing value is measured against an exposure-matched always-long holding (window P&L minus the in-window fraction
  times the always-long return). It is positive but insignificant for BNDX and IAF.AX.
- Yearly valid P&L: IAF.AX gives back 433bp in 2021 and is carried by 2015/16/18/22. BNDX is mixed.
- Caveats: Yahoo history is short (IAF.AX from 2012, 2510.T from late 2017, so V3 has no train period). The registry's
  `valid_bp_per_trade` is per day, because the net series is registered as an always-on position; the per-event
  figures above are the ones that matter.

Compared with LOG 277/278/284 (IGLT.L +0.5/+1.0, XBB.TO +1.95/+2.64, EUNH.DE +0.9/+0.8, BWX +1.2/+0.6, EMB
+2.65/+1.38): hedging (BNDX) does not sharpen the effect beyond unhedged BWX. Australia is in line with Canada but
weaker. JGBs show no bid. EM local bonds with the FX basket removed run the wrong way, likely because the EM-equity
CEW hedge is a noisy proxy for FX.

**Verdict: dead or weak.** No variant passes `lib.passes_gate`. The month-end bond bid looks concentrated in
USD/CAD-benchmarked bonds and does not extend in a tradable form to hedged global, Australian, Japanese or EM-local
bonds.
