# 336 — Vol day/night split in VIXY (idea card P4, Muravyev & Ni 2020)

Script: `experiments/336_vol_day_night.py` · Registry: `results/registry_agent_336.csv` (8 rows = 4 variants x {raw, timing value})
Data: Yahoo VIXY 2011-01 → 2022-12 (adjusted overnight `gap`, intraday `oc`). TRAIN 2011-14, VALID 2015-22. No 2023+ looked at.
Cost 2bp/side on every leg (C pays 4 sides/day). Timing value (TV) = net P&L − period-mean signed position × VIXY close→close
(exposure-matched always-short, uncosted benchmark), the 082 convention.

| Variant (pre-specified) | metric | train t | valid t | train bp/day | valid bp/day | valid Sharpe | gate |
|---|---|---|---|---|---|---|---|
| A short night (close→open) | raw | +0.97 | −0.14 | +7.3 | −0.9 | −0.05 | no |
| A | TV | −1.43 | −1.93 | −14.3 | −15.5 | −0.68 | no |
| B long day (open→close) | raw | −1.42 | −1.93 | −14.2 | −15.3 | −0.68 | no |
| B | TV | +0.99 | −0.10 | +7.4 | −0.6 | −0.03 | no |
| C short night + long day | raw = TV | −0.56 | −1.54 | −7.0 | −16.2 | −0.55 | no |
| D short night if VIX9D/VIX<1 (lag-1, 72% of nights) | raw | +0.02 | +0.20 | +0.1 | +0.8 | +0.07 | no |
| D | TV | −1.82 | −1.43 | −13.9 | −10.2 | −0.51 | no |

Gross VIXY drift (bp/day): night −11.3 train / −3.1 valid; day −10.2 / −11.3; close→close −21.6 / −14.6.

**Finding:** the predicted asymmetry is absent in VIX-futures ETPs. VIXY's roll/vol-premium decay accrues about equally
overnight and intraday (and mostly *intraday* in 2015-22), so short-night-only captures less than half the short-vol carry
(TV vs always-short ≈ −15bp/day) and long-day loses ~15bp/day. The option-level day/night effect (dealer pricing of overnight
variance in SPX options) does not transmit to 1-month VIX futures, whose price tracks forward implied vol, not realized-vs-
implied gamma P&L. Yearly P&L is unstable in sign for every variant. N=8 registered, luck t ≈ 2.0; nothing comes close.

**Verdict: dead.** No variant passes `lib.passes_gate`; no holdout look spent. (Note: the full-sample 2011-22 night/day
mean returns were printed once as a data sanity check before the run; variants were fixed by the idea card beforehand.)
