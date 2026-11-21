# 330 — Daily London 4pm WM/R fix: fade the pre-fix run (idea B2)

Script: `experiments/330_wmr_fix_reversal.py` · registry: `results/registry_agent_330.csv` (4 tests) · table: `results/330_wmr_fix_reversal.csv`
Data: HistData 1-min USDJPY + AUDUSD, 2001–2022 only (no 2023+ loaded). Pooled equal-weight across the two pairs. EURUSD not downloaded (rate limits).
Cost 0.5bp/side (1bp round trip). Train 2001–14, valid 2015–22. Price at hh:mm = close of the bar stamped hh:mm−1.

## Clock finding (important for other HistData FX work)
The FX files are **not fixed EST**. Evidence: the Sunday-open/Friday-close bars and the 08:30 NFP spike sit at 08:30 in the file in both summer and winter.
- 2001–2018: file clock = **America/New_York local time** (US DST).
- 2019–2022: file clock = **UTC−5 plus EU summer time** (= London − 5h). In the US/UK DST-mismatch weeks the Sunday open is at 16:00 and the Nov NFP spike is at 07:30.
- Check: after conversion, the mean |1-min return| spikes at the bar ending 16:00 London (USDJPY 2.27bp vs ~1.5bp nearby; AUDUSD 3.02 vs ~2.0). The spike is in the same place in the mismatch weeks (2.51 / 3.54). So the mapping is right.
- `tools/histdata.py` docstring and `003`'s `ny_minute_close` assume fixed EST. For FX pairs this puts summer bars 1h off in 2001–18 (index CFD files not checked here).

## Variants (pre-specified)
| variant | events tr/va | train t | valid t | valid Sharpe | gross bp tr/va | net bp/trade tr/va | valid net @1bp/side | yrs + | gate |
|---|---|---|---|---|---|---|---|---|---|
| a) −sign(R 15:30→16:00), hold 16:00→16:30, non-month-end | 3318/1976 | −3.45 | −2.37 | −0.83 | +0.41/+0.60 | −0.59/−0.40 | −1.40 | 2/22 | no |
| b) as (a), only when \|R\| > 1σ (60d) | 1404/881 | −1.03 | +0.47 | +0.16 | +0.63/+1.17 | −0.37/+0.17 | −0.83 | 11/22 | no |
| c) month-end days only | 170/97 | +1.97 | +0.89 | +0.31 | +2.56/+1.90 | +1.56/+0.90 | −0.10 | 14/22 | no |
| d) R 15:45→16:00, hold 16:00→16:15, non-month-end | 3312/1976 | −3.44 | −1.23 | −0.43 | +0.56/+0.84 | −0.44/−0.16 | −1.16 | 5/22 | no |

Events are pooled trading days. A day counts once even when both pairs trade.

## Verdict: DEAD (no variant passes `lib.passes_gate`)
- The reversal exists but is small. Gross, the fade earns +0.4 to +0.8bp per day on ordinary days, in both periods. That is below the 1bp round-trip cost, so net t-stats are significantly negative.
- The month-end version (c) is the strongest: about +2–2.6bp gross, which fits Evans's finding that month-end fixes are larger. It still fails: train t is 1.97, valid t is 0.89, and it nets about 0 at 1bp/side.
- With 4 tests the luck threshold is about 1.7, and c only just clears it in train.
- Possible follow-up: test EURUSD/GBPUSD month-ends only, where spreads are tighter and fix volume is bigger. Expect little.
