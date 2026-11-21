# 363 — Bond-ETF month-start ex-date (card S4) + bond-bid adjusted-close audit

Script: `experiments/363_bond_etf_exdiv.py`. Registry: `results/registry_agent_363.csv` (4 rows).
Outputs: `results/363_events.csv`, `results/363_summary.csv`, `results/363_bondbid_audit.csv`.
Data: Yahoo RAW closes + actual dividends (chart API events=div); total return r=(C_t+D_t)/C_{t-1}-1.
Rate residual vs IEF, 60-day beta fixed at the decision close. Ex-dates pre-announced; lag=1 implementation.
1bp/side on every leg (incl. IEF hedge). 2002-08..2022 only; no 2023+ data looked at.

## Variants (pre-specified, net, event P&L booked on ex-date)
| variant | train t | valid t | valid Sharpe | valid net bp/trade | n tr/va | gate |
|---|---|---|---|---|---|---|
| A long basket (HYG,LQD,EMB,AGG,TLT) − βIEF, ex-day | −0.42 | −0.38 | −0.13 | −1.05 | 147/96 | no |
| B short basket + βIEF, cum-day | −4.73 | −1.80 | −0.64 | −3.74 | 147/96 | no |
| C long HYG − βIEF, ex-1→ex+1 | +1.73 | +1.20 | +0.42 | +7.46 | 90/96 | no |
| D long HYG+EMB − βIEF, ex-1→ex+1 | +1.99 | +1.70 | +0.60 | +9.78 | 90/96 | no |

Exposure-matched baseline: the IEF-hedged basket residual averages +1.2bp/day on all days (credit carry/beta drift),
so ~2.4bp of C/D's 2-day gross is unconditional. None passes the gate.

## Descriptive (c), 2002-22, all ex-dates
| ETF | n | div bp | median drop/D | median rate-adj drop/D | ex resid bp (t) | cum resid bp (t) |
|---|---|---|---|---|---|---|
| HYG | 186 | 52.6 | 0.84 | 0.83 | +7.3 (1.5) | +9.3 (2.4) |
| LQD | 242 | 35.0 | 0.97 | 0.94 | +3.5 (1.6) | +8.0 (1.9) |
| EMB | 178 | 40.0 | 0.88 | 0.80 | +4.9 (0.9) | +10.7 (2.2) |
| AGG | 230 | 26.0 | 1.11 | 0.95 | −0.5 (−0.4) | +2.4 (2.3) |
| TLT | 241 | 27.7 | 1.10 | 1.01 | +0.1 (0.1) | −0.5 (−0.2) |
| IEF | 243 | 23.5 | 1.12 | – | – | – |
Credit ETFs drop ~80-95% of the distribution (Elton-Gruber direction), Treasuries ~100%. Pooled ex-day residual
+3.0bp (t 1.4) train / +2.5bp (t 1.4) valid — smaller than the ~3-4bp round-trip cost of a hedged trade.
The cum-day residual is POSITIVE (opposite to capture-selling): train +9.9bp t=4.1, valid +0.2bp t=0.1. The cum day is
the last session of the month, so this is the credit month-end bid (277/278) not fully hedged by IEF, not tax selling.

## Bond-bid audit (LOG 275/276, last-3-session window, lib.get 'aC')
- Regular IEF/TLT ex-dates are the first session of the month, outside the window. But 8 ex-dates per fund DO fall
  inside it: the December (year-end) distributions 2002-2009 (Dec 2002..Dec 2009).
- Yahoo adjusted window returns match raw-price-plus-dividend returns: mean |diff| 0.006bp (IEF) / 0.009bp (TLT),
  max 0.26 / 0.45bp per month; per-ex-day max 1.3bp. Window mean IEF +23.17bp (t 5.37) both ways; TLT +39.59bp
  (t 4.65) both ways; train/valid t unchanged to two decimals (IEF 3.95/3.69, TLT 3.67/2.84).
- A price-only version would understate the window by ~1.1-1.3bp/month (the 8 December dividends), so price-only
  work would be slightly conservative. **The bond-bid estimates are NOT affected.**
- 281's early-month reversal leg (first 3 sessions incl. the ex-day) used lib.get 'cc' (adjusted), which handles the
  ex-day correctly per the per-ex-day check above — no contamination.

## Verdict
Dead as a trade (A/B fail; C/D best t≈2.0/1.7, below gate and below the luck threshold). Descriptive: a modest
Elton-Gruber-type undershoot in HY/EM ex-day drops (~0.8-0.9×D), not tradable after hedge costs.
