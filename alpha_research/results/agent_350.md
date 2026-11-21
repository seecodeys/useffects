# 350 — Twin bond-ETF residual reversion (card 1, ideas_options_etf.md)

Script: `experiments/350_twin_bond_etf.py`. Registry: `results/registry_agent_350.csv` (8 rows). Summary: `results/350_summary.csv`.

**Setup.** Yahoo adjusted close→close returns (no issuer NAV data). beta = trailing-250d OLS of r_A on r_B; residual
e = r_A − beta·r_B; z = trailing-250d z-score of the 3-day residual sum, known at close t. |z|>2 → short rich leg, long
cheap leg, beta-neutral, gross 1, hold 3 sessions (new trigger restarts). `lib.backtest_weights(lag=1)`, 1bp/side per
leg (both legs' turnover charged). Two 250d windows mean first triggers are in 2010, so train is effectively 2010-14.
4 pre-specified variants, each also registered at lag=0 as a **BEST-CASE** reference (same closing print as the signal).

| Variant | triggers tr/va | train t | valid t | valid Sharpe | valid bp/unit-day | net bp/trigger tr / va | gate |
|---|---|---|---|---|---|---|---|
| A HYG/JNK lag1 | 51/98 | +1.15 | −0.09 | −0.03 | −0.0 | +2.0 / −0.6 | no |
| B A + HYG 5d dd<−2% lag1 | 10/20 | +1.42 | +0.13 | +0.05 | +0.2 | +9.6 / +0.0 | no |
| C MUB/VTEB lag1 (valid only) | 0/73 | n/a | +0.86 | +0.32 | +1.4 | — / +2.5 | no |
| D EMB/PCY lag1 | 63/101 | +1.73 | −0.86 | −0.31 | −0.8 | +6.6 / −2.4 | no |
| A0 HYG/JNK lag0 BEST-CASE | 51/98 | +2.82 | +0.32 | +0.11 | +0.1 | +6.1 / −0.2 | no |
| B0 stress lag0 BEST-CASE | 10/20 | +3.06 | −0.21 | −0.07 | −0.3 | +25.9 / −1.0 | no |
| C0 MUB/VTEB lag0 BEST-CASE | 0/73 | n/a | +1.80 | +0.66 | +3.0 | — / +6.1 | no |
| D0 EMB/PCY lag0 BEST-CASE | 63/101 | +1.97 | +1.97 | +0.70 | +2.0 | +8.1 / +4.1 | no |

**Yearly (lag1, net bp).** A: 10 +59, 11 +19, 12 +23, 13 +47, 14 −45, 15 +12, 16 −31, 17 +8, 18 −30, 19..21 ≈0, 22 −18.
C: 2020 +233, every other year −28..+11 (one-event result). D: 10 +181, 11 +244, then ≤+10 or negative every year 2012-22.

**Notes.**
- The reversion exists but is mostly a same-close effect: lag0 vs lag1 roughly halves to wipes out the edge in every
  pair, i.e. much of the "dislocation" is closing-print noise between twins that reverts by the next close.
- Pair is beta-neutral, so timing value ≈ net (timing t lag1: A +0.88/−0.69, D +1.62/−1.12, C n/a/+0.73).
  Leg P&Ls are large and offsetting (e.g. D valid EMB +2325 / PCY −2432bp), as expected for a hedged pair.
- Signal decayed after 2011 (post-GFC/euro-crisis era when bond-ETF AP capacity was thinner); the 2020 COVID episode
  helped MUB/VTEB only. Stress filter (B) has 10/20 triggers — too few.
- With N=8 tests here, luck alone gives a best t of ~2.0; no lag-1 row comes close.

**Verdict: dead** (lag-1). No variant passes `lib.passes_gate`; best-case lag-0 also fails (D0 closest: t 1.97/1.97,
Sharpe 0.70, but needs execution on the signal's own closing print). Holdout untouched.
