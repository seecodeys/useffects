# 345 - Bank securities buying (Fed H.8) -> next-week IEF / MBB (card P10)

Script: `experiments/345_bank_securities_h8.py`; registry `results/registry_agent_345.csv` (4 variants); summary `results/345_summary.csv`.

**Data / timing.** FRED H.8 weekly SA, all commercial banks: TASACBW027SBOG (Treasury+agency, 1973+), TMBACBW027SBOG (MBS, 2009-07+).
Week ending Wed w is released Fri w+9d ~16:15 ET; the signal is put on the first trading day >= that Friday and traded with
`lib.backtest_weights(lag=1)`, so the position starts at the next close (normally Monday close) and is held ~5 days until the next signal.
Signal z = (log-change - prior-52w mean) / prior-52w std; +1 if z>+1, -1 if z<-1, else flat.
**Limitation:** ALFRED first-release vintages could not be reached without an API key (alfredgraph.csv 404; fredgraph.csv ignores
vintage_date), so this uses **revised** history (seasonal-factor and benchmark revisions leak a little; H.8 levels are not break-adjusted).
That should make the results look better than real time, not worse.

Costs: 1bp/side IEF/MBB, 0.5bp/side SPY. Windows: ETF/signal start to 2022 (train <=2014, valid 2015-22). Nothing from 2023+ was looked at.

| Variant | train t | valid t | valid Sharpe | blocks tr/va | net bp/trade tr / va | timing-value t tr / va | gate |
|---|---|---|---|---|---|---|---|
| A IEF, 1w z(Tsy+agency) | +0.46 | +0.14 | +0.05 | 119 / 100 | +5.1 / +2.5 | +0.46 / +0.16 | no |
| B MBB, 1w z(MBS) (2010-07+) | -1.82 | +0.03 | +0.01 | 42 / 98 | -12.4 / +1.0 | -1.65 / +0.04 | no |
| C IEF, 4w z(Tsy+agency) | +1.74 | +0.62 | +0.22 | 85 / 46 | +31.7 / +13.0 | +1.73 / +0.66 | no |
| D SPY control, A signal | -1.03 | -1.24 | -0.44 | 264* / 100 | -15.4 / -46.6 | -0.93 / -1.14 | no |

"Trade" = one holding block: consecutive weeks with the same sign are merged. *D's train block count includes 1993-99, but the registry t uses 2000-14.
t-stats are on daily net P&L. lib's `valid_bp_per_trade` is per day because pos=1 is passed with net P&L.
C's train result comes from a few years (2002, 2008 +1274bp, 2022 +798bp in valid), and 2007 and 2011-12 lost money.
Luck t for the best of N: about 1.67 at the local N=4, and about 3.5 at the repo-wide N of roughly 450.

**Verdict: DEAD.** No variant passes `lib.passes_gate`. The 1-week bank-flow signal has no next-week predictive power for IEF or MBB,
and the MBS version has the wrong sign in train. The 4-week version is weakly positive but under t=2 in both periods and driven by a few
years, even though revised data favours it. The SPY control is about zero/negative, as expected. A follow-up is worth it only with real ALFRED vintages (needs a FRED API key).
