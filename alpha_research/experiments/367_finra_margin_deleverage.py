"""367 - FINRA margin statistics: leverage / forced-deleveraging flow (data source 2, results/data_sources_new.md).
Data: https://www.finra.org/sites/default/files/2021-03/margin-statistics.xlsx (cached data/finra/, not committed).
Monthly 1997-01+: margin debit balances (D); free credits in cash accts (pre-2010-02 = cash+margin combined) and
free credits in margin accts (2010-02+). Total free credit F = cash + margin (NaN->0), which splices the 2010 break.
Publication lag: FINRA (NYSE pre-2010) releases month m around the 3rd week of m+1. Used lag: month-m data is first
used at the CLOSE OF THE LAST TRADING DAY OF m+1 (>= ~1 week after the usual release), and backtest_weights lag=1
-> position from close of 1st trading day of m+2, held one month (until next monthly decision). Monthly rebalancing.
Market level for ratios = SPY adjusted close at end of month m (known before the decision).
Instrument: SPY, 0.5bp/side on turnover. Registered as pos = |weight|, ret = net/gross (cost inside, cost_bp=0).
Expanding percentiles use only months known at the decision, min 36 non-NaN months of history.
Registry: run 1 had a NaN bug in exp_pct (C never traded); fixed and re-run, both runs registered (N=8).

Pre-specified variants (fixed before results; no tuning after):
  A (H2a deleveraging regime) 12m % change in D < 0 while the max 12m % change over the prior 12 months was > +20%
    -> short SPY (-1) next month; else flat. Margin calls / deleveraging = mechanical seller.
  B (sharp unwind) 3m % change in D in the bottom decile of its expanding history -> short SPY next month
    (forced-selling continuation).
  C (leverage level) log(D / SPY price) minus its trailing 60m mean (debt relative to market value proxy),
    top quintile of expanding history -> short SPY next month (overlevered market, fragile).
  D (H2b dry powder) F / D in top decile of expanding history -> long SPY next month (sideline cash redeployed).
Timing value = net - W * period-mean daily SPY return (exposure-matched static holding removed).
Lib TRAIN is 2000-2014 (registry); 1997-99 is burn-in (signals need 24-36m history). Nothing from 2023+ printed.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

EXP = "367_finra_margin_deleverage"; C = 0.5
tt = lambda x: x.mean() / x.std() * np.sqrt(len(x)) if len(x) > 1 and x.std() > 0 else np.nan

raw = pd.read_excel(os.path.join(lib.DATA, "finra", "margin-statistics.xlsx"))
raw.columns = ["ym", "D", "Fc", "Fm"]
m = raw.assign(ym=pd.PeriodIndex(raw.ym, freq="M")).set_index("ym").sort_index()
m["F"] = m.Fc + m.Fm.fillna(0)

spy = lib.get("SPY")
cal = spy.index
idx = cal[cal <= "2022-12-31"]
me = spy.aC.groupby(cal.to_period("M")).last()                    # SPY month-end adj close
m["P"] = me.reindex(m.index)

def exp_pct(s, minp=36):
    """percentile of s[t] within s[..t] (expanding, own history only)"""
    s2 = s.dropna()      # bug fix after run 1: leading NaNs were counted in the denominator (367C never fired)
    return s2.expanding(minp).apply(lambda x: (x[:-1] < x[-1]).mean() if len(x) > 1 else np.nan, raw=True).reindex(s.index)

g12 = m.D.pct_change(12)
sig = pd.DataFrame(index=m.index)
sig["A"] = np.where((g12 < 0) & (g12.shift(1).rolling(12).max() > 0.20), -1.0, 0.0)
sig["B"] = np.where(exp_pct(m.D.pct_change(3)) <= 0.10, -1.0, 0.0)
lev = np.log(m.D / m.P); lev = lev - lev.rolling(60).mean()
sig["C"] = np.where(exp_pct(lev) >= 0.80, -1.0, 0.0)
sig["D"] = np.where(exp_pct(m.F / m.D) >= 0.90, 1.0, 0.0)

# decision date for data month p = last trading day of month p+1
dec = pd.Series(cal, index=cal).groupby(cal.to_period("M")).last()
R = pd.DataFrame({"SPY": spy.cc.reindex(idx)})
DESC = {"A": "short SPY in deleveraging regime (12m chg<0 after >+20%)", "B": "short SPY when 3m margin chg bottom decile",
        "C": "short SPY when debt/price (60m-demeaned) top quintile", "D": "long SPY when freecredit/debt top decile"}
rows = []
for k in "ABCD":
    w = pd.Series(np.nan, index=cal)
    for p, v in sig[k].items():
        if p + 1 in dec.index:
            w.loc[dec[p + 1]] = v
    w = w.ffill().fillna(0.0).reindex(idx)
    W = pd.DataFrame({"SPY": w})
    net = lib.backtest_weights(W, R, C, lag=1).loc[:"2022"]
    P = W.shift(2).fillna(0.0)                                     # held weight (lag 1 + 1)
    gr = P.abs().sum(axis=1)
    ret = (net / gr).where(gr > 0, 0.0)
    r = lib.register(EXP, f"367{k} {DESC[k]} lag1", gr, ret, 0.0,
                     notes="run2 (exp_pct NaN fix; supersedes run1 rows); FINRA month m used at close of last trading day of m+1; lag1; 0.5bp/side; ret=net/gross")
    print(f"\n{r['variant']}\n  {lib.fmt(r)}  gate={lib.passes_gate(r)}")
    on = gr > 0
    for nm, (a, b) in [("train97", ("1997-01-01", "2014-12-31"))] + list(lib.PERIODS.items()):
        Pa, Ra, na = P.loc[a:b, "SPY"], R.loc[a:b, "SPY"], net.loc[a:b]
        tv = na - Pa * Ra.mean()
        mo = (Pa != 0).groupby(Pa.index.to_period("M")).any()
        mnet = na.groupby(na.index.to_period("M")).sum()[mo]
        print(f"  {nm}: signal months {int(mo.sum())}  net bp/month {1e4*mnet.mean() if len(mnet) else np.nan:+.1f}  "
              f"monthly t {tt(mnet):+.2f}  timing-value daily t {tt(tv[on.loc[a:b]]):+.2f}  "
              f"hit {100*(mnet>0).mean() if len(mnet) else np.nan:.0f}%")
    yr = net[on].groupby(net[on].index.year).sum() * 1e4
    print("  per-year net bp:", " ".join(f"{y}:{v:+.0f}" for y, v in yr.items()))
    rows.append({"variant": k, **{x: r[x] for x in ["train_t", "train_sharpe", "valid_t", "valid_sharpe", "valid_bp_per_trade", "valid_traded_%"]},
                 "gate": lib.passes_gate(r)})
pd.DataFrame(rows).to_csv(os.path.join(lib.HERE, "results", "367_summary.csv"), index=False)
print("\nregistry size (this agent):", lib.n_tests())
