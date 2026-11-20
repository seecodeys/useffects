"""250 - Quarterly / annual rebalancers react to the QUARTER-to-date (or year-to-date) stock-bond gap, not month-to-date.
  a) last trading day of each quarter: pos = −sign(SPY QTD − TLT QTD), SPY open->close
  b) last trading day of the year: pos = −sign(SPY YTD − TLT YTD)
QTD/YTD through the prior close. 0.5bp/side."""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
spy, tlt = lib.get("SPY"), lib.get("TLT")
idx = spy.index.intersection(tlt.index); idx = idx[idx >= "2002-08-01"]
s, t = spy.loc[idx], tlt.loc[idx]
def todate(x, per):
    p = pd.Series(idx.to_period(per), index=idx)
    lastp = p != p.shift(-1)
    base = x["aC"].where(lastp).shift(1).ffill()       # previous period's last close
    base = base.where(p != p.shift(1)).ffill()          # constant within period
    return x["aC"].shift(1) / base - 1, lastp & (p != p.iloc[-1])
for tag, per in [("a quarter-end, QTD gap", "Q"), ("b year-end, YTD gap", "Y")]:
    gs, lastp = todate(s, per); gt, _ = todate(t, per)
    pos = (-np.sign(gs - gt)).where(lastp, 0).fillna(0)
    r = lib.register("013_month_end_rebalance", f"250{tag}", pos, s.oc, 0.5)
    tt = (pos * s.oc)[pos != 0] * 1e4
    print(f"250{tag:<24} {lib.fmt(r)}  per-trade train {tt.loc[:'2014'].mean():+.1f} valid {tt.loc['2015':'2022'].mean():+.1f}  n={len(tt.loc[:'2022'])}")
print("registry size:", lib.n_tests())
