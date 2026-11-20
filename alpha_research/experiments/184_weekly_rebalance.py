"""184 - Weekly rebalancers: on the last trading day of each week, pos = −sign(SPY WTD − TLT WTD) (week-to-date through the
prior close), SPY open->close, 0.5bp; and the high-vol version (SPY 20d vol > 2002-14 median)."""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
spy, tlt = lib.get("SPY"), lib.get("TLT")
idx = spy.index.intersection(tlt.index); idx = idx[idx >= "2002-08-01"]
s, t = spy.loc[idx], tlt.loc[idx]
wk = idx.to_period("W-FRI")
last = pd.Series(wk, index=idx) != pd.Series(wk, index=idx).shift(-1)
last.iloc[-1] = False
def wtd(x):
    base = x["aC"].where(last).shift(1).ffill()            # previous week's last close
    return x["aC"].shift(1) / base - 1
gap = wtd(s) - wtd(t)
v = s.cc.rolling(20).std().shift(1); med = v.loc["2002":"2014"].median()
for tag, m in [("all weeks", last), ("high-vol weeks", last & (v > med))]:
    pos = (-np.sign(gap)).where(m, 0).fillna(0)
    r = lib.register("184_weekly_rebalance", f"184 week-end stock-bond fade ({tag})", pos, s.oc, 0.5)
    print(f"184 {tag:<15} {lib.fmt(r)}  gate={lib.passes_gate(r)}")
print("registry size:", lib.n_tests())
