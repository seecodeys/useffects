"""028 - Japanese fiscal year-end (Mar 31) / half-year-end (Sep 30) institutional window dressing and post-period selling.
  a) long EWJ open->close on the last US trading day of March and September
  b) short EWJ close->close over the first 3 sessions of April and October
2bp/side. ~25 events per leg per period -> low power."""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
e = lib.get("EWJ").loc["2000":]; idx = e.index; ym = pd.Series(idx.to_period("M"), index=idx)
bwd = pd.Series(1, index=idx)[::-1].groupby(ym[::-1]).cumsum()[::-1]
fwd = pd.Series(1, index=idx).groupby(ym).cumsum()
a = ((bwd == 1) & pd.Series(np.isin(idx.month, [3, 9]), index=idx) & (ym != ym.iloc[-1])).astype(float)
r = lib.register("028_japan_fy", "028a long EWJ open->close last day of Mar/Sep", a, e.oc, 2.0)
print(f"028a {lib.fmt(r)}  n={int(a.loc[:'2022'].sum())}")
want = -((fwd <= 3) & pd.Series(np.isin(idx.month, [4, 10]), index=idx)).astype(float)
pos = want.shift(-1).fillna(0)
net = lib.backtest_weights(pos.to_frame("E"), e.cc.to_frame("E"), 2.0)
r = lib.register("028_japan_fy", "028b short EWJ first 3 sessions of Apr/Oct", pd.Series(1.0, index=net.index), net, 0.0)
print(f"028b {lib.fmt(r)}")
print("registry size:", lib.n_tests())
