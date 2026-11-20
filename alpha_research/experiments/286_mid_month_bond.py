"""286 - Mid-month Treasury cash flows (coupons/maturities on the 15th): long IEF close->close over the 3 sessions ending
on the 15th (or next business day), every month; 1bp/side."""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
d = lib.get("IEF"); idx = d.index
want = pd.Series(0.0, index=idx)
for y in range(2002, 2027):
    for m in range(1, 13):
        k = idx.searchsorted(pd.Timestamp(y, m, 15))
        if 3 <= k < len(idx):
            want.iloc[k - 2:k + 1] = 1.0
pos = want.shift(-1).fillna(0)
net = lib.backtest_weights(pos.to_frame("I"), d.cc.to_frame("I"), 1.0).loc["2002-08":]
r = lib.register("286_mid_month_bond", "286 long IEF 3 sessions ending on the 15th", pd.Series(1.0, index=net.index), net, 0.0)
print(f"286  {lib.fmt(r)}  gate={lib.passes_gate(r)}")
print("registry size:", lib.n_tests())
