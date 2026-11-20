"""186 - Vol-seller re-striking: after monthly SPX options expire (3rd Friday), systematic option sellers re-establish
      short-vol positions the next week -> VIX futures soften. Short VIXY over the 5 sessions after monthly OPEX.
189 - Turn-of-month vol supply: short VIXY over the first 3 sessions of each month.
Both as timing value vs exposure-matched always-short VIXY (removes the roll premium), close->close, 5bp/side.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
v = lib.get("VIXY"); idx = v.index; r = v.cc
opex = pd.Series((idx.dayofweek == 4) & (idx.day >= 15) & (idx.day <= 21), index=idx)
after = pd.Series(0.0, index=idx)
for k in np.where(opex.values)[0]:
    after.iloc[k + 1:k + 6] = 1.0
fwd = pd.Series(1, index=idx).groupby(idx.to_period("M")).cumsum()
tom = (fwd <= 3).astype(float)
for name, want in [("186 short VIXY week after monthly OPEX", after), ("189 short VIXY first 3 sessions of month", tom)]:
    pos = -want.shift(-1).fillna(0)
    net = lib.backtest_weights(pos.to_frame("V"), r.to_frame("V"), 5.0)
    held = pos.shift(1).fillna(0); out = pd.Series(0.0, index=net.index)
    for a, b in [lib.TRAIN, lib.VALID, lib.HOLDOUT]:
        out.loc[a:b] = net.loc[a:b] - held.loc[a:b].mean() * r.loc[a:b].fillna(0)
    res = lib.register("186_vix_flows", name + " (timing value)", pd.Series(1.0, index=out.index), out.fillna(0), 0.0)
    print(f"{name:<44} {lib.fmt(res)}  gate={lib.passes_gate(res)}  (train only 2011-14)")
print("registry size:", lib.n_tests())
