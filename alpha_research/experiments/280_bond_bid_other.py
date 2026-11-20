"""280 - Month-end bond bid in other index segments (train/valid consistency, no holdout looks): MBB (agency MBS, 2007+),
MUB (munis, 2007+), TIP (TIPS, 2003+). Long over the last 3 sessions of each month, close->close, 1.5bp/side."""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
for sym in ["MBB", "MUB", "TIP"]:
    d = lib.get(sym); idx = d.index; ym = pd.Series(idx.to_period("M"), index=idx)
    bwd = pd.Series(1, index=idx)[::-1].groupby(ym[::-1]).cumsum()[::-1]
    pos = ((bwd <= 3) & (ym != ym.iloc[-1])).astype(float).shift(-1).fillna(0)
    net = lib.backtest_weights(pos.to_frame("X"), d.cc.to_frame("X"), 1.5).loc["2002-08":]
    r = lib.register("275_bond_tom", f"280 long {sym} last 3 sessions of month", pd.Series(1.0, index=net.index), net, 0.0)
    print(f"280 {sym}  {lib.fmt(r)}  start {net.index[0].date()}")
print("registry size:", lib.n_tests())
