"""285 - Controls for the month-end bond bid: assets without bond-index extension should NOT show a last-3-session bid.
GLD, SPY, UUP: long over the last 3 sessions of each month, close->close; timing value vs exposure-matched always-long
(to strip drift). 1bp/side."""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
for sym in ["GLD", "SPY", "UUP"]:
    d = lib.get(sym); idx = d.index; ym = pd.Series(idx.to_period("M"), index=idx)
    bwd = pd.Series(1, index=idx)[::-1].groupby(ym[::-1]).cumsum()[::-1]
    pos = ((bwd <= 3) & (ym != ym.iloc[-1])).astype(float).shift(-1).fillna(0)
    net = lib.backtest_weights(pos.to_frame("X"), d.cc.to_frame("X"), 1.0)
    held = pos.shift(1).fillna(0); out = pd.Series(0.0, index=net.index)
    for a, b in [lib.TRAIN, lib.VALID, lib.HOLDOUT]:
        out.loc[a:b] = net.loc[a:b] - held.loc[a:b].mean() * d.cc.loc[a:b].fillna(0)
    out = out.loc["2002-08":]
    r = lib.register("285_bid_controls", f"285 control: {sym} last 3 sessions (timing value)", pd.Series(1.0, index=out.index), out, 0.0)
    print(f"285 {sym}  {lib.fmt(r)}")
print("registry size:", lib.n_tests())
