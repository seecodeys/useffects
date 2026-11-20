"""281 - Full month-end bond cycle on IEF and TLT (consistency, no holdout looks): long over the last 3 sessions of the month,
short over the first 3 sessions of the next month; close->close, 1bp/side."""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
for sym in ["IEF", "TLT"]:
    d = lib.get(sym); idx = d.index; ym = pd.Series(idx.to_period("M"), index=idx)
    fwd = pd.Series(1, index=idx).groupby(ym).cumsum()
    bwd = pd.Series(1, index=idx)[::-1].groupby(ym[::-1]).cumsum()[::-1]
    want = ((bwd <= 3) & (ym != ym.iloc[-1])).astype(float) - (fwd <= 3).astype(float)
    pos = want.shift(-1).fillna(0)
    net = lib.backtest_weights(pos.to_frame("X"), d.cc.to_frame("X"), 1.0).loc["2002-08":]
    r = lib.register("275_bond_tom", f"281 {sym} long last 3 / short first 3 sessions", pd.Series(1.0, index=net.index), net, 0.0)
    print(f"281 {sym}  {lib.fmt(r)}")
print("registry size:", lib.n_tests())
