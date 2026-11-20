"""276 - Month-end extension by curve point (train/valid consistency; no holdout look): long IEF (7-10y) and SHY (1-3y)
over the last 3 sessions of each month, close->close, 1bp/side. Extension effect should scale with duration: TLT > IEF > SHY."""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
for sym in ["IEF", "SHY"]:
    d = lib.get(sym); idx = d.index; ym = pd.Series(idx.to_period("M"), index=idx)
    bwd = pd.Series(1, index=idx)[::-1].groupby(ym[::-1]).cumsum()[::-1]
    pos = ((bwd <= 3) & (ym != ym.iloc[-1])).astype(float).shift(-1).fillna(0)
    net = lib.backtest_weights(pos.to_frame("X"), d.cc.to_frame("X"), 1.0).loc["2002-08":]
    g = lib.backtest_weights(pos.to_frame("X"), d.cc.to_frame("X"), 0.0).loc["2002-08":"2022"]
    r = lib.register("275_bond_tom", f"276 long {sym} last 3 sessions of month", pd.Series(1.0, index=net.index), net, 0.0)
    print(f"276 {sym}  {lib.fmt(r)}  gross ann % 2002-22: {g.mean()*252*100:.2f}")
print("registry size:", lib.n_tests())
