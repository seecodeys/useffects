"""276b - One holdout look for the IEF last-3-sessions month-end bid (276 was pre-specified as consistency-only; this
deliberate look is logged as the 2nd holdout look of the month-end bond-bid family, after 275 on TLT)."""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
for sym, c in [("IEF", 1.0)]:
    d = lib.get(sym); idx = d.index; ym = pd.Series(idx.to_period("M"), index=idx)
    bwd = pd.Series(1, index=idx)[::-1].groupby(ym[::-1]).cumsum()[::-1]
    pos = ((bwd <= 3) & (ym != ym.iloc[-1])).astype(float).shift(-1).fillna(0)
    for cost in [1.0, 2.0]:
        net = lib.backtest_weights(pos.to_frame("X"), d.cc.to_frame("X"), cost).loc["2002-08":]
        h = lib.holdout_eval("275_bond_tom", f"276b IEF last 3 sessions (cost {cost}bp) [2nd family look]", pd.Series(1.0, index=net.index), net, 0.0)
        print("HOLDOUT", cost, {k: round(float(v), 2) for k, v in h.items()})
    net = lib.backtest_weights(pos.to_frame("X"), d.cc.to_frame("X"), 1.0).loc["2023":]
    print("holdout yearly %:", (net.groupby(net.index.year).sum() * 100).round(2).to_dict())
