"""307 - Mechanism control for the month-end bond bid: do rate-sensitive equities (utilities XLU, REITs VNQ/IYR) show a
last-3-session bid? If the bond bid were a general 'rates fall at month-end' effect, bond proxies should rise too; if it
is bond-index specific (extension / benchmark buying), they shouldn't. Timing value over always-long, close->close."""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
for sym in ["XLU", "VNQ", "IYR"]:
    d = lib.get(sym); idx = d.index; ym = pd.Series(idx.to_period("M"), index=idx)
    bwd = pd.Series(1, index=idx)[::-1].groupby(ym[::-1]).cumsum()[::-1]
    pos = ((bwd <= 3) & (ym != ym.iloc[-1])).astype(float).shift(-1).fillna(0)
    net = lib.backtest_weights(pos.to_frame("X"), d.cc.to_frame("X"), 1.0).loc["2002-08":]
    held = pos.shift(1).loc[net.index]
    tv = net - held.loc[:"2022"].mean() * d.cc.loc[net.index]
    r = lib.register("275_bond_tom", f"307 {sym} last-3 timing value (bond-proxy control)", pd.Series(1.0, index=tv.index), tv, 0.0)
    print(f"307 {sym}: {lib.fmt(r)}")
print("registry size:", lib.n_tests())
