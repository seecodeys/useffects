"""163 - Bond-market holiday flows: SIFMA early closes / bond-market holidays cause dealers to square positions.
Pre-specified: long TLT close->close over the session BEFORE a market holiday (weekday closure) — mirrors the classic
equity pre-holiday effect, which was dead for SPY intraday (001). 1bp/side. Also SPY close->close as comparison.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
for sym, c in [("TLT", 1.0), ("SPY", 0.5)]:
    d = lib.get(sym); idx = d.index
    nxt = pd.Series(idx, index=idx).shift(-1)
    gapd = (nxt - pd.Series(idx, index=idx)).dt.days
    pre = (gapd > 1) & ~((idx.dayofweek == 4) & (gapd == 3))     # next session not the normal next weekday
    # hold over the pre-holiday session: position set at the close BEFORE it
    pos = pre.shift(-1, fill_value=False).astype(float)
    net = lib.backtest_weights(pos.to_frame("X"), d.cc.to_frame("X"), c, lag=0).loc["2000":]
    r = lib.register("163_preholiday_cc", f"163 long {sym} close->close on pre-holiday session", pd.Series(1.0, index=net.index), net, 0.0)
    print(f"163 {sym}  {lib.fmt(r)}  gate={lib.passes_gate(r)}  n={int(pre.loc['2000':'2022'].sum())}")
print("registry size:", lib.n_tests())
