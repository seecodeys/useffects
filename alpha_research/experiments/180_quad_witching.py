"""180 - Quad-witching / S&P quarterly rebalance close (3rd Friday of Mar/Jun/Sep/Dec): huge closing-auction volume from
index rebalances + futures/options expiry. Does that close reverse at the next open? SPY and IWM, close->next open,
direction = −sign(quad-witching day's open->close move) (fade the day's move into the close), 1bp/side."""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
for sym in ["SPY", "IWM"]:
    d = lib.get(sym); idx = d.index
    qw = pd.Series((idx.dayofweek == 4) & (idx.day >= 15) & (idx.day <= 21) & (idx.month % 3 == 0), index=idx)
    co = d["Open"].shift(-1) / d["Close"] - 1
    ser = (-np.sign(d.oc) * co - 2e-4).where(qw, 0).fillna(0).loc["2000":]
    r = lib.register("180_quad_witching", f"180 {sym} fade quad-witching day move from close to next open", pd.Series(1.0, index=ser.index), ser, 0.0)
    print(f"180 {sym}  {lib.fmt(r)}  gate={lib.passes_gate(r)}  n={int(qw.loc['2000':'2022'].sum())}")
print("registry size:", lib.n_tests())
