"""219 - Weekend option-decay mechanics: option market makers price weekend theta in ahead of time -> implied vol (and VIX
futures) tends to soften into Friday's close and rebound on Monday. VIXY close->close, 5bp/side, timing value vs
exposure-matched always-short is not appropriate (two-sided), so raw:
  a) short VIXY over Fridays (close Thu -> close Fri)
  b) long VIXY over Mondays (close Fri -> close Mon)   [pre-specified pair; registered separately]
Also descriptive: mean ^VIX % change by weekday 2000-2022.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
vix = lib.get("^VIX")["Close"].loc["2000":"2022"]
ch = vix.pct_change()
print("^VIX mean % change by weekday 2000-22:", (ch.groupby(ch.index.dayofweek).mean() * 100).round(2).to_dict())
v = lib.get("VIXY"); idx = v.index
for tag, dow, sgn in [("a short VIXY on Fridays", 4, -1.0), ("b long VIXY on Mondays", 0, 1.0)]:
    want = pd.Series(np.where(idx.dayofweek == dow, sgn, 0.0), index=idx)
    pos = want.shift(-1).fillna(0)
    net = lib.backtest_weights(pos.to_frame("V"), v.cc.to_frame("V"), 5.0)
    r = lib.register("219_vix_weekend", f"219{tag}", pd.Series(1.0, index=net.index), net, 0.0)
    print(f"219{tag:<26} {lib.fmt(r)}  gate={lib.passes_gate(r)}  (train only 2011-14)")
print("registry size:", lib.n_tests())
