"""011 - Rebound after extreme overseas sell-offs (the one weak pattern from global_open_close).
Uses global_open_close.features (look-ahead-free Asia closes + Europe opens since last US close).
  H1 long SPY open->close when global_avg is in the bottom 5% of its TRAIN distribution
  H2 same, only when prior VIX close > 25
0.5bp/side.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
sys.path.insert(0, os.path.join(os.path.dirname(lib.HERE), "global_open_close"))
from features import build

f = build()
thr = f.global_avg.loc[lib.TRAIN[0]:lib.TRAIN[1]].quantile(0.05)
ev = f.global_avg <= thr
for name, mask in {"011 H1 long SPY after bottom-5% overseas session": ev,
                   "011 H2 same, prior VIX > 25": ev & (f.vix_prev > 25)}.items():
    pos = mask.astype(float)
    r = lib.register("011_overseas_selloff", name, pos, f.y, 0.5)
    print(f"{name:<50} {lib.fmt(r)}  gate={lib.passes_gate(r)}  events={int(mask.sum())}")
print("threshold global_avg <=", round(thr * 100, 2), "%   registry size:", lib.n_tests())
