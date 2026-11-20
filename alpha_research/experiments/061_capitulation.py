"""061 - Market-level capitulation / euphoria (behavioral), SPY close->close, lag-1, 0.5bp/side on turnover.
Because a long-only SPY position earns the equity premium regardless, each rule is evaluated as an EXCESS over an
exposure-matched buy-and-hold: daily net = pos*r - mean(pos over the period)*r  (i.e. pure timing value).
  a) capitulation: long when SPY is > 15% below its 252-day high (fear over-sold)
  b) euphoria: flat when SPY is at a 252-day high and its 21-day return > +8% (over-bought), long otherwise
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
spy = lib.get("SPY")
r = spy.cc
dd = spy.aC / spy.aC.rolling(252).max() - 1
mom = spy.aC / spy.aC.shift(21) - 1
rules = {"061a long SPY when >15% below 252d high (timing excess)": (dd < -0.15).astype(float),
         "061b flat at euphoric highs, long otherwise (timing excess)": 1.0 - ((dd >= 0) & (mom > 0.08)).astype(float)}
for name, pos in rules.items():
    p = pos.to_frame("SPY")
    net = lib.backtest_weights(p, r.to_frame("SPY"), 0.5, lag=1)
    held = p["SPY"].shift(2).fillna(0)
    out = pd.Series(0.0, index=net.index)
    for a, b in [lib.TRAIN, lib.VALID, lib.HOLDOUT]:
        m = held.loc[a:b].mean()
        out.loc[a:b] = net.loc[a:b] - m * r.loc[a:b].fillna(0)
    out = out.loc["2000":]
    res = lib.register("061_capitulation", name, pd.Series(1.0, index=out.index), out, 0.0, notes="excess over exposure-matched B&H")
    print(f"{name:<60} {lib.fmt(res)}  gate={lib.passes_gate(res)}  time-in-mkt={100*held.loc['2000':'2022'].mean():.0f}%")
print("registry size:", lib.n_tests())
