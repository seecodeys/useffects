"""151 - CTA trend-following flows: when SPY's close crosses its 50-day (or 200-day) moving average, trend followers
      add exposure in that direction over the following days -> follow the cross for 3 sessions (close->close, from the
      crossing close; signal known at that close, best-case MOC) and lag-1 version. 0.5bp/side.
154 - VIX ETP daily rebalancing: after a |VIXY close->close| > 10% day, ETPs have traded VIX futures into the close in the
      direction of the move -> overshoot -> fade it next day (close->close, lag-1: signal at close t, position over t+2?
      No: position from close t+1 is lag-1; we report lag0 best-case and lag1). 5bp/side.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
spy = lib.get("SPY")
for n in [50, 200]:
    ma = spy.aC.rolling(n).mean()
    above = spy.aC > ma
    cross = above.astype(int).diff()                 # +1 up-cross, -1 down-cross at close t
    sig = cross.replace(0, np.nan).ffill(limit=2).fillna(0)   # hold 3 sessions incl. the crossing close
    for lag in [0, 1]:
        net = lib.backtest_weights(sig.to_frame("S"), spy.cc.to_frame("S"), 0.5, lag=lag).loc["2000":]
        r = lib.register("151_cta_flows", f"151 follow SPY {n}d-MA cross for 3 sessions (lag{lag})", pd.Series(1.0, index=net.index), net, 0.0)
        print(f"151 MA{n} lag{lag}  {lib.fmt(r)}  gate={lib.passes_gate(r)}  crosses/yr={cross.abs().loc['2000':'2022'].sum()/23:.0f}")
v = lib.get("VIXY")
sig = pd.Series(np.where(v.cc > 0.10, -1.0, np.where(v.cc < -0.10, 1.0, 0.0)), index=v.index)
for lag in [0, 1]:
    net = lib.backtest_weights(sig.to_frame("V"), v.cc.to_frame("V"), 5.0, lag=lag)
    r = lib.register("154_vix_etp_rebalance", f"154 fade |VIXY day|>10% next day (lag{lag})", pd.Series(1.0, index=net.index), net, 0.0)
    print(f"154 lag{lag}  {lib.fmt(r)}  gate={lib.passes_gate(r)}  events={int(sig.abs().loc[:'2022'].sum())} (train only 2011-14)")
print("registry size:", lib.n_tests())
