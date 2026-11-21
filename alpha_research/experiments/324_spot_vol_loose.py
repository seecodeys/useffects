"""324 - 323 with looser, pre-set thresholds for statistical power. Same mechanism (options demand moving WITH spot is
unstable). Signal at close t, traded from close t+1 (lag-1), SPY close->close, 0.5bp/side. Pre-specified:
 A short 5d after SPY up & VIX up same day (any size)        B same with SPY > +0.25% and VIX > +2%
 C long 5d after SPY down & VIX down same day (any size)      D same with SPY < -0.25% and VIX < -2%"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
spy = lib.get("SPY"); vix = lib.get("^VIX")["Close"].reindex(spy.index).ffill(); dv = vix.pct_change()
hold = lambda trig: trig.astype(float).rolling(5, min_periods=1).max()
T = {"A short 5d after SPY up & VIX up": (-1, (spy.cc > 0) & (dv > 0)),
     "B short 5d after SPY>+0.25% & VIX>+2%": (-1, (spy.cc > 0.0025) & (dv > 0.02)),
     "C long 5d after SPY down & VIX down": (1, (spy.cc < 0) & (dv < 0)),
     "D long 5d after SPY<-0.25% & VIX<-2%": (1, (spy.cc < -0.0025) & (dv < -0.02))}
for name, (sgn, trig) in T.items():
    w = sgn * hold(trig)
    net = lib.backtest_weights(w.to_frame("X"), spy.cc.to_frame("X"), 0.5, lag=1).loc["2000":]
    held = w.shift(2).fillna(0).loc[net.index]
    tv = net - held.loc[:"2022"].mean() * spy.cc.loc[net.index]
    r = lib.register("323_spot_vol", f"324{name}", pd.Series(1.0, index=net.index), net, 0.0)
    tt = lambda s: s.mean() / s.std() * np.sqrt(len(s))
    print(f"324{name}: events {int(trig.loc['2000':'2022'].sum())}  {lib.fmt(r)}  timing t {tt(tv.loc[:'2014']):+.2f} / {tt(tv.loc['2015':'2022']):+.2f}")
print("registry size:", lib.n_tests())
