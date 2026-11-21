"""323 - 'Spot up, vol up'. Observation: normally VIX falls when stocks rise. Days when both rise mean someone is paying
up for options into a rally (upside call chasing / hedging demand while rising) — often seen near short-term tops and
before reversals. Signal at close t (SPY cc > +0.5% and VIX change > +5%), traded from close t+1 (lag-1 per protocol)
for H sessions, SPY close->close, 0.5bp/side. Pre-specified:
 A short SPY 1 session   B short SPY 5 sessions   C mirror: 'spot down, vol down' (SPY < -0.5%, VIX < -5%) long 5 sessions"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
spy = lib.get("SPY"); vix = lib.get("^VIX")["Close"].reindex(spy.index).ffill()
dv = vix.pct_change()
up = (spy.cc > 0.005) & (dv > 0.05); dn = (spy.cc < -0.005) & (dv < -0.05)
hold = lambda trig, h: trig.astype(float).rolling(h, min_periods=1).max()
V = {"A short 1d after spot-up-vol-up": -hold(up, 1), "B short 5d after spot-up-vol-up": -hold(up, 5),
     "C long 5d after spot-down-vol-down": hold(dn, 5)}
print("events 2000-22: up", int(up.loc[:"2022"].sum()), "down", int(dn.loc[:"2022"].sum()))
for name, w in V.items():
    net = lib.backtest_weights(w.to_frame("X"), spy.cc.to_frame("X"), 0.5, lag=1).loc["2000":]
    held = w.shift(2).fillna(0).loc[net.index]
    tv = net - held.loc[:"2022"].mean() * spy.cc.loc[net.index]
    r = lib.register("323_spot_vol", f"323{name}", pd.Series(1.0, index=net.index), net, 0.0)
    tt = lambda s: s.mean() / s.std() * np.sqrt(len(s))
    print(f"323{name}: {lib.fmt(r)}  timing t {tt(tv.loc[:'2014']):+.2f} / {tt(tv.loc['2015':'2022']):+.2f}")
print("registry size:", lib.n_tests())
