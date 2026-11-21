"""325 - Re-leveraging after a volatility spike fades. Observation: vol-target / risk-parity funds cut equity exposure
when realized and implied vol spike, then add it back gradually (their vol estimates lag) once vol subsides -> steady
buying for 1-3 weeks after the spike breaks. Trigger at close t: VIX has fallen >= 25% from its 20-day high, and that
high was >= 25 (a real spike). First trigger in a 20-session window only. Long SPY from close t+1 (lag-1) for H sessions,
close->close, 0.5bp/side. Pre-specified: A H=10   B H=20   C H=10 with spike high >= 30."""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
spy = lib.get("SPY"); vix = lib.get("^VIX")["Close"].reindex(spy.index).ffill()
hi = vix.rolling(20).max()
def triggers(level):
    raw = (vix <= 0.75 * hi) & (hi >= level)
    out, last = pd.Series(False, index=raw.index), None
    for i, (d, x) in enumerate(raw.items()):
        if x and (last is None or i - last >= 20): out[d] = True; last = i
    return out
for name, lvl, H in [("A spike>=25, VIX -25% from 20d high, hold 10", 25, 10),
                     ("B spike>=25, hold 20", 25, 20), ("C spike>=30, hold 10", 30, 10)]:
    trig = triggers(lvl); w = trig.astype(float).rolling(H, min_periods=1).max()
    net = lib.backtest_weights(w.to_frame("X"), spy.cc.to_frame("X"), 0.5, lag=1).loc["2000":]
    held = w.shift(2).fillna(0).loc[net.index]
    tv = net - held.loc[:"2022"].mean() * spy.cc.loc[net.index]
    r = lib.register("325_vol_releverage", f"325{name}", pd.Series(1.0, index=net.index), net, 0.0)
    tt = lambda s: s.mean() / s.std() * np.sqrt(len(s))
    ev = trig.loc["2000":"2022"]
    print(f"325{name}: events {int(ev.loc[:'2014'].sum())}/{int(ev.loc['2015':].sum())}  {lib.fmt(r)}  timing t {tt(tv.loc[:'2014']):+.2f} / {tt(tv.loc['2015':'2022']):+.2f}")
print("registry size:", lib.n_tests())
