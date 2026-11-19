"""009/010 - Volatility regime & prior-day decomposition -> SPY/QQQ open->close.
Pre-specified hypotheses (all info known before 9:30 ET; VIX from prior close):
  H1 prior-day intraday reversal: pos = -sign(yesterday's open->close)
  H2 H1 only when VIX term structure is inverted (VIX > VIX3M at prior close)
  H3 gap fade only when prior VIX close > 25
  H4 long after a VIX spike (prior-day VIX close-to-close > +20%)
  H5 long when VIX term structure inverted (fear rebound), else flat
  H6 'tug-of-war' : yesterday's gap and intraday had opposite signs -> follow yesterday's intraday
"""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

EXP = "009_vix_regime"
vix = lib.get("^VIX")["Close"]
vix3m = lib.get("^VIX3M")["Close"]
for sym in ["SPY", "QQQ"]:
    d = lib.get(sym).loc["2000":]
    D = d.index
    P = pd.DatetimeIndex(pd.Series(D, index=D).shift(1))
    on_D = lambda s, dates: pd.Series(lib.asof(s, dates).values, index=D)   # values known at prior closes
    v = on_D(vix, P)
    v_prev2 = on_D(vix, pd.DatetimeIndex(pd.Series(D, index=D).shift(2)))
    v3 = on_D(vix3m, P)
    inv = (v > v3) & v3.notna()
    prev_oc, prev_gap = d.oc.shift(1), d.gap.shift(1)
    S = {
        "H1 fade prior intraday": -np.sign(prev_oc),
        "H2 fade prior intraday | VIX>VIX3M": (-np.sign(prev_oc)).where(inv, 0),
        "H3 gap fade | VIX>25": (-np.sign(d.gap)).where(v > 25, 0),
        "H4 long after VIX spike >20%": pd.Series(np.where(v / v_prev2 - 1 > 0.20, 1.0, 0.0), index=D),
        "H5 long when VIX>VIX3M": inv.astype(float),
        "H6 follow prior intraday | prior gap & intraday disagree": np.sign(prev_oc).where(np.sign(prev_oc) != np.sign(prev_gap), 0),
    }
    for name, pos in S.items():
        r = lib.register(EXP, f"{sym} {name}", pos.fillna(0), d.oc, 0.5)
        print(f"{sym} {name:<55} {lib.fmt(r)}  gate={lib.passes_gate(r)}")
print("registry size:", lib.n_tests())
