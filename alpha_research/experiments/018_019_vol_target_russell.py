"""018 - Vol-target / risk-parity (de)leveraging, SPY open->close, 0.5bp/side.
   ratio = 5d realized vol / 60d realized vol of SPY close-to-close, through the prior close.
   H1 short when ratio > 2 (funds still de-levering)   H2 long when ratio < 0.5 (re-levering)
   H3 both combined
019 - Russell reconstitution day (last Friday of June; IWM-SPY spread open->close, 1bp/side/leg).
   Direction picked on TRAIN (2000-14) only; ~1 event/yr so this is mostly descriptive.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

spy = lib.get("SPY").loc["1999":]
rv = lambda n: spy.cc.rolling(n).std()
ratio = (rv(5) / rv(60)).shift(1)                     # known at prior close
y = spy.oc.loc["2000":]
ratio = ratio.reindex(y.index)
S = {"018 H1 short SPY when 5d/60d vol > 2": -(ratio > 2).astype(float),
     "018 H2 long SPY when 5d/60d vol < 0.5": (ratio < 0.5).astype(float),
     "018 H3 combined": (ratio < 0.5).astype(float) - (ratio > 2).astype(float)}
for name, pos in S.items():
    r = lib.register("018_vol_target", name, pos, y, 0.5)
    print(f"{name:<42} {lib.fmt(r)}  gate={lib.passes_gate(r)}  days={int((pos!=0).sum())}")

iwm = lib.get("IWM")
idx = y.index
lastfri_june = [d for d in idx if d.month == 6 and d.dayofweek == 4 and (d + pd.Timedelta(days=7)).month == 7]
spread = (iwm.oc - spy.oc).reindex(idx)
ev = spread.loc[lastfri_june].dropna()
tr = ev.loc[:lib.TRAIN[1]]
direction = np.sign(tr.mean())
pos = pd.Series(0.0, index=idx); pos.loc[ev.index] = direction
net = pos * spread.fillna(0) - pos.abs() * 4 * 1.0 / 1e4
r = lib.register("019_russell_recon", f"019 IWM-SPY on recon day, direction {int(direction):+d} (from train)",
                 pd.Series(1.0, index=idx), net, 0.0, notes="~1 event/yr")
print("019 events:", len(ev), "train mean bp:", round(1e4 * tr.mean(), 1), "valid mean bp:",
      round(1e4 * ev.loc[lib.VALID[0]:lib.VALID[1]].mean(), 1), " ", lib.fmt(r))
print("registry size:", lib.n_tests())
