"""334 - Intraday momentum into the close (Gao, Han, Li & Zhou, JFE 2018; Baltussen, Da, Lammers & Martens, JFE 2021:
hedging demand of short-gamma dealers and LETF rebalancing push the last half hour in the direction of the day's move).
On the clock-corrected SPX CFD (2010-22), 290's 'fade' was −3.91 (train) / −2.29 (valid), i.e. the direction chosen on
TRAIN data is continuation. Single pre-specified rule (no new variants): position = sign(09:30→15:30 return), held
15:30→16:00, 0.5bp/side, every trading day. If it passes the gate: ONE holdout look on 2023+ CFD data."""
import os, sys, importlib
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
sys.path.insert(0, os.path.join(lib.HERE, "experiments"))
fx = importlib.import_module("003_overnight_fx")
px = fx.ny_minute_close("SPXUSD", range(2010, 2026))
spy = lib.get("SPY"); idx = spy.index[spy.index >= "2010-01-01"]
o, a, c = (fx.level_at(px, idx, h, m, tol_min=5) for h, m in [(9, 30), (15, 30), (16, 0)])
sig = np.sign(a / o - 1)
ret = (sig * (c / a - 1) - sig.abs() * 1e-4).dropna()
full = pd.Series(0.0, index=spy.index); full.loc[ret.index] = ret
pos = pd.Series(1.0, index=full.index)
r = lib.register("334_intraday_momentum", "334 follow sign(09:30->15:30) on 15:30->16:00 (SPX CFD, direction from train)",
                 pos.loc["2010":"2022"], full.loc["2010":"2022"], 0.0, notes="direction chosen on 2010-14 (290 clock-fix)")
print("334:", lib.fmt(r), " gate:", lib.passes_gate(r))
g = ret * 1e4
print("by year bp/day:", g.loc[:"2022"].groupby(g.loc[:"2022"].index.year).mean().round(2).to_dict())
if lib.passes_gate(r):
    h = lib.holdout_eval("334_intraday_momentum", "334 follow sign(09:30->15:30) on 15:30->16:00", pos.loc["2023":], full.loc["2023":], 0.0)
    print("HOLDOUT (2023+):", {k: round(float(v), 2) for k, v in h.items()}, " n days with data:", int(ret.loc['2023':].shape[0]))
