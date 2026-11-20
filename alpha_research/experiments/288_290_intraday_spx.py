"""288-290 - Intraday flow patterns with HistData SPXUSD 1-min CFD (2010-2025), NY clock; levels at prev 16:00, 09:30,
10:00, 15:00, 15:30, 16:00. Positions on the CFD/ES-like instrument, 0.5bp/side.
288 intraday momentum / hedging demand (Gao-Han-Li-Zhou 2018; Baltussen et al. 2021): sign(prev 16:00 -> 10:00) -> hold 15:30->16:00
289 LETF pre-positioning (Baltussen et al.; arXiv 2608.03703): sign(prev 16:00 -> 15:00) -> hold 15:00->16:00
290 end-of-day reversal (arXiv 2611.09286): -sign(09:30 -> 15:30) -> hold 15:30->16:00
"""
import os, sys, importlib
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
sys.path.insert(0, os.path.join(lib.HERE, "experiments"))
fx = importlib.import_module("003_overnight_fx")
px = fx.ny_minute_close("SPXUSD", range(2010, 2026))
spy = lib.get("SPY")
D = spy.index[(spy.index >= "2010-01-04") & (spy.index <= "2025-12-31")]
P = pd.DatetimeIndex(pd.Series(D, index=D).shift(1)).fillna(D[0])
L = lambda dates, h, m: fx.level_at(px, dates, h, m, tol_min=5)
c_prev = pd.Series(L(P, 16, 0).values, index=D)
o, t10, t15, t1530, c = L(D, 9, 30), L(D, 10, 0), L(D, 15, 0), L(D, 15, 30), L(D, 16, 0)
tests = {"288 momentum: sign(prev close->10:00) on 15:30->16:00": (np.sign(t10 / c_prev - 1), c / t1530 - 1),
         "289 LETF pre-positioning: sign(prev close->15:00) on 15:00->16:00": (np.sign(t15 / c_prev - 1), c / t15 - 1),
         "290 end-of-day reversal: -sign(09:30->15:30) on 15:30->16:00": (-np.sign(t1530 / o - 1), c / t1530 - 1)}
for name, (sig, ret) in tests.items():
    ser = (sig * ret - 2 * 0.5e-4).dropna()
    full = pd.Series(0.0, index=spy.index); full.loc[ser.index] = ser
    r = lib.register("288_intraday_spx", name, pd.Series(1.0, index=full.index), full.loc["2010":], 0.0)
    g = (sig * ret).dropna() * 1e4
    print(f"{name:<62} {lib.fmt(r)}  gross bp/day 2010-14 {g.loc[:'2014'].mean():+.2f} 2015-22 {g.loc['2015':'2022'].mean():+.2f}")
print("registry size:", lib.n_tests())
