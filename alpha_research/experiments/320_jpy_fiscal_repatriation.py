"""320 - Japanese fiscal-year-end repatriation (FX): Japanese exporters and institutions bring foreign earnings home
ahead of book closing (Mar 31; half-year Sep 30) -> yen buying in the run-up, then outflows in the new fiscal year
(lifers' new-year foreign investment plans). FX leg of 028 (which used EWJ). USDJPY daily (Yahoo JPY=X), close->close
(Yahoo FX 'close' ~ 23:00 London), 1bp/side. Pre-specified:
 A short USDJPY over the last 10 sessions of March      B same for March and September
 C long USDJPY over the first 10 sessions of April (new-fiscal-year outflows)"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
d = lib.get("JPY=X", drop_today=False); idx = d.index[d.index >= "2000-01-01"]
r = d.cc.loc[idx]
ym = pd.Series(idx.to_period("M"), index=idx)
bwd = pd.Series(1, index=idx)[::-1].groupby(ym[::-1]).cumsum()[::-1]
fwd = pd.Series(1, index=idx).groupby(ym).cumsum()
mo = pd.Series(idx.month, index=idx)
complete = ym != ym.iloc[-1]
V = {"A short USDJPY last 10 sessions of Mar": -((bwd <= 10) & (mo == 3) & complete).astype(float),
     "B short USDJPY last 10 sessions of Mar+Sep": -((bwd <= 10) & mo.isin([3, 9]) & complete).astype(float),
     "C long USDJPY first 10 sessions of Apr": ((fwd <= 10) & (mo == 4)).astype(float)}
for name, held in V.items():
    w = held.shift(-1).fillna(0)                        # weight decided at the prior close
    net = lib.backtest_weights(w.to_frame("X"), r.to_frame("X"), 1.0)
    res = lib.register("320_jpy_fiscal", f"320{name}", pd.Series(1.0, index=net.index), net, 0.0)
    per = net[held != 0].groupby(ym[held != 0]).sum() * 1e4
    print(f"320{name}: {lib.fmt(res)}  per-window bp 2000-14 {per.loc[:'2014'].mean():+.0f} (n={len(per.loc[:'2014'])}), 2015-22 {per.loc['2015':'2022'].mean():+.0f}")
print("registry size:", lib.n_tests())
