"""274 - Global funds' month-end country rebalancing: on the last US trading day, long the 4 single-country ETFs with the
lowest MTD return (through the prior close), short the 4 highest; open->close, equal weight, 2bp/side/leg."""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
C = ["EWJ", "EWG", "EWU", "EWH", "EWA", "EWS", "EWC", "EWW", "EWZ", "EWY", "EWT", "EWP", "EWI", "EWQ", "EWL", "EWN", "EWD", "EWK", "EWO", "EWM"]
spy = lib.get("SPY"); idx = spy.index[spy.index >= "2000-01-01"]
ym = pd.Series(idx.to_period("M"), index=idx)
last = (ym != ym.shift(-1)) & (ym != ym.iloc[-1])
first = ym != ym.shift(1)
AC = pd.DataFrame({c: lib.get(c)["aC"] for c in C}).reindex(idx)
OC = pd.DataFrame({c: lib.get(c).oc for c in C}).reindex(idx)
base = AC.where(last, axis=0).shift(1).ffill().where(first, axis=0).ffill()
mtd = AC.shift(1) / base - 1
rk = mtd.rank(axis=1); n = mtd.notna().sum(axis=1)
W = (rk.le(4).astype(float) / 8 - rk.ge(n.values[:, None] - 3).astype(float) / 8).where(last & (n >= 12), 0, axis=0).fillna(0)
net = (W * OC.fillna(0)).sum(axis=1) - W.abs().sum(axis=1) * 2 * 2e-4
r = lib.register("274_country_monthend", "274 country ETFs last day: long 4 MTD losers / short 4 winners", pd.Series(1.0, index=idx), net, 0.0)
tt = net[last] * 1e4
print(f"274  {lib.fmt(r)}  per-trade net train {tt.loc[:'2014'].mean():+.1f} valid {tt.loc['2015':'2022'].mean():+.1f}")
print("registry size:", lib.n_tests())
