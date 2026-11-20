"""179 - Robustness of 013g+168 (no holdout looks): last trading day, -sign(SPY MTD - TLT MTD) in QQQ-TLT and IWM-TLT pairs,
only when SPY 20d vol > its 2002-14 median. open->close, 1bp/side/leg."""
import os, sys, contextlib, io
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
ns = {"__file__": os.path.join(lib.HERE, "experiments", "013_month_end_rebalance.py")}
with contextlib.redirect_stdout(io.StringIO()):
    exec(open(ns["__file__"]).read().split("S = {")[0], ns)
s, t, last1, gap, idx = ns["s"], ns["t"], ns["last1"], ns["gap_mtd"], ns["idx"]
v = s.cc.rolling(20).std().shift(1); med = v.loc["2002":"2014"].median()
d = (-np.sign(gap)).where(last1 & (v > med), 0).fillna(0)
for sym in ["QQQ", "IWM"]:
    x = lib.get(sym).reindex(idx)
    net = (d * (x.oc - t.oc) - d.abs() * 4e-4).fillna(0)
    r = lib.register("013_month_end_rebalance", f"179 {sym}-TLT pair, month-end, high-vol only", pd.Series(1.0, index=net.index), net, 0.0)
    tt = (d * (x.oc - t.oc))[d != 0] * 1e4
    print(f"179 {sym}-TLT  {lib.fmt(r)}  per-trade train {tt.loc[:'2014'].mean():+.1f} valid {tt.loc['2015':'2022'].mean():+.1f}")
print("registry size:", lib.n_tests())
