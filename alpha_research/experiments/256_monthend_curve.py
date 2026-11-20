"""256 - Bond side of the month-end rebalance in curve terms: when stocks beat bonds MTD, balanced funds buy bonds
(index-weighted, but long duration moves most). Last trading day, position +sign(SPY MTD − TLT MTD) in TLT−IEF
(duration-unhedged spread), open->close, 1bp/side/leg."""
import os, sys, contextlib, io
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
ns = {"__file__": os.path.join(lib.HERE, "experiments", "013_month_end_rebalance.py")}
with contextlib.redirect_stdout(io.StringIO()):
    exec(open(ns["__file__"]).read().split("S = {")[0], ns)
t, last1, gap, idx = ns["t"], ns["last1"], ns["gap_mtd"], ns["idx"]
ief = lib.get("IEF").reindex(idx)
d = (np.sign(gap)).where(last1, 0).fillna(0)
ser = (d * (t.oc - ief.oc) - d.abs() * 4e-4).fillna(0)
r = lib.register("013_month_end_rebalance", "256 TLT-IEF curve on last day, +sign(stock-bond MTD gap)", pd.Series(1.0, index=idx), ser, 0.0)
tt = (d * (t.oc - ief.oc))[d != 0] * 1e4
print(f"256  {lib.fmt(r)}  per-trade gross train {tt.loc[:'2014'].mean():+.1f} valid {tt.loc['2015':'2022'].mean():+.1f}")
print("registry size:", lib.n_tests())
