"""013f - Robustness of 013: bond leg = AGG (broad US aggregate bond ETF, 2003+) instead of TLT. Same rule, SPY open->close,
last trading day, 0.5bp/side."""
import os, sys, contextlib, io
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
ns = {"__file__": os.path.join(lib.HERE, "experiments", "013_month_end_rebalance.py")}
with contextlib.redirect_stdout(io.StringIO()):
    exec(open(ns["__file__"]).read().split("S = {")[0], ns)
s, last1, mtd, idx = ns["s"], ns["last1"], ns["mtd"], ns["idx"]
agg = lib.get("AGG").reindex(idx)
gap_agg = mtd(s) - mtd(agg)
pos = (-np.sign(gap_agg)).where(last1 & agg["aC"].notna(), 0).fillna(0)
r = lib.register("013_month_end_rebalance", "013f bond leg = AGG instead of TLT", pos, s.oc, 0.5)
print(f"013f  {lib.fmt(r)}  gate={lib.passes_gate(r)}")
agree = (np.sign(gap_agg) == np.sign(ns["gap_mtd"]))[last1 & agg["aC"].notna()].loc[:"2022"].mean()
print(f"sign agreement with TLT-based signal on month-ends (2003-22): {agree:.0%}")
print("registry size:", lib.n_tests())
