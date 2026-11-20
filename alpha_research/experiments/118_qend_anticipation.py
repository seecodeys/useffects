"""118 - Quarter-end rebalancing anticipation: at QUARTER-end months, apply the 013 rule (pos = -sign(SPY MTD - TLT MTD)
through the prior close) on trading days -3 and -2 (i.e. the 3rd- and 2nd-to-last sessions), SPY open->close, 0.5bp.
"""
import os, sys, contextlib, io
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
ns = {"__file__": os.path.join(lib.HERE, "experiments", "013_month_end_rebalance.py")}
with contextlib.redirect_stdout(io.StringIO()):
    exec(open(ns["__file__"]).read().split("S = {")[0], ns)
s, rb, complete, gap, idx = ns["s"], ns["rank_bwd"], ns["complete"], ns["gap_mtd"], ns["idx"]
mask = rb.isin([2, 3]) & complete & pd.Series(idx.month % 3 == 0, index=idx)
pos = (-np.sign(gap)).where(mask, 0)
r = lib.register("013_month_end_rebalance", "118 quarter-end anticipation: days -3,-2 of quarter months", pos.fillna(0), s.oc, 0.5)
print(f"118  {lib.fmt(r)}  gate={lib.passes_gate(r)}  n={int(mask.sum())}")
print("registry size:", lib.n_tests())
