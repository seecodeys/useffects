"""226 - Risk-parity / multi-asset month-end rebalancing in gold: on the last trading day,
pos_GLD = −sign(GLD MTD − ½(SPY MTD + TLT MTD)) (MTD through the prior close), GLD open->close, 1bp/side. 2005+."""
import os, sys, contextlib, io
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
ns = {"__file__": os.path.join(lib.HERE, "experiments", "013_month_end_rebalance.py")}
with contextlib.redirect_stdout(io.StringIO()):
    exec(open(ns["__file__"]).read().split("S = {")[0], ns)
s, t, last1, mtd, idx = ns["s"], ns["t"], ns["last1"], ns["mtd"], ns["idx"]
g = lib.get("GLD").reindex(idx)
gap = mtd(g) - 0.5 * (mtd(s) + mtd(t))
pos = (-np.sign(gap)).where(last1 & g["aC"].notna(), 0).fillna(0)
r = lib.register("226_riskparity_gold", "226 GLD month-end: -sign(GLD MTD - avg(SPY,TLT) MTD)", pos, g.oc.fillna(0), 1.0)
print(f"226  {lib.fmt(r)}  gate={lib.passes_gate(r)}")
print("registry size:", lib.n_tests())
