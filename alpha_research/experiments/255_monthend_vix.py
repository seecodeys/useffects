"""255 - Month-end rebalancing flow seen through volatility: when the flow buys equities (d=+1) VIX futures should soften.
Last trading day, position −d in VIXY open->close (short vol when the signal is long equities), 5bp/side. 2011+."""
import os, sys, contextlib, io
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
ns = {"__file__": os.path.join(lib.HERE, "experiments", "013_month_end_rebalance.py")}
with contextlib.redirect_stdout(io.StringIO()):
    exec(open(ns["__file__"]).read().split("S = {")[0], ns)
last1, gap, idx = ns["last1"], ns["gap_mtd"], ns["idx"]
v = lib.get("VIXY").reindex(idx)
d = (-np.sign(gap)).where(last1 & v.oc.notna(), 0).fillna(0)
r = lib.register("013_month_end_rebalance", "255 month-end signal via VIXY (short vol when long-equity signal)", -d, v.oc.fillna(0), 5.0)
tt = (-d * v.oc)[d != 0] * 1e4
print(f"255  {lib.fmt(r)}  gate={lib.passes_gate(r)}  per-trade train {tt.loc[:'2014'].mean():+.1f} (2011-14) valid {tt.loc['2015':'2022'].mean():+.1f}")
print("registry size:", lib.n_tests())
