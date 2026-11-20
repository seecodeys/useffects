"""013g - Both sides of the rebalancing flow as a pair: on the last trading day, pos = -sign(SPY MTD - TLT MTD) in the
SPY-minus-TLT spread (long SPY/short TLT when stocks lagged, the reverse when stocks led), open->close, equal dollars,
0.5bp SPY + 1bp TLT per side."""
import os, sys, contextlib, io
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
ns = {"__file__": os.path.join(lib.HERE, "experiments", "013_month_end_rebalance.py")}
with contextlib.redirect_stdout(io.StringIO()):
    exec(open(ns["__file__"]).read().split("S = {")[0], ns)
s, t, last1, gap = ns["s"], ns["t"], ns["last1"], ns["gap_mtd"]
d = (-np.sign(gap)).where(last1, 0).fillna(0)
net = d * (s.oc - t.oc) - d.abs() * 2 * (0.5 + 1.0) / 1e4
r = (lib.split_eval(pd.Series(1.0, index=net.index), net, 0.0) if "--holdout" in sys.argv else lib.register("013_month_end_rebalance", "013g SPY-TLT spread on last day, -sign(stock-bond MTD gap)", pd.Series(1.0, index=net.index), net, 0.0))
tt = (d * (s.oc - t.oc))[last1] * 1e4
print(f"013g  {lib.fmt(r)}  per-trade bp train {tt.loc[:'2014'].mean():+.1f} valid {tt.loc['2015':'2022'].mean():+.1f}")
ok = r["train_t"] >= 2 and r["valid_t"] >= 2 and r["valid_sharpe"] >= 0.5 and tt.loc['2015':'2022'].mean() >= 1
print("gate (per-trade):", ok)
if ok and "--holdout" in sys.argv:
    h = lib.holdout_eval("013_month_end_rebalance", "013g SPY-TLT spread month-end", pd.Series(1.0, index=net.index), net, 0.0)
    print("HOLDOUT", {k: round(float(v), 2) for k, v in h.items()}, " per-trade", round(tt.loc["2023":].mean(), 1), "bp hit", round(100 * (tt.loc["2023":] > 0).mean()))
print("registry size:", lib.n_tests())
