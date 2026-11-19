"""013b - Concentration check + single holdout look for 013 H1."""
import os, sys, importlib
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
import contextlib, io
with contextlib.redirect_stdout(io.StringIO()):
    sys.argv = ["x"]
    src = open(os.path.join(lib.HERE, "experiments", "013_month_end_rebalance.py")).read().split("for name, (pos, y, c)")[0]
    exec(src)
pos = sig.where(last1, 0)
pnl = lib.daily_pnl(pos, s.oc, 0.5)
yr = (pnl.groupby(pnl.index.year).sum() * 100).loc[:2022]
print("yearly %:", yr.round(1).to_dict())
print("positive years:", (yr > 0).sum(), "/", len(yr), " total:", round(yr.sum(), 1), " ex best 3:", round(yr.sort_values().iloc[:-3].sum(), 1))
tr = (np.sign(pos) == np.sign(s.oc))[pos != 0]
print("by |gap| tercile (train+valid) mean bp:")
k = pd.DataFrame({"g": gap_mtd[last1].abs(), "r": (pos * s.oc)[last1] * 1e4}).loc[:"2022"]
print(k.groupby(pd.qcut(k.g, 3, labels=["small", "mid", "large"]), observed=True).r.agg(["mean", "count"]).round(1).to_string())
for c in [0.5, 2.0]:
    h = lib.holdout_eval("013_month_end_rebalance", f"H1 SPY last day of month fade MTD gap (cost {c}bp) [rerun after bugfix: incomplete month excluded]", pos, s.oc, c)
    print("HOLDOUT", c, {k_: round(float(v), 2) for k_, v in h.items()})
hp = (pos * s.oc * 1e4).loc["2023":][last1.loc["2023":]]
print(hp.round(0).astype(int).to_string())
