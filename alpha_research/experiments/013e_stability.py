"""013e - Stability of the month-end signal through time (descriptive, no new test): per-trade P&L of the exact 013
rule (TYX proxy before Aug 2002, TLT after), rolling 36-trade mean and t-stat. Writes results/013_stability.png."""
import os, sys, contextlib, io
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
def run(path, cut):
    ns = {"__file__": path}
    with contextlib.redirect_stdout(io.StringIO()):
        exec(open(path).read().split(cut)[0], ns)
    return ns
b = run(os.path.join(lib.HERE, "experiments", "040_month_end_backcast.py"), "pos = (-np.sign(gap_proxy))")
m = run(os.path.join(lib.HERE, "experiments", "013_month_end_rebalance.py"), "S = {")
spy = b["spy"]
pre = (-np.sign(b["gap_proxy"])).where(b["last"], 0).loc[:"2002-07-31"]
post = (-np.sign(m["gap_mtd"])).where(m["last1"], 0)
pos = pd.concat([pre, post]).sort_index()
pos = pos[~pos.index.duplicated()]
tr = (pos * spy.oc.reindex(pos.index) - 1e-4)[pos != 0] * 1e4
roll_mean = tr.rolling(36).mean()
roll_t = tr.rolling(36).mean() / tr.rolling(36).std() * np.sqrt(36)
fig, ax = plt.subplots(2, 1, figsize=(10, 6), sharex=True)
ax[0].bar(tr.index, tr.values, width=20, color=np.where(tr > 0, "#2a7", "#c44")); ax[0].set_ylabel("bp per trade")
ax[0].set_title("Month-end rebalancing rule: per-trade P&L (SPY open->close, 0.5bp/side)")
ax[1].plot(roll_t, color="k"); ax[1].axhline(2, ls="--", color="grey"); ax[1].axhline(0, color="grey", lw=0.6)
ax[1].set_ylabel("rolling 36-trade t-stat")
for a in ax:
    for x in ["2002-08-01", "2015-01-01", "2023-01-01"]:
        a.axvline(pd.Timestamp(x), color="steelblue", ls=":", lw=1)
fig.tight_layout(); fig.savefig(os.path.join(lib.HERE, "results", "013_stability.png"), dpi=130)
by = tr.groupby((tr.index.year // 5) * 5).agg(["mean", "count"])
by["t"] = tr.groupby((tr.index.year // 5) * 5).apply(lambda x: x.mean() / x.std() * np.sqrt(len(x)))
print(by.round(2).to_string())
