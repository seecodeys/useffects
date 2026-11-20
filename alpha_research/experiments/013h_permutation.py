"""013h - Permutation test of 013 on 2002-2022 (holdout untouched): keep the 245 month-end days and their SPY open->close
returns, randomly reassign the trade signs (preserving the long/short counts) 10,000 times; p = share of shuffles with
mean per-trade P&L >= actual. Also the same for random 'month-end-like' days (any day) to show the day matters."""
import os, sys, contextlib, io
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
ns = {"__file__": os.path.join(lib.HERE, "experiments", "013_month_end_rebalance.py")}
with contextlib.redirect_stdout(io.StringIO()):
    exec(open(ns["__file__"]).read().split("S = {")[0], ns)
s, last1, gap = ns["s"], ns["last1"], ns["gap_mtd"]
m = last1.loc[:"2022"]
d = (-np.sign(gap)).loc[:"2022"][m]; y = s.oc.loc[:"2022"][m]
actual = (d * y).mean()
rng = np.random.default_rng(7)
null = np.array([(rng.permutation(d.values) * y.values).mean() for _ in range(10000)])
print(f"013h actual mean {1e4*actual:+.1f}bp/trade over {len(d)} month-ends; sign-permutation p = {(null >= actual).mean():.4f}")
# same rule on random non-month-end days (gap computed the same way) -> is it the DAY that matters?
alld = (-np.sign(gap)).loc[:"2022"].dropna(); ally = s.oc.loc[:"2022"].reindex(alld.index)
other = alld.index[~m.reindex(alld.index).fillna(False).values]
sims = []
for _ in range(2000):
    pick = rng.choice(other, size=len(d), replace=False)
    sims.append((alld.loc[pick] * ally.loc[pick]).mean())
sims = np.array(sims)
print(f"013h same rule on random NON-month-end days: mean {1e4*sims.mean():+.1f}bp, 95th pct {1e4*np.percentile(sims,95):+.1f}bp; "
      f"share >= actual: {(sims >= actual).mean():.4f}")
