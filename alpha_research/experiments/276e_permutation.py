"""276e - Permutation test for the month-end bond bid (2002-22; descriptive): compare the mean 3-session return over the
last 3 sessions of each month with the same statistic for a randomly placed 3-session window in each month (5,000 draws)."""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
rng = np.random.default_rng(11)
for sym in ["IEF", "TLT"]:
    d = lib.get(sym); r = d.cc.loc["2002-08":"2022-12-31"]
    ym = pd.Series(r.index.to_period("M"), index=r.index)
    groups = [g.values for _, g in r.groupby(ym)]
    actual = np.mean([g[-3:].sum() for g in groups])
    sims = np.array([np.mean([g[s:s + 3].sum() for g, s in zip(groups, [rng.integers(0, len(g) - 3) for g in groups])]) for _ in range(5000)])
    print(f"276e {sym}: actual {1e4*actual:+.1f}bp per month-end window; random windows mean {1e4*sims.mean():+.1f}bp, "
          f"99th pct {1e4*np.percentile(sims, 99):+.1f}bp; share >= actual {(sims >= actual).mean():.4f}")
