"""264 - Pooled international month-end rule (257) with the 168 volatility filter: trade a country only when its equity
ETF's 20d vol (prior close) > its own 2009-14 median (Canada: 2001-14). Train/valid robustness only — no holdout look."""
import os, sys, importlib
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
sys.path.insert(0, os.path.join(lib.HERE, "experiments"))
sys.argv = [sys.argv[0], "--holdout-canada-skip"]
m = importlib.import_module("257_intl_monthend")
P = {}
for name, (eq, bd) in m.PAIRS.items():
    pos, y = m.country(eq, bd)
    v = lib.get(eq).cc.rolling(20).std().shift(1).reindex(pos.index)
    med = v.loc[:"2014"].median()
    pos = pos.where(v > med, 0)
    P[name] = lib.daily_pnl(pos, y, 2.0)
Pdf = pd.DataFrame(P)
act = Pdf.ne(0).sum(axis=1).replace(0, np.nan)
pooled = (Pdf.fillna(0).sum(axis=1) / act).fillna(0)
r = lib.register("257_intl_monthend", "264 pooled international month-end + high-vol filter", pd.Series(1.0, index=pooled.index), pooled, 0.0)
print(f"264  {lib.fmt(r)}")
print("registry size:", lib.n_tests())
