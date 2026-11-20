"""027 - What drives 013? Equity leg vs bond leg, last trading day, SPY open->close (train+valid only).
  L1 pos = -sign(SPY MTD)          (equity-only rebalancing proxy)
  L2 pos = +sign(TLT MTD)          (bond-only proxy: bonds up -> funds sell bonds / buy stocks)
  Regression (HAC) on last days: oc ~ SPY_mtd + TLT_mtd, 2002-2022.
  Disagreement subset: days where -sign(SPY_mtd) != -sign(gap) — which one wins?
"""
import os, sys, contextlib, io
import numpy as np, pandas as pd, statsmodels.api as sm
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
ns = {"__file__": os.path.join(lib.HERE, "experiments", "013_month_end_rebalance.py")}
with contextlib.redirect_stdout(io.StringIO()):
    exec(open(ns["__file__"]).read().split("S = {")[0], ns)
s, t, last1, mtd, gap = ns["s"], ns["t"], ns["last1"], ns["mtd"], ns["gap_mtd"]
ms, mt = mtd(s), mtd(t)
for name, pos in ({} if "--diag" in sys.argv else {"027 L1 last day: -sign(SPY MTD) only": (-np.sign(ms)).where(last1, 0),
                  "027 L2 last day: +sign(TLT MTD) only": (np.sign(mt)).where(last1, 0)}).items():
    r = lib.register("013_month_end_rebalance", name, pos.fillna(0), s.oc, 0.5)
    print(f"{name:<40} {lib.fmt(r)}")
X = pd.DataFrame({"y": s.oc, "spy_mtd": ms, "tlt_mtd": mt})[last1].loc[:"2022"].dropna()
m = sm.OLS(X.y, sm.add_constant(X[["spy_mtd", "tlt_mtd"]])).fit(cov_type="HAC", cov_kwds={"maxlags": 1})
print("HAC regression 2002-22 on last days (n=%d):" % len(X)); print(pd.DataFrame({"coef": m.params, "t": m.tvalues}).round(3).to_string())
dis = last1 & (np.sign(ms) != np.sign(gap))
D = pd.DataFrame({"y": s.oc, "g": gap})[dis].loc[:"2022"]
print(f"disagreement days {len(D)}: following gap rule earns {1e4*(-np.sign(D.g)*D.y).mean():+.1f}bp/trade "
      f"(hit {100*((-np.sign(D.g)*D.y)>0).mean():.0f}%)")
print("registry size:", lib.n_tests())
