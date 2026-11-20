"""303 - Does the month-end equity rebalancing trade (013) scale with real-money positioning? (descriptive, 2009-22)
If asset managers are unusually long equity futures (CFTC TFF ES, z vs 3y, last release before the day), a stocks-beat-
bonds month leaves more equity to sell -> the short-side trades should be stronger; when they're unusually short/
under-weight, the long-side trades. Report 013's signed open->close bp by AM-z tercile, overall and by trade side."""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
exec(open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "299_cftc_positioning.py")).read().split("ES, TY, US =")[0])
spy, tlt = lib.get("SPY"), lib.get("TLT")
idx = spy.index.intersection(tlt.index); idx = idx[idx >= "2002-08-01"]
s, t = spy.loc[idx], tlt.loc[idx]; ym = idx.to_period("M")
pmc = lambda x: x["aC"].groupby(ym).transform("first") / (1 + x["cc"].groupby(ym).transform("first"))
mtd = lambda x: x["aC"].shift(1) / pmc(x) - 1
gap = mtd(s) - mtd(t)
rank_bwd = pd.Series(1, index=idx)[::-1].groupby(ym[::-1]).cumsum()[::-1]
last1 = (rank_bwd == 1) & pd.Series(ym != ym[-1], index=idx)
sig = -np.sign(gap)[last1].loc["2009":"2022"]
pnl = sig * s.oc[sig.index]
zz = lib.asof(z(load("13874A").am).dropna(), sig.index, strict=True)
terc = pd.qcut(zz, 3, labels=False)
for lab, m in [("all", sig != 0), ("short-side (stocks led)", sig < 0), ("long-side (stocks lagged)", sig > 0)]:
    out = []
    for k in range(3):
        x = pnl[m & (terc == k)]
        out.append(f"T{k+1} {1e4*x.mean():+.1f} (n={len(x)})")
    print(f"303 {lab}: " + " | ".join(out))
