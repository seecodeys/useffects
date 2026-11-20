"""300 - Mechanism check for the month-end bond bid (descriptive, 2009-22, rule 4b): benchmarked asset managers that hold
duration through Treasury futures must add futures at month-end when the index extends. If that's the channel, the
last-3-session bid should be larger when asset managers' net long in 10y futures (share of OI, as last released before
the window) is high vs its trailing 3-year norm. Terciles of the z-score; IEF and TLT."""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
exec(open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "299_cftc_positioning.py")).read().split("ES, TY, US =")[0])
TY = load("043602"); zam = z(TY.am)
for sym in ["IEF", "TLT"]:
    d = lib.get(sym); idx = d.index; ym = pd.Series(idx.to_period("M"), index=idx)
    bwd = pd.Series(1, index=idx)[::-1].groupby(ym[::-1]).cumsum()[::-1]
    inwin = (bwd <= 3) & (ym != ym.iloc[-1])
    win = d.cc[inwin].groupby(ym).sum().loc["2009-01":"2022-12"]
    first = d.index[inwin].to_series().groupby(ym[inwin]).min()      # first day of the window: last close before it holds
    zz = lib.asof(zam.dropna(), pd.DatetimeIndex(first.values), strict=True)
    zz.index = first.index; zz = zz.reindex(win.index)
    terc = pd.qcut(zz, 3, labels=False)
    out = []
    for t in range(3):
        x = win[terc == t]
        out.append(f"T{t+1} {1e4*x.mean():+.1f} (t={x.mean()/x.std()*np.sqrt(len(x)):.1f}, n={len(x)})")
    print(f"300 {sym} last-3 bp by asset-mgr 10y futures z (2009-22): " + " | ".join(out),
          f"corr {np.corrcoef(zz.dropna(), win[zz.notna()])[0,1]:+.3f}")
