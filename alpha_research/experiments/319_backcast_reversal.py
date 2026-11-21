"""319 - Does the month-end bond bid reverse in the first 3 sessions of the next month? Back-cast on FRED yields
(DGS10, DGS2) by decade (descriptive). 281 found a reversal in 2002-14 that faded in 2015-22. A persistent reversal would
support a temporary price-pressure (flow) reading; no reversal suggests a permanent level effect or offsetting flows."""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
for s in ["DGS10", "DGS2"]:
    y = pd.read_csv(os.path.join(lib.DATA, f"fred_{s}.csv"), parse_dates=["observation_date"], index_col=0, na_values=".").iloc[:, 0].dropna()
    dy = y.diff().dropna() * 100
    ym = pd.Series(dy.index.to_period("M"), index=dy.index)
    first = pd.Series({m: -v.values[:3].sum() for m, v in dy.groupby(ym) if len(v) >= 15}); first.index = first.index.to_timestamp()
    out = []
    for a, b in [("1973", "1982"), ("1983", "1992"), ("1993", "2001"), ("2002", "2014"), ("2015", "2022")]:
        x = first.loc[a:b]
        out.append(f"{a}-{b[2:]}: {x.mean():+.2f}bp (t={x.mean()/x.std()*np.sqrt(len(x)):.1f})")
    print(f"319 {s} first-3-session yield decline: " + " | ".join(out))
