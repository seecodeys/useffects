"""302 - Month-end bond bid by month type (descriptive, 2002-22, rule 4b): ordinary vs quarter-end vs year-end months.
Quarter-ends add balance-sheet/window-dressing demand for Treasuries; year-end has the largest index extension."""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
for sym in ["IEF", "TLT", "SHY"]:
    d = lib.get(sym); idx = d.index; ym = pd.Series(idx.to_period("M"), index=idx)
    bwd = pd.Series(1, index=idx)[::-1].groupby(ym[::-1]).cumsum()[::-1]
    win = d.cc[(bwd <= 3) & (ym != ym.iloc[-1])].groupby(ym).sum().loc["2002-08":"2022-12"]
    mo = win.index.month
    out = []
    for lab, m in [("ordinary", ~np.isin(mo, [3, 6, 9, 12])), ("Mar/Jun/Sep", np.isin(mo, [3, 6, 9])), ("December", mo == 12)]:
        x = win[m]; out.append(f"{lab} {1e4*x.mean():+.1f} (t={x.mean()/x.std()*np.sqrt(len(x)):.1f}, n={len(x)})")
    print(f"302 {sym}: " + " | ".join(out))
