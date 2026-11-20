"""310 - Where in the day does the month-end bond bid accrue? (descriptive, 2002-22, not registered). Split each of the
last-3 sessions' close->close return of IEF/TLT into overnight (prev close->open) and intraday (open->close).
Practical: tells whether entering at an open (MOO) instead of a close (MOC) loses part of it."""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
for sym in ["IEF", "TLT"]:
    d = lib.get(sym).loc["2002-08":"2022"]; idx = d.index; ym = pd.Series(idx.to_period("M"), index=idx)
    bwd = pd.Series(1, index=idx)[::-1].groupby(ym[::-1]).cumsum()[::-1]
    ov = d.aO / d.aC.shift(1) - 1; intr = d.aC / d.aO - 1
    out = []
    for k in [3, 2, 1]:
        m = bwd == k
        f = lambda s: f"{1e4*s[m].mean():+.1f} (t={s[m].mean()/s[m].std()*np.sqrt(m.sum()):.1f})"
        out.append(f"day-{k}: overnight {f(ov)} intraday {f(intr)}")
    allov, allin = 1e4 * ov.mean(), 1e4 * intr.mean()
    print(f"310 {sym}: " + " | ".join(out) + f" | all days ov {allov:+.1f} id {allin:+.1f}")
