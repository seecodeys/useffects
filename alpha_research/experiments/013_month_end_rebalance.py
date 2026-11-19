"""013 - Month-end rebalancing flows. If stocks beat bonds month-to-date, balanced
funds sell stocks into month-end (and buy when stocks lagged).
gap_mtd = SPY month-to-date return - TLT month-to-date return, through the PRIOR close.
  H1 last trading day of month:      pos_SPY = -sign(gap_mtd)
  H2 last 2 trading days of month:   pos_SPY = -sign(gap_mtd)
  H3 quarter-end last trading day only
Also the mirror on TLT for H1. SPY 0.5bp/side, TLT 1bp/side. Data from 2002-08 (TLT start).
"""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

EXP = "013_month_end_rebalance"
spy, tlt = lib.get("SPY"), lib.get("TLT")
idx = spy.index.intersection(tlt.index)
idx = idx[idx >= "2002-08-01"]
s, t = spy.loc[idx], tlt.loc[idx]
ym = idx.to_period("M")
prev_month_end_close = lambda x: x["aC"].groupby(ym).transform("first") / (1 + x["cc"].groupby(ym).transform("first"))
mtd = lambda x: x["aC"].shift(1) / prev_month_end_close(x) - 1           # through prior close
gap_mtd = (mtd(s) - mtd(t))
# first day of month: prior close IS last month's close -> mtd = 0 ; fine (no trade then anyway)
rank_bwd = pd.Series(1, index=idx)[::-1].groupby(ym[::-1]).cumsum()[::-1]
complete = pd.Series(ym != ym[-1], index=idx)   # current month unfinished -> its 'last day' isn't month-end
last1, last2 = (rank_bwd == 1) & complete, (rank_bwd <= 2) & complete
qend = last1 & (idx.month % 3 == 0)
sig = -np.sign(gap_mtd)
S = {"H1 SPY last day of month: fade stock-bond MTD gap": (sig.where(last1, 0), s.oc, 0.5),
     "H2 SPY last 2 days: fade stock-bond MTD gap": (sig.where(last2, 0), s.oc, 0.5),
     "H3 SPY quarter-end last day: fade gap": (sig.where(qend, 0), s.oc, 0.5),
     "H1-mirror TLT last day of month: follow stock-bond MTD gap": (-sig.where(last1, 0), t.oc, 1.0)}
for name, (pos, y, c) in S.items():
    r = lib.register(EXP, name, pos, y, c)
    print(f"{name:<58} {lib.fmt(r)}  gate={lib.passes_gate(r)}")
print("sanity: corr(gap_mtd on last day, SPY MTD-TLT MTD computed directly) skipped; mean |gap_mtd| last day:",
      round(gap_mtd[last1].abs().mean() * 100, 2), "%")
print("registry size:", lib.n_tests())
