"""031 - Treasury index-extension buying at month-end: long TLT / IEF open->close on the last trading day (1bp/side).
032 - VIX expiration (Wednesday 30 days before the next month's 3rd-Friday SPX expiry): SPY open->close long/short?
      Pre-specified as SHORT (dealer hedge unwind after AM SOQ, mirroring 001's OPEX-Friday short). 0.5bp/side.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

for sym in ["TLT", "IEF"]:
    d = lib.get(sym).loc["2002-08-01":]
    idx = d.index; ym = idx.to_period("M")
    rb = pd.Series(1, index=idx)[::-1].groupby(ym[::-1]).cumsum()[::-1]
    last = (rb == 1) & pd.Series(ym != ym[-1], index=idx)
    r = lib.register("031_bond_index_extension", f"{sym} long last trading day of month", last.astype(float), d.oc, 1.0)
    print(f"031 {sym} long month-end   {lib.fmt(r)}  gate={lib.passes_gate(r)}")

spy = lib.get("SPY").loc["2006-01-01":]
idx = spy.index
def third_fri(y, m):
    x = pd.Timestamp(y, m, 15); return x + pd.Timedelta(days=(4 - x.dayofweek) % 7)
vix_exp = []
for y in range(2006, 2027):
    for m in range(1, 13):
        n = pd.Timestamp(y, m, 1) + pd.offsets.MonthBegin(1)
        w = third_fri(n.year, n.month) - pd.Timedelta(days=30)
        while w not in idx and w > idx[0] and w < idx[-1]:
            w -= pd.Timedelta(days=1)                     # holiday -> prior business day
        vix_exp.append(w)
mask = pd.Series(idx.isin(vix_exp), index=idx)
r = lib.register("032_vix_expiry", "SPY short VIX-expiration Wednesday", -mask.astype(float), spy.oc, 0.5)
print(f"032 SPY short VIX expiry   {lib.fmt(r)}  gate={lib.passes_gate(r)}  n={int(mask.sum())}")
print("registry size:", lib.n_tests())
