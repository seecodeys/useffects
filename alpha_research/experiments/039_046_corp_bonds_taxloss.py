"""039 - Corporate-bond index extension at month-end: long HYG / LQD open->close on the last trading day (1bp/side).
046 - Tax-loss selling / January effect in small caps, IWM-SPY spread open->close (1bp/side/leg):
      A short the spread on the last 5 trading days of December   B long the spread on the first 5 trading days of January
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

for sym in ["HYG", "LQD"]:
    d = lib.get(sym)
    idx = d.index; ym = idx.to_period("M")
    rb = pd.Series(1, index=idx)[::-1].groupby(ym[::-1]).cumsum()[::-1]
    last = (rb == 1) & pd.Series(ym != ym[-1], index=idx)
    r = lib.register("039_corp_bond_extension", f"039 {sym} long last trading day", last.astype(float), d.oc, 1.0)
    print(f"039 {sym} long month-end      {lib.fmt(r)}  gate={lib.passes_gate(r)}")

spy, iwm = lib.get("SPY"), lib.get("IWM")
idx = spy.index.intersection(iwm.index)
idx = idx[idx >= "2000-06-01"]
spread = (iwm.oc - spy.oc).reindex(idx)
yr = pd.Series(idx.year, index=idx)
dec = pd.Series(idx.month == 12, index=idx); jan = pd.Series(idx.month == 1, index=idx)
dec_rank = pd.Series(1, index=idx)[dec][::-1].groupby(yr[dec][::-1]).cumsum()[::-1].reindex(idx)
jan_rank = pd.Series(1, index=idx)[jan].groupby(yr[jan]).cumsum().reindex(idx)
A = -(dec_rank <= 5).fillna(False).astype(float)
A[idx.year == idx[-1].year] = 0                        # current December not complete
B = (jan_rank <= 5).fillna(False).astype(float)
for name, pos in {"046A short IWM-SPY last 5 days of Dec": A, "046B long IWM-SPY first 5 days of Jan": B}.items():
    net = pos * spread - pos.abs() * 4 * 1.0 / 1e4
    r = lib.register("046_tax_loss_january", name, pd.Series(1.0, index=idx), net, 0.0, notes="spread; 1bp/side/leg in series")
    print(f"{name:<40} {lib.fmt(r)}  gate={lib.passes_gate(r)}  days={int(pos.abs().sum())}")
print("registry size:", lib.n_tests())
