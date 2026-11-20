"""253 - Threshold rebalancers: some funds rebalance when the stock-bond allocation drifts past a band, not on a calendar.
Daily running month-to-date gap g = SPY MTD − TLT MTD (through close t). On the FIRST day in a month that |g| > 5%, trade
−sign(g) in SPY on the NEXT day open->close (excluding the month's last day, which is 013). 0.5bp/side. Variant: 4%."""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
spy, tlt = lib.get("SPY"), lib.get("TLT")
idx = spy.index.intersection(tlt.index); idx = idx[idx >= "2002-08-01"]
s, t = spy.loc[idx], tlt.loc[idx]
ym = pd.Series(idx.to_period("M"), index=idx)
lastm = ym != ym.shift(-1)
def mtd_close(x):
    base = x["aC"].where(lastm).shift(1).ffill()
    base = base.where(ym != ym.shift(1)).ffill()
    return x["aC"] / base - 1                      # through close t
g = mtd_close(s) - mtd_close(t)
for thr in [0.05, 0.04]:
    cross = (g.abs() > thr)
    first = cross & ~cross.groupby(ym).cummax().shift(1, fill_value=False).where(ym == ym.shift(1), False)
    sig = (-np.sign(g)).where(first, 0)
    pos = sig.shift(1).fillna(0)                   # trade next day
    pos = pos.where(~lastm, 0)                     # exclude month-end day (013)
    r = lib.register("253_threshold_rebalance", f"253 next day after first |MTD gap|>{int(thr*100)}% in month", pos, s.oc, 0.5)
    print(f"253 thr {int(thr*100)}%  {lib.fmt(r)}  gate={lib.passes_gate(r)}  events={int((pos!=0).loc[:'2022'].sum())}")
print("registry size:", lib.n_tests())
