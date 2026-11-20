"""178 - MSCI quarterly/semi-annual index review effective days (close of the last business day of Feb, May, Aug, Nov):
global index funds trade at that close. EEM−SPY and EFA−SPY spreads: (a) open->close on review day, (b) review-day
close -> next open. Direction chosen on TRAIN for each (sign of train mean), evaluated as registered. 1bp/side/leg.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
spy = lib.get("SPY")
for etf in ["EEM", "EFA"]:
    e = lib.get(etf)
    idx = spy.index.intersection(e.index); idx = idx[idx >= "2003-06-01"]
    ym = idx.to_period("M")
    last = (pd.Series(1, index=idx)[::-1].groupby(ym[::-1]).cumsum()[::-1] == 1) & pd.Series(np.isin(idx.month, [2, 5, 8, 11]), index=idx)
    last &= pd.Series(ym != ym[-1], index=idx)
    co = lambda d: (d["Open"].shift(-1) / d["Close"] - 1).reindex(idx)
    legs = {"open->close": (e.oc - spy.oc).reindex(idx), "close->next open": co(e) - co(spy)}
    for lab, ser in legs.items():
        sgn = np.sign(ser[last].loc[:lib.TRAIN[1]].mean())
        net = ((sgn * ser - 4e-4).where(last, 0)).fillna(0)
        r = lib.register("178_msci_review", f"178 {etf}-SPY {lab} on MSCI review day (sign {int(sgn):+d} from train)", pd.Series(1.0, index=idx), net, 0.0)
        print(f"178 {etf} {lab:<16} sign{int(sgn):+d}  {lib.fmt(r)}  gate={lib.passes_gate(r)}  n={int(last.loc[:'2022'].sum())}")
print("registry size:", lib.n_tests())
