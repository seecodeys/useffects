"""139 - Portfolio pumping / quarter-end marking (Carhart-Kaniel-Musto-Reed 2002): fund managers push up small-cap
holdings into the quarter-end close, which reverses next morning. IWM-SPY spread (1bp/side/leg):
  a) long IWM / short SPY open->close on the last trading day of each quarter
  b) short IWM / long SPY from that close to the next open (overnight reversal)
  c) same as (a)+(b) at ordinary month-ends (control: pumping should be strongest at quarter/year-ends)
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
spy, iwm = lib.get("SPY"), lib.get("IWM")
idx = spy.index.intersection(iwm.index); idx = idx[idx >= "2000-06-01"]
ym = idx.to_period("M")
rb = pd.Series(1, index=idx)[::-1].groupby(ym[::-1]).cumsum()[::-1]
last = (rb == 1) & pd.Series(ym != ym[-1], index=idx)
qend = last & pd.Series(idx.month % 3 == 0, index=idx)
mend = last & ~qend
oc_spread = (iwm.oc - spy.oc).reindex(idx)
co = lambda d: (d["Open"].shift(-1) / d["Close"] - 1)
on_spread = (co(iwm) - co(spy)).reindex(idx)          # close t -> open t+1, booked on day t
cost = 4 * 1.0 / 1e4
for name, mask, ser, sign in ([] if "--flip" in sys.argv else [("139a long IWM-SPY open->close, quarter-end day", qend, oc_spread, 1),
                              ("139b short IWM-SPY quarter-end close->next open", qend, on_spread, -1),
                              ("139c control: long IWM-SPY open->close, other month-ends", mend, oc_spread, 1)]):
    net = (sign * ser - cost).where(mask, 0).fillna(0)
    r = lib.register("139_portfolio_pumping", name, pd.Series(1.0, index=idx), net, 0.0)
    t = (sign * ser)[mask] * 1e4
    print(f"{name:<54} {lib.fmt(r)}  gate={lib.passes_gate(r)}  gross bp/trade by era:",
          t.groupby((t.index.year // 10) * 10).mean().round(1).to_dict())
print("registry size:", lib.n_tests())

if "--flip" in sys.argv:
    net = (on_spread - cost).where(qend, 0).fillna(0)
    r = lib.register("139_portfolio_pumping", "139d long IWM-SPY quarter-end close->next open (direction from train)", pd.Series(1.0, index=idx), net, 0.0)
    print(f"139d  {lib.fmt(r)}  gate={lib.passes_gate(r)}")
