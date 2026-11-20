"""024 - FOMC announcement days (scheduled meetings only; dates from federalreserve.gov in tools/fomc_dates.csv).
  H1 long SPY open->close on FOMC day        H2 long QQQ on FOMC day
  H3 long SPY open->close on the day BEFORE FOMC (pre-FOMC drift, Lucca-Moench)
  H4 long TLT on FOMC day
0.5bp/side for SPY/QQQ, 1bp for TLT.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
f = pd.read_csv(os.path.join(lib.HERE, "tools", "fomc_dates.csv"), parse_dates=["date"])
fomc = set(f[f.type == "scheduled"].date)
for name, sym, shift, c in [("H1 long SPY on FOMC day", "SPY", 0, 0.5), ("H2 long QQQ on FOMC day", "QQQ", 0, 0.5),
                            ("H3 long SPY day before FOMC", "SPY", -1, 0.5), ("H4 long TLT on FOMC day", "TLT", 0, 1.0)]:
    d = lib.get(sym).loc["2000":]
    on = pd.Series(d.index.isin(fomc), index=d.index)
    pos = on.shift(shift, fill_value=False).astype(float) if shift else on.astype(float)
    r = lib.register("024_fomc", f"024 {name}", pos, d.oc, c)
    print(f"{name:<30} n={int(pos.sum())}  {lib.fmt(r)}  gate={lib.passes_gate(r)}")
print("registry size:", lib.n_tests())
