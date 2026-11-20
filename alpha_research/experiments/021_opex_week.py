"""021 - OPEX-week structure, SPY & QQQ open->close, 0.5bp/side (pre-specified):
  A long Mon-Thu of monthly-OPEX week (the week containing the 3rd Friday)
  B short the first trading day after monthly OPEX
  C short OPEX Friday, quarterly expiries only (Mar/Jun/Sep/Dec)
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

for sym in ([] if "--holdout" in sys.argv else ["SPY", "QQQ"]):
    d = lib.get(sym).loc["2000":]
    idx = d.index
    third_fri = pd.Series((idx.dayofweek == 4) & (idx.day >= 15) & (idx.day <= 21), index=idx)
    # week containing the 3rd Friday (calendar rule, robust to a holiday on that Friday)
    fri_of_week = idx + pd.to_timedelta(4 - idx.dayofweek, unit="D")
    opex_week = pd.Series((fri_of_week.day >= 15) & (fri_of_week.day <= 21), index=idx)
    A = (opex_week & (idx.dayofweek <= 3)).astype(float)
    B = -third_fri.shift(1, fill_value=False).astype(float)
    C = -(third_fri & (idx.month % 3 == 0)).astype(float)
    for name, pos in {"021A long Mon-Thu of OPEX week": A, "021B short day after OPEX": B,
                      "021C short quarterly OPEX Friday": C}.items():
        r = lib.register("021_opex_week", f"{sym} {name}", pos, d.oc, 0.5)
        print(f"{sym} {name:<34} {lib.fmt(r)}  gate={lib.passes_gate(r)}")
print("registry size:", lib.n_tests())

if "--holdout" in sys.argv:
    d = lib.get("SPY"); idx = d.index
    tf = pd.Series((idx.dayofweek == 4) & (idx.day >= 15) & (idx.day <= 21), index=idx)
    pos = -(tf & (idx.month % 3 == 0)).astype(float)
    for c in [0.5, 2.0]:
        h = lib.holdout_eval("021_opex_week", f"SPY 021C short quarterly OPEX Friday (cost {c}bp)", pos, d.oc, c)
        print("HOLDOUT", c, {k: round(float(v), 2) for k, v in h.items()})
    print((pos * d.oc * 1e4).loc["2023":][pos.loc["2023":] != 0].round(0).astype(int).to_dict())
