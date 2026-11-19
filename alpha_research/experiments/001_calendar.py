"""001 - Calendar baselines for SPY / QQQ / IWM open->close.

Hypotheses (classic, pre-specified - no parameter search):
  * day-of-week: long on each weekday separately
  * turn-of-month (last trading day + first 3): long
  * pre-holiday (day before a weekday market closure): long
  * OPEX (3rd Friday) and the Monday after: long / short
Positions are long-only on signal days (0 otherwise) unless noted.
"""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

EXP = "001_calendar"
rows = []
for sym in ["SPY", "QQQ", "IWM"]:
    d = lib.get(sym)
    d = d.loc["2000":]
    y = d["oc"]
    idx = d.index
    dow = idx.dayofweek
    ym = idx.to_period("M")
    rank_fwd = pd.Series(1, index=idx).groupby(ym).cumsum()
    rank_bwd = pd.Series(1, index=idx)[::-1].groupby(ym[::-1]).cumsum()[::-1]
    nxt = pd.Series(idx, index=idx).shift(-1)
    gap_days = (nxt - pd.Series(idx, index=idx)).dt.days
    # pre-holiday: next trading day is > 1 calendar day away and it's not a normal weekend
    pre_hol = ((gap_days > 1) & ~((dow == 4) & (gap_days == 3))).astype(float)
    third_fri = (dow == 4) & (idx.day >= 15) & (idx.day <= 21)
    signals = {
        **{f"long {n}": pd.Series((dow == i).astype(float), index=idx)
           for i, n in enumerate(["Mon", "Tue", "Wed", "Thu", "Fri"])},
        "long turn-of-month (last+first3)": ((rank_bwd == 1) | (rank_fwd <= 3)).astype(float),
        "long pre-holiday": pre_hol,
        "short OPEX Friday": -pd.Series(third_fri.astype(float), index=idx),
        "always long": pd.Series(1.0, index=idx),
    }
    for name, pos in signals.items():
        r = lib.register(EXP, f"{sym} {name}", pos, y)
        rows.append(r)
        print(f"{sym} {name:<34} {lib.fmt(r)}  gate={lib.passes_gate(r)}")
pd.DataFrame(rows).to_csv(os.path.join(lib.HERE, "results", "001_calendar.csv"), index=False)
print("registry size:", lib.n_tests())
