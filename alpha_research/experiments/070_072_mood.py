"""070 - Daylight-saving-time weekends (Kamstra-Kramer-Levi 'losing sleep'): short SPY open->close on the Monday
      after a DST change (spring-forward and fall-back). US rules: 2000-06 first Sun Apr / last Sun Oct; 2007+ second
      Sun Mar / first Sun Nov.
072 - Lunar phase (Yuan-Zheng-Zhu): long SPY open->close in the 7 days around new moon (+-3d), short around full moon.
SPY, 0.5bp/side.
"""
import os, sys, math
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
d = lib.get("SPY").loc["2000":]
idx = d.index


def nth_sunday(y, m, n):
    days = [x for x in pd.date_range(f"{y}-{m:02d}-01", periods=31) if x.month == m and x.dayofweek == 6]
    return days[n] if n >= 0 else days[-1]
dst = []
for y in range(2000, 2027):
    if y < 2007:
        dst += [nth_sunday(y, 4, 0), nth_sunday(y, 10, -1)]
    else:
        dst += [nth_sunday(y, 3, 1), nth_sunday(y, 11, 0)]
mondays = set()
for s in dst:
    nxt = idx[idx > s]
    if len(nxt):
        mondays.add(nxt[0])
pos = -pd.Series(idx.isin(list(mondays)), index=idx).astype(float)
r = lib.register("070_dst", "070 short SPY first session after DST change", pos, d.oc, 0.5)
print(f"070 DST Monday short   {lib.fmt(r)}  gate={lib.passes_gate(r)}  n={int(-pos.sum())}")

ref_new = pd.Timestamp("2000-01-06 18:14")     # known new moon (UTC)
syn = 29.530588853
age = ((idx - ref_new).total_seconds() / 86400.0) % syn          # days since new moon
near_new = (age <= 3) | (age >= syn - 3)
near_full = (abs(age - syn / 2) <= 3)
pos = pd.Series(np.where(near_new, 1.0, np.where(near_full, -1.0, 0.0)), index=idx)
r = lib.register("072_lunar", "072 long new-moon week / short full-moon week", pos, d.oc, 0.5)
print(f"072 lunar              {lib.fmt(r)}  gate={lib.passes_gate(r)}")
print("registry size:", lib.n_tests())
