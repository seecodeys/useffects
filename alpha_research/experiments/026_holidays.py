"""026 - Holiday-adjacent sessions, SPY open->close (long), 0.5bp/side. ~25 events each -> low power, descriptive.
  day after Thanksgiving (4th Thu of Nov + 1) | Dec 24 session | last trading day of year | first trading day of year
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
d = lib.get("SPY").loc["2000":]
idx = d.index
s = pd.Series(idx, index=idx)
thx = [max(x for x in pd.date_range(f"{y}-11-01", f"{y}-11-30") if x.dayofweek == 3 and x.day >= 22) for y in range(2000, 2027)]
after_thx = s.isin([t + pd.Timedelta(days=1) for t in thx])
dec24 = pd.Series((idx.month == 12) & (idx.day == 24), index=idx)
yr = idx.year
last_y = s.groupby(yr).transform("max") == s
first_y = s.groupby(yr).transform("min") == s
last_y &= idx < pd.Timestamp(f"{idx[-1].year}-12-01")      # current year not finished
for name, m in {"day after Thanksgiving": after_thx, "Dec 24 session": dec24,
                "last trading day of year": last_y, "first trading day of year": first_y}.items():
    r = lib.register("026_holidays", f"SPY long {name}", m.astype(float), d.oc, 0.5)
    print(f"{name:<26} n={int(m.sum()):2d}  {lib.fmt(r)}  gate={lib.passes_gate(r)}")
print("registry size:", lib.n_tests())
