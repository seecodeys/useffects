"""311 - Out-of-sample check of the OPEX mechanism (dealer hedge unwinds around expiry) on WEEKLY expiries: SPX weekly
options listed from late 2005, heavily traded from ~2011. If 001/297's OPEX-Friday weakness is expiry-driven, ordinary
(non-monthly) Fridays should show a smaller version. Pre-specified on the SPX CFD, 2011-22 (train 2011-14, valid 15-22):
 A short 10:30->15:00 on non-OPEX Fridays   B short 09:30->16:00 on non-OPEX Fridays
 C control: short 10:30->15:00 on Thursdays (no expiry before 2022's Tue/Thu dailies)."""
import os, sys, importlib
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
sys.path.insert(0, os.path.join(lib.HERE, "experiments"))
fx = importlib.import_module("003_overnight_fx")
px = fx.ny_minute_close("SPXUSD", range(2011, 2023))
spy = lib.get("SPY"); idx = spy.index[(spy.index >= "2011-01-01") & (spy.index <= "2022-12-31")]
fri = idx[(idx.dayofweek == 4) & ~((idx.day >= 15) & (idx.day <= 21))]
thu = idx[idx.dayofweek == 3]
for name, days, a, b in [("A non-OPEX Fri short 10:30-15:00", fri, (10, 30), (15, 0)),
                         ("B non-OPEX Fri short 09:30-16:00", fri, (9, 30), (16, 0)),
                         ("C control Thu short 10:30-15:00", thu, (10, 30), (15, 0))]:
    ret = -(fx.level_at(px, days, *b, tol_min=5) / fx.level_at(px, days, *a, tol_min=5) - 1) - 1e-4
    full = pd.Series(0.0, index=spy.index); full.loc[ret.dropna().index] = ret.dropna()
    r = lib.register("297_opex_midsession", f"311{name}", pd.Series(1.0, index=full.index), full.loc["2011":"2022"], 0.0)
    t = ret.dropna() * 1e4
    print(f"311{name:<36} {lib.fmt(r)}  per-trade {t.mean():+.1f}bp n={len(t)}")
print("registry size:", lib.n_tests())
