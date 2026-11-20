"""297 - POST-HOC refinement of 001 from 292's segment table: short the SPX CFD 10:30->15:00 on monthly OPEX Fridays
(after the AM settlement hour, before the close). Train 2010-14 / valid 2015-22 only; no holdout look. 0.5bp/side."""
import os, sys, importlib
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
sys.path.insert(0, os.path.join(lib.HERE, "experiments"))
fx = importlib.import_module("003_overnight_fx")
px = fx.ny_minute_close("SPXUSD", range(2010, 2023))
spy = lib.get("SPY"); idx = spy.index[(spy.index >= "2010-01-01") & (spy.index <= "2022-12-31")]
opex = idx[(idx.dayofweek == 4) & (idx.day >= 15) & (idx.day <= 21)]
for name, a, b in [("297 OPEX Fri short 10:30-15:00 (post-hoc)", (10, 30), (15, 0)), ("297 OPEX Fri short 09:30-16:00 (CFD baseline)", (9, 30), (16, 0))]:
    ret = -(fx.level_at(px, opex, *b, tol_min=5) / fx.level_at(px, opex, *a, tol_min=5) - 1) - 1e-4
    full = pd.Series(0.0, index=spy.index); full.loc[ret.dropna().index] = ret.dropna()
    r = lib.register("297_opex_midsession", name, pd.Series(1.0, index=full.index), full.loc["2010":"2022"], 0.0, notes="post-hoc (292)")
    t = ret.dropna() * 1e4
    print(f"{name:<46} {lib.fmt(r)}  per-trade {t.mean():+.1f}bp mean/sd {t.mean()/t.std():.3f}")
print("registry size:", lib.n_tests())
