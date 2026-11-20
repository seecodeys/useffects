"""293 - POST-HOC (from 287's segment table): 013 signal held 09:30->12:00 only on the SPX CFD (half the exposure time).
Train 2010-14 / valid 2015-22 only; no holdout look (month-end family has had ~9 already). 0.5bp/side."""
import os, sys, contextlib, io, importlib
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
sys.path.insert(0, os.path.join(lib.HERE, "experiments"))
fx = importlib.import_module("003_overnight_fx")
ns = {"__file__": os.path.join(lib.HERE, "experiments", "013_month_end_rebalance.py")}
with contextlib.redirect_stdout(io.StringIO()):
    exec(open(ns["__file__"]).read().split("S = {")[0], ns)
last1, gap = ns["last1"], ns["gap_mtd"]
px = fx.ny_minute_close("SPXUSD", range(2010, 2026))
D = last1[last1].index; D = D[(D >= "2010-01-01") & (D <= "2022-12-31")]
L = lambda h, m: fx.level_at(px, D, h, m, tol_min=5)
d = np.sign(-gap.reindex(D))
for name, a, b in [("293 013 morning only 09:30-12:00", (9, 30), (12, 0)), ("293 013 full session 09:30-16:00 (CFD, same days)", (9, 30), (16, 0))]:
    ret = (L(*b) / L(*a) - 1) * d - 1e-4
    spy = lib.get("SPY"); full = pd.Series(0.0, index=spy.index); full.loc[ret.dropna().index] = ret.dropna()
    r = lib.register("013_month_end_rebalance", name, pd.Series(1.0, index=full.index), full.loc["2010":"2022"], 0.0, notes="post-hoc (287)")
    t = ret.dropna() * 1e4
    print(f"{name:<52} {lib.fmt(r)}  per-trade {t.mean():+.1f}bp  per-trade Sharpe-like {t.mean()/t.std():.3f}")
print("registry size:", lib.n_tests())
