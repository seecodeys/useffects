"""295 - Replicate 287 on the Nasdaq-100 CFD (HistData NSXUSD 1-min, 2010-22): month-end equity flow (013 signal) signed
bp by intraday segment. Descriptive (not registered)."""
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
px = fx.ny_minute_close("NSXUSD", range(2010, 2023))
D = last1[last1].index; D = D[(D >= "2010-01-01") & (D <= "2022-12-31")]
L = lambda h, m: fx.level_at(px, D, h, m, tol_min=5)
d = np.sign(-gap.reindex(D))
seg = pd.DataFrame({"09:30-12:00": L(12, 0) / L(9, 30) - 1, "12:00-15:00": L(15, 0) / L(12, 0) - 1,
                    "15:00-15:50": L(15, 50) / L(15, 0) - 1, "15:50-16:00": L(16, 0) / L(15, 50) - 1}).mul(d, axis=0).dropna() * 1e4
print(f"295 NSX month-ends {len(seg)}: mean signed bp", seg.mean().round(1).to_dict(), " t", (seg.mean() / seg.std() * np.sqrt(len(seg))).round(2).to_dict())
