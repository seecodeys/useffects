"""309 - Execution check for 013 (descriptive, 2010-22, not registered): is the month-end edge an opening-print artifact
like 004/006 (015)? Enter at 09:30 / 09:35 / 09:45 / 10:00 on the SPX CFD, exit 16:00, signed by 013's signal.
Also NSX (Nasdaq-100 CFD)."""
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
D = last1[last1].index; D = D[(D >= "2010-01-01") & (D <= "2022-12-31")]
d = np.sign(-gap.reindex(D))
for sym in ["SPXUSD", "NSXUSD"]:
    px = fx.ny_minute_close(sym, range(2010, 2023))
    end = fx.level_at(px, D, 16, 0, tol_min=5)
    out = []
    for h, m in [(9, 30), (9, 35), (9, 45), (10, 0)]:
        t = ((end / fx.level_at(px, D, h, m, tol_min=5) - 1) * d * 1e4).dropna()
        out.append(f"{h:02d}:{m:02d} {t.mean():+.1f}bp (t={t.mean()/t.std()*np.sqrt(len(t)):.1f})")
    print(f"309 {sym}: " + " | ".join(out), f"n={len(t)}")
