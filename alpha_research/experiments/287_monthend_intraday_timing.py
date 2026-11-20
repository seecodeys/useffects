"""287 - When during the last day does the month-end equity flow (013) hit? HistData SPXUSD 1-min CFD (2010-2025), levels at
09:30 / 12:00 / 15:00 / 15:50 / 16:00 NY. Signed by 013's signal (−sign(SPY MTD − TLT MTD) through the prior close).
Descriptive by segment for 2010-22, plus ONE new registered execution variant: enter 15:00, exit 16:00 (last hour only).
CFD has a small basis vs SPY; costs for the variant 0.5bp/side."""
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
D = last1[last1].index
D = D[(D >= "2010-01-01") & (D <= "2025-12-31")]
lv = {t: fx.level_at(px, D, *t, tol_min=5) for t in [(9, 30), (12, 0), (15, 0), (15, 50), (16, 0)]}
d = np.sign(-gap.reindex(D))
seg = pd.DataFrame({"09:30-12:00": lv[(12, 0)] / lv[(9, 30)] - 1, "12:00-15:00": lv[(15, 0)] / lv[(12, 0)] - 1,
                    "15:00-15:50": lv[(15, 50)] / lv[(15, 0)] - 1, "15:50-16:00": lv[(16, 0)] / lv[(15, 50)] - 1}).mul(d, axis=0) * 1e4
s22 = seg.loc[:"2022"].dropna()
print(f"287 month-ends 2010-22 with data: {len(s22)}")
print("   mean signed bp by segment:", s22.mean().round(1).to_dict())
print("   t by segment:", (s22.mean() / s22.std() * np.sqrt(len(s22))).round(2).to_dict())
last_hour = (lv[(16, 0)] / lv[(15, 0)] - 1) * d
spy = lib.get("SPY")
ser = pd.Series(0.0, index=spy.index)
ser.loc[last_hour.dropna().index] = last_hour.dropna() - 2 * 0.5e-4
r = lib.register("013_month_end_rebalance", "287 013 last hour only (15:00->16:00, SPX CFD timing)", pd.Series(1.0, index=ser.index), ser.loc["2010":], 0.0)
tt = last_hour.dropna() * 1e4
print(f"287 last-hour variant  {lib.fmt(r)}  per-trade 2010-14 {tt.loc[:'2014'].mean():+.1f} 2015-22 {tt.loc['2015':'2022'].mean():+.1f}")
print("registry size:", lib.n_tests())
