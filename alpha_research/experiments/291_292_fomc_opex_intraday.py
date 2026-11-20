"""291 - FOMC 14:00 announcement: does the first 30-minute reaction (14:00->14:30) continue or reverse into the close?
      Direction chosen on TRAIN (2010-14); hold 14:30->16:00 on the SPX CFD, 0.5bp/side. Scheduled FOMC days only.
292 - Intraday timing of the OPEX-Friday weakness (001), descriptive 2010-22: short-signed bp by segment on monthly OPEX Fridays."""
import os, sys, importlib
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
sys.path.insert(0, os.path.join(lib.HERE, "experiments"))
fx = importlib.import_module("003_overnight_fx")
px = fx.ny_minute_close("SPXUSD", range(2010, 2026))
L = lambda dates, h, m: fx.level_at(px, dates, h, m, tol_min=5)
f = pd.read_csv(os.path.join(lib.HERE, "tools", "fomc_dates.csv"), parse_dates=["date"])
D = pd.DatetimeIndex(sorted(d for d in f[f.type == "scheduled"].date if pd.Timestamp("2010-01-01") <= d <= pd.Timestamp("2025-12-31")))
first = L(D, 14, 30) / L(D, 14, 0) - 1
rest = L(D, 16, 0) / L(D, 14, 30) - 1
sgn = np.sign((np.sign(first) * rest).loc[:"2014"].mean())
ser = (sgn * np.sign(first) * rest - 1e-4).dropna()
spy = lib.get("SPY"); full = pd.Series(0.0, index=spy.index); full.loc[ser.index.intersection(full.index)] = ser
r = lib.register("291_fomc_intraday", f"291 FOMC: {'follow' if sgn > 0 else 'fade'} 14:00-14:30 move until close (dir from train)", pd.Series(1.0, index=full.index), full.loc["2010":], 0.0)
print(f"291 dir {'follow' if sgn>0 else 'fade'}  {lib.fmt(r)}  n={len(ser.loc[:'2022'])}")

idx = spy.index[(spy.index >= "2010-01-01") & (spy.index <= "2022-12-31")]
opex = idx[(idx.dayofweek == 4) & (idx.day >= 15) & (idx.day <= 21)]
segs = {"09:30-10:30": ((9, 30), (10, 30)), "10:30-12:00": ((10, 30), (12, 0)), "12:00-15:00": ((12, 0), (15, 0)), "15:00-16:00": ((15, 0), (16, 0))}
out = {k: (-(L(opex, *b) / L(opex, *a) - 1) * 1e4).dropna() for k, (a, b) in segs.items()}
print("292 OPEX Friday short-signed bp by segment 2010-22:", {k: (round(v.mean(), 1), round(v.mean() / v.std() * np.sqrt(len(v)), 2)) for k, v in out.items()}, " n", len(opex))
print("registry size:", lib.n_tests())
