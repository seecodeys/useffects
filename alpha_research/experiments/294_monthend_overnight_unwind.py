"""294 - Does the month-end equity flow unwind overnight? Position −d (opposite of 013's signal) in SPY from the last day's
close to the next session's open (close->open), 0.5bp/side."""
import os, sys, contextlib, io
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
ns = {"__file__": os.path.join(lib.HERE, "experiments", "013_month_end_rebalance.py")}
with contextlib.redirect_stdout(io.StringIO()):
    exec(open(ns["__file__"]).read().split("S = {")[0], ns)
s, last1, gap = ns["s"], ns["last1"], ns["gap_mtd"]
d = (-np.sign(gap)).where(last1, 0).fillna(0)
co_next = s["Open"].shift(-1) / s["Close"] - 1
ser = ((-d) * co_next - d.abs() * 1e-4).shift(1).fillna(0)        # booked on the next day
r = lib.register("013_month_end_rebalance", "294 overnight unwind after month-end (−d, close->next open)", pd.Series(1.0, index=ser.index), ser, 0.0)
tt = ((-d) * co_next)[d != 0] * 1e4
print(f"294  {lib.fmt(r)}  per-trade gross train {tt.loc[:'2014'].mean():+.1f} valid {tt.loc['2015':'2022'].mean():+.1f}")
print("registry size:", lib.n_tests())
