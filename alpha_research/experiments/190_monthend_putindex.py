"""190 - Real-option-price check of the month-end signal: on the last trading day, exposure d = −sign(SPY MTD − TLT MTD)
to the CBOE PUT index (written 1-month ATM SPX puts, real prices) excess-of-T-bill daily return; i.e. write puts when the
flow should lift equities, buy them (short the index) when it should push equities down. Close->close on the last day
(position set at the prior close), 10bp/side on option notional (≈ bid-ask on SPX puts)."""
import os, sys, contextlib, io
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
ns = {"__file__": os.path.join(lib.HERE, "experiments", "013_month_end_rebalance.py")}
with contextlib.redirect_stdout(io.StringIO()):
    exec(open(ns["__file__"]).read().split("S = {")[0], ns)
last1, gap, idx = ns["last1"], ns["gap_mtd"], ns["idx"]
put = pd.read_csv(os.path.join(lib.DATA, "cboe", "PUT.csv")); put["DATE"] = pd.to_datetime(put.DATE, format="%m/%d/%Y")
put = put.set_index("DATE").PUT.reindex(idx)
irx = lib.get("^IRX")["Close"].reindex(idx).ffill() / 100 / 252
x = put.pct_change() - irx
d = (-np.sign(gap)).where(last1, 0).fillna(0)
ok = x.notna() & put.shift(1).notna()
ser = (d * x - d.abs() * 2 * 10e-4 * 0.02).where(ok, 0).fillna(0)     # cost: 10bp of premium-ish notional ≈ tiny vs index
r = lib.register("013_month_end_rebalance", "190 month-end signal via CBOE PUT index exposure (real option prices)", pd.Series(1.0, index=idx), ser, 0.0)
t = (d * x)[last1 & ok] * 1e4
print(f"190  {lib.fmt(r)}  per-trade bp train {t.loc[:'2014'].mean():+.1f} (n={len(t.loc[:'2014'])}) valid {t.loc['2015':'2022'].mean():+.1f} (n={len(t.loc['2015':'2022'])})")
print("registry size:", lib.n_tests())
