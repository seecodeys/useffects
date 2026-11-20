"""252 - Month-end rebalancing flow at the stock level: index-wide buying/selling should move high-beta stocks more.
On the last trading day, d = −sign(SPY MTD − TLT MTD); position d × (top-quintile-beta − bottom-quintile-beta) large caps
(252d beta vs SPY at the prior close; price ≥ $20), open->close, equal weight, 2bp/side/leg. Survivorship-biased universe."""
import os, sys, contextlib, io, importlib
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
sys.path.insert(0, os.path.join(lib.HERE, "experiments"))
U = importlib.import_module("006_stock_gap_reversal").UNIVERSE
ns = {"__file__": os.path.join(lib.HERE, "experiments", "013_month_end_rebalance.py")}
with contextlib.redirect_stdout(io.StringIO()):
    exec(open(ns["__file__"]).read().split("S = {")[0], ns)
s, last1, gap, idx = ns["s"], ns["last1"], ns["gap_mtd"], ns["idx"]
d = (-np.sign(gap)).where(last1, 0).fillna(0)
OC, CC, PX = {}, {}, {}
for sym in U:
    try:
        x = lib.get(sym)
    except Exception:
        continue
    OC[sym], CC[sym], PX[sym] = x.oc, x.cc, x.Close
OC, CC, PX = (pd.DataFrame(v).reindex(idx) for v in (OC, CC, PX))
m = s.cc.reindex(idx)
beta = CC.rolling(252).cov(m).div(m.rolling(252).var(), axis=0).shift(1).where(PX.shift(1) >= 20)
rk = beta.rank(axis=1, pct=True)
hi, lo = rk.gt(0.8), rk.le(0.2)
spread = (OC.where(hi).mean(axis=1) - OC.where(lo).mean(axis=1))
ser = (d * spread - d.abs() * 2 * 2 * 2e-4).fillna(0)
r = lib.register("013_month_end_rebalance", "252 month-end signal in high-minus-low beta large-cap spread", pd.Series(1.0, index=idx), ser, 0.0)
tt = (d * spread)[d != 0] * 1e4
print(f"252  {lib.fmt(r)}  per-trade gross train {tt.loc[:'2014'].mean():+.1f} valid {tt.loc['2015':'2022'].mean():+.1f}")
print("registry size:", lib.n_tests())
