"""167 - Month-end flow in credit: on the last day, HYG−IEF spread with pos = −sign(SPY MTD − TLT MTD) (when stocks led,
      balanced funds also trim risky credit). Open->close, 1bp/side/leg. 2007+.
168 - Does 013 scale with volatility? TRAIN+VALID only: per-trade bp of 013 by tercile of TLT and SPY 20d realized vol
      at the prior close (descriptive), then — only if monotone in both periods — a single filter registered.
"""
import os, sys, contextlib, io
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
ns = {"__file__": os.path.join(lib.HERE, "experiments", "013_month_end_rebalance.py")}
with contextlib.redirect_stdout(io.StringIO()):
    exec(open(ns["__file__"]).read().split("S = {")[0], ns)
s, t, last1, gap, idx = ns["s"], ns["t"], ns["last1"], ns["gap_mtd"], ns["idx"]
d = (-np.sign(gap)).where(last1, 0).fillna(0)
hyg, ief = lib.get("HYG").reindex(idx), lib.get("IEF").reindex(idx)
sp = (hyg.oc - ief.oc)
net = (d * sp - d.abs() * 4e-4).where(hyg.oc.notna(), 0).fillna(0)
r = None if set(sys.argv) & {"--filter","--holdout"} else lib.register("013_month_end_rebalance", "167 HYG-IEF spread on last day by stock-bond gap", pd.Series(1.0, index=net.index), net, 0.0)
r is not None and print(f"167  {lib.fmt(r)}  gate={lib.passes_gate(r)}")

tr = (d * s.oc)[last1].loc[:"2022"] * 1e4
for lab, vol in [("TLT", t.cc), ("SPY", s.cc)]:
    v = vol.rolling(20).std().shift(1).reindex(tr.index)
    for per, (a, b) in {"train": ("2002", "2014"), "valid": ("2015", "2022")}.items():
        x = pd.DataFrame({"r": tr.loc[a:b], "v": v.loc[a:b]}).dropna()
        print(f"168 {lab} vol tercile {per}:", x.groupby(pd.qcut(x.v, 3, labels=["low", "mid", "high"]), observed=True).r.mean().round(1).to_dict())
print("registry size:", lib.n_tests())

if "--filter" in sys.argv or "--holdout" in sys.argv:
    v = s.cc.rolling(20).std().shift(1)
    med = v.loc["2002":"2014"].median()
    pos = d.where(v > med, 0)
    if "--filter" in sys.argv:
        r = lib.register("013_month_end_rebalance", "168 013 only when SPY 20d vol > train median", pos, s.oc, 0.5)
        tt = (pos * s.oc)[pos != 0] * 1e4
        print(f"168f {lib.fmt(r)}  gate={lib.passes_gate(r)}  per-trade train {tt.loc[:'2014'].mean():+.1f} valid {tt.loc['2015':'2022'].mean():+.1f}  trades/yr {len(tt.loc[:'2022'])/20.4:.1f}")
    if "--holdout" in sys.argv:
        h = lib.holdout_eval("013_month_end_rebalance", "168 013 only when SPY vol > train median", pos, s.oc, 0.5)
        tt = (pos * s.oc)[pos != 0].loc["2023":] * 1e4
        print("HOLDOUT", {k: round(float(x), 2) for k, x in h.items()}, " per-trade", round(tt.mean(), 1), "n", len(tt))
