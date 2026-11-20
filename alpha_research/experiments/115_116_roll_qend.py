"""115 - Equity-index futures quarterly roll week (the 5 trading days ending the Friday BEFORE the 3rd-Friday expiry of
      Mar/Jun/Sep/Dec): long SPY / IWM close->close during roll week; timing value vs exposure-matched always-long. 0.5/1bp.
      (No strong directional prior; pre-specified long.)
116 - Does the month-end rebalancing edge (013) scale with rebalancing size? Mean per-trade bp at quarter-ends and
      year-ends vs other month-ends, TRAIN+VALID only (descriptive interaction; holdout untouched).
"""
import os, sys, contextlib, io
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
for sym, cost in [("SPY", 0.5), ("IWM", 1.0)]:
    d = lib.get(sym); idx = d.index; r = d.cc
    roll = pd.Series(False, index=idx)
    for y in range(1999, 2027):
        for m in [3, 6, 9, 12]:
            x = pd.Timestamp(y, m, 15); tf = x + pd.Timedelta(days=(4 - x.dayofweek) % 7)
            fri_before = tf - pd.Timedelta(days=7)
            wk = idx[(idx <= fri_before) & (idx > fri_before - pd.Timedelta(days=7))]
            roll[wk] = True
    pos = roll.shift(-1, fill_value=False).astype(float)
    net = lib.backtest_weights(pos.to_frame("S"), r.to_frame("S"), cost)
    held = pos.shift(1).fillna(0); out = pd.Series(0.0, index=net.index)
    for a, b in [lib.TRAIN, lib.VALID, lib.HOLDOUT]:
        out.loc[a:b] = net.loc[a:b] - held.loc[a:b].mean() * r.loc[a:b].fillna(0)
    out = out.loc["2000":]
    res = lib.register("115_futures_roll", f"115 long {sym} in index-futures roll week (timing value)", pd.Series(1.0, index=out.index), out, 0.0)
    print(f"115 {sym}  {lib.fmt(res)}  gate={lib.passes_gate(res)}")

ns = {"__file__": os.path.join(lib.HERE, "experiments", "013_month_end_rebalance.py")}
with contextlib.redirect_stdout(io.StringIO()):
    exec(open(ns["__file__"]).read().split("S = {")[0], ns)
s, last1, gap = ns["s"], ns["last1"], ns["gap_mtd"]
t = ((-np.sign(gap)) * s.oc)[last1].loc[:"2022"] * 1e4
grp = pd.Series(np.where(t.index.month == 12, "year-end", np.where(t.index.month % 3 == 0, "other quarter-end", "other month-end")), index=t.index)
print("116 013 per-trade bp 2002-22 by month type:")
print(t.groupby(grp).agg(["mean", "count", lambda x: x.mean() / x.std() * np.sqrt(len(x))]).round(2).rename(columns={"<lambda_0>": "t"}).to_string())
print("registry size:", lib.n_tests())
