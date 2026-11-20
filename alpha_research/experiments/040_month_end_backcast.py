"""040 - Fresh out-of-sample test of 013 BACKWARDS in time: Feb 1993 - Jul 2002 (before TLT existed).
Bond leg proxy: 30y Treasury yield (^TYX, in %); bond MTD return ~= -D * (yield change)/100 with D = 17 (TLT-like duration).
1) Validate the proxy on 2002-08+ (sign agreement of the stock-bond gap vs the TLT-based one on last days).
2) Apply the exact 013 rule on 1993-02..2002-07 (never examined). SPY open->close, 0.5bp/side.
This period is outside TRAIN/VALID/HOLDOUT, so it is evaluated directly (one look) and logged in holdout_log.
"""
import os, sys, contextlib, io
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

spy = lib.get("SPY")
tyx = lib.get("^TYX")["Close"]
idx = spy.index
ym = idx.to_period("M")
rb = pd.Series(1, index=idx)[::-1].groupby(ym[::-1]).cumsum()[::-1]
last = (rb == 1) & pd.Series(ym != ym[-1], index=idx)
first = pd.Series(ym != pd.Series(ym, index=idx).shift(1).values, index=idx)


def mtd_from_level(level):          # MTD through prior close, month base = previous month-end level
    lv = level.reindex(idx).ffill()
    base = lv.where(rb == 1).ffill().shift(1).where(first).ffill()
    return lv.shift(1), base


s_now, s_base = mtd_from_level(spy["aC"])
spy_mtd = s_now / s_base - 1
y_now, y_base = mtd_from_level(tyx)
bond_mtd_proxy = -17 * (y_now - y_base) / 100
gap_proxy = spy_mtd - bond_mtd_proxy

ns = {"__file__": os.path.join(lib.HERE, "experiments", "013_month_end_rebalance.py")}
with contextlib.redirect_stdout(io.StringIO()):
    exec(open(ns["__file__"]).read().split("S = {")[0], ns)
L = ns["last1"][ns["last1"]].index
agree = (np.sign(gap_proxy.reindex(L)) == np.sign(ns["gap_mtd"].reindex(L))).mean()
print(f"proxy check 2002-08+: sign agreement with TLT-based gap on {len(L)} month-ends = {agree:.1%}; "
      f"corr = {gap_proxy.reindex(L).corr(ns['gap_mtd'].reindex(L)):.2f}")

pos = (-np.sign(gap_proxy)).where(last, 0)
per = ("1993-02-01", "2002-07-31")
m = lib.evaluate(pos.loc[per[0]:per[1]], spy.oc.loc[per[0]:per[1]], 0.5)
print("BACKCAST 1993-02..2002-07:", {k: round(float(v), 2) for k, v in m.items()})
row = pd.DataFrame([{"timestamp": pd.Timestamp.now().isoformat(timespec="seconds"), "experiment": "040_month_end_backcast",
                     "variant": "013 rule, TYX bond proxy, 1993-02..2002-07 (fresh pre-sample)", **m}])
p = os.path.join(lib.HERE, "results", "holdout_log.csv")
pd.concat([pd.read_csv(p), row], ignore_index=True).to_csv(p, index=False)
tr = (pos * spy.oc * 1e4).loc[per[0]:per[1]][last.loc[per[0]:per[1]]]
print("trades:", len(tr), " yearly bp sum:", tr.groupby(tr.index.year).sum().round(0).astype(int).to_dict())
