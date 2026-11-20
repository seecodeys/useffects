"""059/060 - Crowd-sentiment gauges from free data, contrarian, next-day SPY/QQQ open->close (0.5bp/side).
060a fear premium: VIX - 20d realized vol (annualised, %) at prior close in top 20% of TRAIN distribution -> long SPY
060b short-term panic: VIX9D / VIX > 1 at prior close -> long SPY   (VIX9D from 2011)
059  crowd positioning: SQQQ share of (TQQQ+SQQQ) dollar volume, prior day, above its 60d mean + 2 sd -> long QQQ
     (crowd piling into the bearish 3x ETF = capitulation). 2010+.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
spy, qqq = lib.get("SPY"), lib.get("QQQ")
vix, vix9 = lib.get("^VIX")["Close"], lib.get("^VIX9D")["Close"]
rv = spy.cc.rolling(20).std() * np.sqrt(252) * 100
fp = (vix.reindex(spy.index) - rv).shift(1)
thr = fp.loc[lib.TRAIN[0]:lib.TRAIN[1]].quantile(0.8)
y = spy.oc.loc["2000":]
pos = (fp > thr).reindex(y.index).fillna(False).astype(float)
r = lib.register("060_fear_gauges", "060a long SPY when VIX-RV fear premium top 20%", pos, y, 0.5)
print(f"{'060a fear premium high -> long SPY':<44} {lib.fmt(r)}  gate={lib.passes_gate(r)}")
ratio = (vix9 / vix).reindex(spy.index).shift(1)
pos = (ratio > 1).reindex(y.index).fillna(False).astype(float).loc["2011":]
r = lib.register("060_fear_gauges", "060b long SPY when VIX9D/VIX > 1", pos, y.loc["2011":], 0.5)
print(f"{'060b VIX9D>VIX -> long SPY':<44} {lib.fmt(r)}  gate={lib.passes_gate(r)}  days={int(pos.sum())}")
tq, sq = lib.get("TQQQ"), lib.get("SQQQ")
dv = lambda d: d.Close * d.Volume
share = (dv(sq) / (dv(sq) + dv(tq))).dropna()
z = ((share - share.rolling(60).mean()) / share.rolling(60).std()).shift(1)
yq = qqq.oc.loc[share.index[0]:]
pos = (z > 2).reindex(yq.index).fillna(False).astype(float)
r = lib.register("059_crowd_letf", "059 long QQQ when SQQQ dollar-volume share z>2", pos, yq, 0.5)
print(f"{'059 SQQQ share spike -> long QQQ':<44} {lib.fmt(r)}  gate={lib.passes_gate(r)}  days={int(pos.sum())}  (train only 2010-14)")
print("registry size:", lib.n_tests())
