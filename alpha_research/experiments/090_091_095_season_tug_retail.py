"""090 - Seasonal mood: long SPY Nov-Apr, flat May-Oct, close->close lag-1, timing value vs exposure-matched B&H.
091 - Tug-of-war: after >=4 consecutive positive SPY overnight returns (close->open), short SPY open->close. 0.5bp.
095 - Retail-heavy over-reaction: after |prior-day close->close z-score| > 2 (60d vol), bet on reversal next day
      close->close (lag-1: signal at close t, position from close t+1 held 1 day). ARKK (2014+), TQQQ (2010+), SMH,
      and SPY as the institutional control. 2bp/side (SPY 0.5bp). Pooled across the 3 retail-heavy ETFs.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
spy = lib.get("SPY")
pos = pd.Series(np.isin(spy.index.month, [11, 12, 1, 2, 3, 4]).astype(float), index=spy.index)
net = lib.backtest_weights(pos.to_frame("S"), spy.cc.to_frame("S"), 0.5, lag=1)
held = pos.shift(2).fillna(0); out = pd.Series(0.0, index=net.index)
for a, b in [lib.TRAIN, lib.VALID, lib.HOLDOUT]:
    out.loc[a:b] = net.loc[a:b] - held.loc[a:b].mean() * spy.cc.loc[a:b].fillna(0)
r = lib.register("090_seasonal", "090 Sell-in-May: long Nov-Apr only (timing value)", pd.Series(1.0, index=out.loc['2000':].index), out.loc["2000":], 0.0)
print(f"{'090 sell in May':<46} {lib.fmt(r)}  gate={lib.passes_gate(r)}")

on = (spy["Open"] / spy["Close"].shift(1) - 1) > 0
streak = on.astype(int).groupby((~on).cumsum()).cumsum()
y = spy.oc.loc["2000":]
pos = -(streak.shift(1) >= 4).reindex(y.index).fillna(False).astype(float)   # 4+ positive overnights through yesterday
# note: today's overnight is NOT included (would need today's open); signal fully known at prior close
r = lib.register("091_tug_of_war", "091 short SPY open->close after >=4 positive overnights", pos, y, 0.5)
print(f"{'091 tug-of-war':<46} {lib.fmt(r)}  gate={lib.passes_gate(r)}  n={int(-pos.sum())}")

def rev_w(sym):
    d = lib.get(sym); z = d.cc / d.cc.rolling(60).std()
    return pd.Series(np.where(z > 2, -1.0, np.where(z < -2, 1.0, 0.0)), index=d.index), d.cc
W, R = {}, {}
for s in ["ARKK", "TQQQ", "SMH"]:
    W[s], R[s] = rev_w(s)
W, R = pd.DataFrame(W), pd.DataFrame(R)
n = W.abs().sum(axis=1).replace(0, np.nan)
net = lib.backtest_weights(W.div(n, axis=0).fillna(0), R, 2.0, lag=1).loc["2000":]
r = lib.register("095_retail_overreaction", "095 retail-heavy ETFs (ARKK/TQQQ/SMH): reverse |z|>2 days", pd.Series(1.0, index=net.index), net, 0.0)
print(f"{'095 retail-heavy reversal (pooled)':<46} {lib.fmt(r)}  gate={lib.passes_gate(r)}")
w, rr = rev_w("SPY")
net = lib.backtest_weights(w.to_frame("S"), rr.to_frame("S"), 0.5, lag=1).loc["2000":]
r = lib.register("095_retail_overreaction", "095 control: SPY reverse |z|>2 days", pd.Series(1.0, index=net.index), net, 0.0)
print(f"{'095 SPY control':<46} {lib.fmt(r)}  gate={lib.passes_gate(r)}")
print("registry size:", lib.n_tests())
