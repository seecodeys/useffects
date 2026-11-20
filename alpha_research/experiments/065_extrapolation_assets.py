"""065 - Extrapolation in sentiment-driven assets (time series): after a 63-day move in the top / bottom 5% of the
asset's own TRAIN-period distribution, bet on reversal for the next 21 days (overlapping), lag-1, 1bp/side.
Assets: GLD, TLT, USO, EEM, QQQ (each registered separately would add 5 tests -> pooled equal-weight instead).
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
A = ["GLD", "TLT", "USO", "EEM", "QQQ"]
R = pd.DataFrame({a: lib.get(a).cc for a in A})
m63 = R.rolling(63).sum()
tr = m63.loc[lib.TRAIN[0]:lib.TRAIN[1]]
hi, lo = tr.quantile(0.95), tr.quantile(0.05)
sig = (m63 < lo).astype(float) - (m63 > hi).astype(float)
W = sig.rolling(21, min_periods=1).mean() / len(A)
net = lib.backtest_weights(W, R, 1.0, lag=1).loc["2000":]
r = lib.register("065_extrapolation", "065 pooled 63d-extreme reversal GLD/TLT/USO/EEM/QQQ (21d hold)", pd.Series(1.0, index=net.index), net, 0.0)
print(f"065 extrapolation reversal   {lib.fmt(r)}  gate={lib.passes_gate(r)}  active%={100*(W.abs().sum(axis=1)>0).loc['2000':].mean():.0f}")
print("registry size:", lib.n_tests())
