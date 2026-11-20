"""062 - Country-level over-reaction / mean reversion (survivorship-free: iShares single-country ETFs since 1996).
Signal at close t: each country's past-N-day return minus the cross-sectional average. Long the 3 worst, short the 3
best, hold H days with overlapping portfolios (average of the last H signal portfolios), lag-1, 2bp/side.
  a) N=5,  H=5   (weekly over-reaction)
  b) N=21, H=21  (monthly over-reaction)
  c) N=63, H=21  (quarterly losers vs winners)
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
C = ["EWJ", "EWG", "EWU", "EWH", "EWA", "EWS", "EWC", "EWW", "EWZ", "EWY", "EWT", "EWP", "EWI", "EWQ", "EWL",
     "EWN", "EWD", "EWK", "EWO", "EWM"]
R = pd.DataFrame({c: lib.get(c).cc for c in C}).loc["1998":]
print("countries:", R.shape[1], " avg available 2000-14:", R.loc["2000":"2014"].notna().sum(axis=1).mean().round(1))
for N, H, tag in [(5, 5, "a"), (21, 21, "b"), (63, 21, "c")]:
    past = R.rolling(N).sum()
    rel = past.sub(past.mean(axis=1), axis=0)
    rk = rel.rank(axis=1); n = rel.notna().sum(axis=1)
    lo, hi = rk.le(3), rk.ge(n.values[:, None] - 2)
    W = (lo.astype(float) / 6 - hi.astype(float) / 6).where(n >= 10, 0).fillna(0)
    W = W.rolling(H, min_periods=1).mean()
    net = lib.backtest_weights(W, R, 2.0, lag=1).loc["2000":]
    r = lib.register("062_country_reversal", f"062{tag} country reversal N={N} H={H} (lag1)", pd.Series(1.0, index=net.index), net, 0.0,
                     notes="close->close, 2bp/side on turnover")
    g = lib.backtest_weights(W, R, 0.0, lag=1).loc["2000":]
    print(f"062{tag} N={N:>2} H={H:>2}  {lib.fmt(r)}  gate={lib.passes_gate(r)}  gross bp/day by 5y:",
          (g.groupby((g.index.year // 5) * 5).mean() * 1e4).round(1).to_dict())
print("registry size:", lib.n_tests())
