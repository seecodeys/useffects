"""120 - [arXiv 2405.12768 'Ponzi Funds'] flow-chasing price pressure reverts. Survivorship-free ETF cross-section
(9 sector SPDRs + 20 country ETFs). Flagged = trailing 63d return in the top third AND 21d avg dollar volume / its
252d avg in the top third (volume surge = flow proxy). Monthly: short flagged, long the rest equal-weight (dollar
neutral), hold 21 days overlapping, lag-1, close->close, 1.5bp/side.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
U = ["XLB", "XLE", "XLF", "XLI", "XLK", "XLP", "XLU", "XLV", "XLY", "EWJ", "EWG", "EWU", "EWH", "EWA", "EWS", "EWC",
     "EWW", "EWZ", "EWY", "EWT", "EWP", "EWI", "EWQ", "EWL", "EWN", "EWD", "EWK", "EWO", "EWM"]
R, M, V = {}, {}, {}
for s in U:
    d = lib.get(s)
    R[s] = d.cc
    M[s] = d.aC / d.aC.shift(63) - 1
    dv = d.Close * d.Volume
    V[s] = dv.rolling(21).mean() / dv.rolling(252).mean()
R, M, V = (pd.DataFrame(x).loc["1998":] for x in (R, M, V))
hiM = M.rank(axis=1, pct=True) > 2 / 3
hiV = V.rank(axis=1, pct=True) > 2 / 3
flag = hiM & hiV & M.notna() & V.notna()
rest = (~flag) & M.notna() & V.notna()
nf, nr = flag.sum(axis=1), rest.sum(axis=1)
W = (rest.astype(float).div(nr.replace(0, np.nan), axis=0) * 0.5 - flag.astype(float).div(nf.replace(0, np.nan), axis=0) * 0.5)
W = W.where(nf > 0, 0).fillna(0).rolling(21, min_periods=1).mean()
net = lib.backtest_weights(W, R, 1.5, lag=1).loc["2000":]
g = lib.backtest_weights(W, R, 0, lag=1).loc["2000":]
r = lib.register("120_ponzi_flows", "120 short flow-chased ETFs (high 3m return + volume surge), long rest", pd.Series(1.0, index=net.index), net, 0.0,
                 notes="paper 2405.12768 analog")
print(f"120  {lib.fmt(r)}  gate={lib.passes_gate(r)}  avg flagged={nf.loc['2000':].mean():.1f}  gross by 5y:",
      (g.groupby((g.index.year // 5) * 5).mean() * 1e4).round(1).to_dict())
print("registry size:", lib.n_tests())
