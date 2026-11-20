"""100 - Is month-end volatility under-priced? realized |open->close| / (VIX9D|VIX-implied 6.5h move) on month-end
      days vs other days, 2002-2022 (descriptive).
103 - Micro-cap over-reaction at ETF level: IWC (micro-cap, 2005+) and IJR: reverse |z|>2 prior-day moves, close->close,
      lag-1, 2bp/side; pooled.
096 - Portfolio of survivors: 013 SPY month-end (open->close, 0.5bp) + 031 TLT month-end long (1bp) + 062b monthly
      country reversal (close->close, 2bp) — combined daily P&L stats by period (construction only, not registered).
"""
import os, sys, math, contextlib, io
import numpy as np, pandas as pd
from scipy.stats import norm
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
ns = {"__file__": os.path.join(lib.HERE, "experiments", "013_month_end_rebalance.py")}
with contextlib.redirect_stdout(io.StringIO()):
    exec(open(ns["__file__"]).read().split("S = {")[0], ns)
s, last1, gap = ns["s"], ns["last1"], ns["gap_mtd"]
iv = lib.get("^VIX9D")["Close"].reindex(s.index).combine_first(lib.get("^VIX")["Close"].reindex(s.index)).shift(1) / 100
exp_move = iv * math.sqrt(6.5 / 24 / 365) * math.sqrt(2 / math.pi)
ratio = ((s.Close / s.Open - 1).abs() / exp_move).loc[:"2022"]
print(f"100 realized/implied |move| ratio: month-end {ratio[last1].mean():.2f} vs other days {ratio[~last1].mean():.2f} "
      f"(median {ratio[last1].median():.2f} vs {ratio[~last1].median():.2f})")

W, R = {}, {}
for sym in ["IWC", "IJR"]:
    d = lib.get(sym); z = d.cc / d.cc.rolling(60).std()
    W[sym] = pd.Series(np.where(z > 2, -1.0, np.where(z < -2, 1.0, 0.0)), index=d.index); R[sym] = d.cc
W, R = pd.DataFrame(W), pd.DataFrame(R)
n = W.abs().sum(axis=1).replace(0, np.nan)
net = lib.backtest_weights(W.div(n, axis=0).fillna(0), R, 2.0, lag=1).loc["2000":]
r = lib.register("103_microcap_reversal", "103 IWC/IJR reverse |z|>2 prior-day moves (lag1)", pd.Series(1.0, index=net.index), net, 0.0)
print(f"103 micro/small-cap reversal  {lib.fmt(r)}  gate={lib.passes_gate(r)}")

p013 = lib.daily_pnl((-np.sign(gap)).where(last1, 0), s.oc, 0.5)
tlt = lib.get("TLT").reindex(s.index)
p031 = lib.daily_pnl(last1.astype(float), tlt.oc, 1.0)
C = ["EWJ", "EWG", "EWU", "EWH", "EWA", "EWS", "EWC", "EWW", "EWZ", "EWY", "EWT", "EWP", "EWI", "EWQ", "EWL", "EWN", "EWD", "EWK", "EWO", "EWM"]
RC = pd.DataFrame({c: lib.get(c).cc for c in C}).loc["1998":]
past = RC.rolling(21).sum(); rel = past.sub(past.mean(axis=1), axis=0); rk = rel.rank(axis=1); nn = rel.notna().sum(axis=1)
WC = (rk.le(3).astype(float) / 6 - rk.ge(nn.values[:, None] - 2).astype(float) / 6).where(nn >= 10, 0).fillna(0).rolling(21, min_periods=1).mean()
p062 = lib.backtest_weights(WC, RC, 2.0, lag=1)
P = pd.concat({"013": p013, "031": p031, "062b": p062}, axis=1).loc["2003":].fillna(0)
P["combined"] = P.sum(axis=1)
print("096 correlations:", P[["013", "031", "062b"]].corr().round(2).values.tolist())
for per, (a, b) in {"train 03-14": ("2003", "2014"), "valid 15-22": lib.VALID, "holdout 23+": lib.HOLDOUT}.items():
    row = {c: round(lib.evaluate(pd.Series(1.0, index=P.loc[a:b].index), P[c].loc[a:b], 0)["sharpe"], 2) for c in P}
    print(f"096 Sharpe {per}: {row}")
print("registry size:", lib.n_tests())
