"""279 - Survivor portfolio v2 (construction, 2002-22 only, not registered): 
  A 013 SPY month-end, sized 1.0 on high-vol month-ends (168 filter) and 0.5 otherwise, open->close, 0.5bp
  B 276 IEF long over the last 3 sessions of each month, close->close, 1bp
  C 062b monthly country reversal, close->close, 2bp
Daily P&L at fixed notional per leg; correlations, Sharpe, max DD, and an equal-risk combination."""
import os, sys, contextlib, io
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
ns = {"__file__": os.path.join(lib.HERE, "experiments", "013_month_end_rebalance.py")}
with contextlib.redirect_stdout(io.StringIO()):
    exec(open(ns["__file__"]).read().split("S = {")[0], ns)
s, last1, gap, idx = ns["s"], ns["last1"], ns["gap_mtd"], ns["idx"]
v = s.cc.rolling(20).std().shift(1); med = v.loc["2002":"2014"].median()
size = pd.Series(np.where(v > med, 1.0, 0.5), index=idx)
A = lib.daily_pnl((-np.sign(gap) * size).where(last1, 0).fillna(0), s.oc, 0.5)
ief = lib.get("IEF"); ii = ief.index; ym = pd.Series(ii.to_period("M"), index=ii)
bwd = pd.Series(1, index=ii)[::-1].groupby(ym[::-1]).cumsum()[::-1]
pB = ((bwd <= 3) & (ym != ym.iloc[-1])).astype(float).shift(-1).fillna(0)
B = lib.backtest_weights(pB.to_frame("I"), ief.cc.to_frame("I"), 1.0)
C_ = ["EWJ", "EWG", "EWU", "EWH", "EWA", "EWS", "EWC", "EWW", "EWZ", "EWY", "EWT", "EWP", "EWI", "EWQ", "EWL", "EWN", "EWD", "EWK", "EWO", "EWM"]
RC = pd.DataFrame({c: lib.get(c).cc for c in C_}).loc["1998":]
past = RC.rolling(21).sum(); rel = past.sub(past.mean(axis=1), axis=0); rk = rel.rank(axis=1); n = rel.notna().sum(axis=1)
WC = (rk.le(3).astype(float) / 6 - rk.ge(n.values[:, None] - 2).astype(float) / 6).where(n >= 10, 0).fillna(0).rolling(21, min_periods=1).mean()
C = lib.backtest_weights(WC, RC, 2.0, lag=1)
P = pd.concat({"A_013": A, "B_IEF": B, "C_062b": C}, axis=1).loc["2003":"2022"].fillna(0)
vol = P.std()
P["equal_risk"] = (P / vol * vol.mean()).sum(axis=1) / 3
print("correlations:\n", P[["A_013", "B_IEF", "C_062b"]].corr().round(2).to_string())
for per, (a, b) in {"2003-14": ("2003", "2014"), "2015-22": ("2015", "2022")}.items():
    row = {c: (round(lib.evaluate(pd.Series(1.0, index=P.loc[a:b].index), P[c].loc[a:b], 0)["sharpe"], 2),
               round(lib.evaluate(pd.Series(1.0, index=P.loc[a:b].index), P[c].loc[a:b], 0)["max_dd_%"], 1)) for c in P}
    print(per, "(Sharpe, maxDD%):", row)
