"""062 refinements (pre-specified; monthly country reversal N=21, H=21, lag-1, 2bp/side):
  d) skip-week: past return over t-21..t-5 (drop the latest week, which showed no reversal)
  e) vol-scaled: rank on past-21d relative return / 63d volatility
  f) broader universe: + EM/other single-country ETFs (EZA, ECH, TUR, THD, EIDO, EPHE, EPOL, EIS, ENZL, INDA, EWX-free)
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
BASE = ["EWJ", "EWG", "EWU", "EWH", "EWA", "EWS", "EWC", "EWW", "EWZ", "EWY", "EWT", "EWP", "EWI", "EWQ", "EWL",
        "EWN", "EWD", "EWK", "EWO", "EWM"]
EXTRA = ["EZA", "ECH", "TUR", "THD", "EIDO", "EPHE", "EPOL", "EIS", "ENZL", "INDA"]


def run(tag, syms, score_fn):
    R = {}
    for c in syms:
        try:
            R[c] = lib.get(c).cc
        except Exception as e:
            print("skip", c, e)
    R = pd.DataFrame(R).loc["1998":]
    rel = score_fn(R)
    rk = rel.rank(axis=1); n = rel.notna().sum(axis=1)
    W = (rk.le(3).astype(float) / 6 - rk.ge(n.values[:, None] - 2).astype(float) / 6).where(n >= 10, 0).fillna(0)
    W = W.rolling(21, min_periods=1).mean()
    net = lib.backtest_weights(W, R, 2.0, lag=1).loc["2000":]
    g = lib.backtest_weights(W, R, 0.0, lag=1).loc["2000":]
    r = lib.register("062_country_reversal", f"062{tag}", pd.Series(1.0, index=net.index), net, 0.0, notes="close->close, 2bp/side")
    print(f"062{tag:<40} {lib.fmt(r)}  gate={lib.passes_gate(r)}  gross by 5y:",
          (g.groupby((g.index.year // 5) * 5).mean() * 1e4).round(1).to_dict())
    return r


rel = lambda P: P.sub(P.mean(axis=1), axis=0)
run("d skip-week (t-21..t-5)", BASE, lambda R: rel(R.rolling(16).sum().shift(5)))
run("e vol-scaled 21d", BASE, lambda R: rel(R.rolling(21).sum()) / R.rolling(63).std())
run("f broader universe (30 countries)", BASE + EXTRA, lambda R: rel(R.rolling(21).sum()))
print("registry size:", lib.n_tests())
