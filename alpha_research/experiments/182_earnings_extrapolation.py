"""182 - [Earnings Extrapolation and Predictable Stock Market Returns, RFS 2025 (abstract via Crossref)]: the US market's
return in the FIRST month of a quarter (earnings-season month) correlates positively with returns in the following
months, but negatively with the first month of the next quarter. Pre-specified (SPY, close->close, positions set at the
month-end close, 0.5bp/side):
  a) months 2-3 of each quarter: long if month-1 return > 0, short if < 0
  b) first month of the NEXT quarter: opposite sign of the current quarter's month-1 return
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
spy = lib.get("SPY"); idx = spy.index
mret = spy.aC.resample("ME").last().pct_change()
qm1 = mret[mret.index.month.isin([1, 4, 7, 10])]
pa, pb = pd.Series(0.0, index=idx), pd.Series(0.0, index=idx)
for me, r1 in qm1.items():
    s = np.sign(r1)
    m2 = me + pd.offsets.MonthEnd(1); m3 = me + pd.offsets.MonthEnd(2); nq1 = me + pd.offsets.MonthEnd(3)
    pa[(idx > me) & (idx <= m3)] = s                 # months 2 and 3
    pb[(idx > m3) & (idx <= nq1)] = -s               # next quarter's first month
for tag, p in [("a months 2-3 follow quarter's month-1 sign", pa), ("b next quarter's month-1 opposite sign", pb)]:
    pos = p.shift(-1).fillna(0)                      # held into day t+1, decided at close t (month-end known)
    net = lib.backtest_weights(pos.to_frame("S"), spy.cc.to_frame("S"), 0.5).loc["2000":]
    r = lib.register("182_earnings_extrapolation", f"182{tag}", pd.Series(1.0, index=net.index), net, 0.0, notes="RFS 2025 earnings extrapolation")
    print(f"182{tag:<46} {lib.fmt(r)}  gate={lib.passes_gate(r)}")
print("registry size:", lib.n_tests())
