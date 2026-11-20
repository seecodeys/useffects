"""058 - Behavioral signals with overnight / multi-day holds (close->close). Signals at close t; best-case = position
from close t (trades on the signal's own closing print); REALISTIC = lag-1 (position from close t+1).
A  1-day residual reversal (vs SPY): ETFs long 3 worst / short 3 best; large caps decile L/S. Hold 1 day.
B  5-day residual reversal ('weekly reversal'), overlapping 5-day holds (avg of last 5 signal portfolios). lag-1.
C  fear rebound: long SPY after a < -2 sigma (60d) close-to-close day; hold 1 day and 5 days.
D  ETF streaks: long after >=4 down closes / short after >=4 up closes, hold 1 day.
Costs per side: ETFs 1bp, large caps 2bp, SPY 0.5bp. Large-cap universe survivorship-biased (flagged).
"""
import os, sys, importlib
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
sys.path.insert(0, os.path.join(lib.HERE, "experiments"))
U = importlib.import_module("006_stock_gap_reversal").UNIVERSE
ETFS = ["QQQ", "IWM", "DIA", "XLB", "XLE", "XLF", "XLI", "XLK", "XLP", "XLU", "XLV", "XLY",
        "EWJ", "EWG", "EWU", "EWH", "EWA", "EWY", "EWT", "EWZ", "FXI", "EFA", "EEM", "TLT", "GLD"]
EXP = "058_behavioral_multiday"
spy = lib.get("SPY")


def panel(syms, minpx=None):
    R, P = {}, {}
    for s in syms:
        try:
            d = lib.get(s).loc["1999-06":]
        except Exception:
            continue
        R[s], P[s] = d.cc, d.Close
    R, P = pd.DataFrame(R), pd.DataFrame(P)
    return R, (P >= minpx if minpx else P.notna())


def ls_weights(sig, k=None, q=None):
    rk = sig.rank(axis=1, pct=q is not None)
    n = sig.notna().sum(axis=1)
    if k:
        lo, hi = rk.le(k), rk.ge(n.values[:, None] - k + 1)
    else:
        lo, hi = rk.le(q), rk.gt(1 - q)
    return (lo.astype(float).div(lo.sum(axis=1), axis=0) * 0.5 - hi.astype(float).div(hi.sum(axis=1), axis=0) * 0.5).fillna(0)


def reg(name, net):
    net = net.loc["2000":]
    r = lib.register(EXP, name, pd.Series(1.0, index=net.index), net, 0.0, notes="close->close, costs on turnover in series")
    print(f"{name:<58} {lib.fmt(r)}  gate={lib.passes_gate(r)}")
    return r


RE, EE = panel(ETFS)
RS, ES = panel(U, 20)
for label, R, E, wfn, c in [("ETFs", RE, EE, lambda s: ls_weights(s, k=3), 1.0),
                            ("large caps", RS, ES, lambda s: ls_weights(s, q=0.1), 2.0)]:
    resid = R.sub(spy.cc.reindex(R.index), axis=0).where(E)
    W = wfn(resid)
    reg(f"A {label}: 1d residual reversal, hold 1d [best-case lag0]", lib.backtest_weights(W, R, c, lag=0))
    reg(f"A {label}: 1d residual reversal, hold 1d [lag1]", lib.backtest_weights(W, R, c, lag=1))
    r5 = R.rolling(5).sum().sub(spy.cc.rolling(5).sum().reindex(R.index), axis=0).where(E)
    W5 = wfn(r5).rolling(5, min_periods=1).mean()
    reg(f"B {label}: 5d residual reversal, overlapping 5d hold [lag1]", lib.backtest_weights(W5, R, c, lag=1))

z = spy.cc / spy.cc.rolling(60).std()
ev = (z < -2).astype(float).to_frame("SPY")
Rspy = spy.cc.to_frame("SPY")
reg("C SPY fear rebound: after < -2sigma, hold 1d [lag0]", lib.backtest_weights(ev, Rspy, 0.5, lag=0))
reg("C SPY fear rebound: after < -2sigma, hold 5d [lag0]", lib.backtest_weights(ev.rolling(5, min_periods=1).max(), Rspy, 0.5, lag=0))
down, up = (RE < 0).astype(int), (RE > 0).astype(int)
rl = lambda x: x.groupby((x != x.shift()).cumsum()).cumsum() * x
ds, us = down.apply(rl), up.apply(rl)
trig = (ds >= 4).astype(float) - (us >= 4).astype(float)
n = trig.abs().sum(axis=1).replace(0, np.nan)
reg("D ETF streak reversal (>=4), hold 1d [lag1]", lib.backtest_weights(trig.div(n, axis=0).fillna(0), RE, 1.0, lag=1))
print("registry size:", lib.n_tests())
