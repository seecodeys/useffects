"""326 - Next-morning reversal after the biggest days. Mechanism: on the largest up/down days, leveraged ETFs
(SSO/UPRO/SDS/SPXU/QLD/TQQQ) and short-gamma option dealers trade in the direction of the move into the close, pushing
the close too far; the overshoot should reverse overnight / early next session.
Event at close t (per ticker, SPY and QQQ): |cc_t| > 95th percentile of |cc| over the prior 252 sessions (t-252..t-1),
i.e. a top-5% day known at the close. Position = -sign(cc_t) (fade). Book = 50/50 SPY+QQQ (0.5 per ticker on event).
Costs 0.5bp/side. Pre-specified variants (max 4, no tuning after results):
  A fade close t -> open t+1 (gap). Needs entry AT the close that generated the signal (MOC) -> labelled BEST-CASE.
  B fade open t+1 -> close t+1 (oc). Fully realistic (known before the open).
  C fade close->close, lag=1 (position from close t+1 to close t+2). Realistic close->close version.
  D fade close->close, lag=0 (close t -> close t+1). Same-close BEST-CASE reference only.
Registered as pos = gross exposure, ret = net pnl / exposure (cost already inside), so bp_per_trade is per unit traded.
Timing value = net - mean(signed exposure over 2000-2022) * underlying return of the same leg (strips any static beta).
Nothing from 2023+ is printed; holdout untouched."""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

TK = ["SPY", "QQQ"]; C = 0.5
D = {s: lib.get(s) for s in TK}
idx = D["SPY"].index.intersection(D["QQQ"].index)
sig = {}
for s in TK:
    cc = D[s]["cc"].reindex(idx); a = cc.abs()
    thr = a.shift(1).rolling(252, min_periods=252).quantile(0.95)
    sig[s] = (-np.sign(cc)).where(a > thr, 0.0).fillna(0.0)          # decided at close t
S = pd.DataFrame(sig) * 0.5                                          # signed weights, 50/50 book
tt = lambda x: x.mean() / x.std() * np.sqrt(len(x))

def legs(name):
    """returns (net pnl, signed exposure per ticker DataFrame, underlying leg returns DataFrame) on idx"""
    if name in ("A", "B"):
        col = "gap" if name == "A" else "oc"
        P = S.shift(1).fillna(0.0)                                   # event at t -> traded on day t+1
        R = pd.DataFrame({s: D[s][col].reindex(idx) for s in TK}).fillna(0.0)
        net = (P * R).sum(axis=1) - P.abs().sum(axis=1) * 2 * C / 1e4
    else:
        lag = 1 if name == "C" else 0
        R = pd.DataFrame({s: D[s]["cc"].reindex(idx) for s in TK}).fillna(0.0)
        net = lib.backtest_weights(S, R, C, lag=lag)
        P = S.shift(1 + lag).fillna(0.0)
    return net, P, R

desc = {"A": "gap close->open (BEST-CASE at-close entry)", "B": "next-day open->close",
        "C": "close->close lag1", "D": "close->close lag0 (BEST-CASE)"}
for k in "ABCD":
    net, P, R = legs(k)
    net, P, R = net.loc["2000":], P.loc["2000":], R.loc["2000":]
    g = P.abs().sum(axis=1)
    ret = (net / g).where(g > 0, 0.0)
    r = lib.register("326_big_day_reversal", f"326{k} {desc[k]}", g, ret, 0.0,
                     notes="SPY+QQQ 50/50, top-5% |cc| vs prior 252d, fade, 0.5bp/side")
    beta = P.loc[:"2022"].mean()
    tv = net - (R * beta).sum(axis=1)
    ev = (S.abs() > 0).sum(axis=1).loc["2000":"2022"]
    print(f"326{k} {desc[k]}: events {int(ev.loc[:'2014'].sum())}/{int(ev.loc['2015':].sum())}  {lib.fmt(r)}  "
          f"timing t {tt(tv.loc[:'2014']):+.2f} / {tt(tv.loc['2015':'2022']):+.2f}  gate={lib.passes_gate(r)}")
    yr = net.loc[:"2022"].groupby(net.loc[:"2022"].index.year).sum() * 1e4
    print("   yearly bp:", " ".join(f"{y % 100:02d}:{v:+.0f}" for y, v in yr.items()))
print("registry size:", lib.n_tests())
