"""052 - Streaks / extrapolation (ETF universe: no survivorship issue). After N>=4 consecutive down closes, investors
      extrapolate and over-sell -> long next day open->close; after >=4 up closes -> short. Pooled equal-weight across
      triggered ETFs each day. 1bp/side.
053 - Fear over-reaction at the index level (SPY, 0.5bp/side), signals at the prior close:
      a) SPY close-to-close < -2 sigma (60d)  b) VIX close-to-close > +25%  c) both  -> long next day open->close
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

ETFS = ["SPY", "QQQ", "IWM", "DIA", "XLB", "XLE", "XLF", "XLI", "XLK", "XLP", "XLU", "XLV", "XLY",
        "EWJ", "EWG", "EWU", "EWH", "EWA", "EWY", "EWT", "EWZ", "FXI", "EFA", "EEM", "TLT", "GLD"]
OC, CC = {}, {}
for s in ETFS:
    d = lib.get(s).loc["1999-06":]
    OC[s], CC[s] = d.oc, d.cc
OC, CC = pd.DataFrame(OC), pd.DataFrame(CC)
down, up = (CC < 0).astype(int), (CC > 0).astype(int)
def run_len(x):
    g = (x != x.shift()).cumsum()
    return x.groupby(g).cumsum() * x
dstreak = down.apply(run_len).shift(1)     # streak length through the prior close
ustreak = up.apply(run_len).shift(1)
OC, dstreak, ustreak = OC.loc["2000":], dstreak.loc["2000":], ustreak.loc["2000":]
for name, trig, sign in [("052a long after >=4 down closes", dstreak >= 4, 1.0),
                         ("052b short after >=4 up closes", ustreak >= 4, -1.0)]:
    n = trig.sum(axis=1)
    w = trig.astype(float).div(n.replace(0, np.nan), axis=0).fillna(0) * sign
    net = (w * OC.fillna(0)).sum(axis=1) - w.abs().sum(axis=1) * 2 * 1.0 / 1e4
    r = lib.register("052_streaks", name, pd.Series(1.0, index=net.index), net, 0.0, notes="pooled ETFs; 1bp/side in series")
    print(f"{name:<36} {lib.fmt(r)}  gate={lib.passes_gate(r)}  active%={100*(n>0).mean():.0f}")

spy = lib.get("SPY"); vix = lib.get("^VIX")["Close"]
z = (spy.cc / spy.cc.rolling(60).std()).shift(1)
vchg = (vix / vix.shift(1) - 1).reindex(spy.index).shift(1)
y = spy.oc.loc["2000":]
for name, m in {"053a long SPY after < -2 sigma day": z < -2, "053b long SPY after VIX +25% day": vchg > 0.25,
                "053c long SPY after both": (z < -2) & (vchg > 0.25)}.items():
    pos = m.reindex(y.index).fillna(False).astype(float)
    r = lib.register("053_fear", name, pos, y, 0.5)
    t = (pos * y * 1e4)[pos > 0]
    print(f"{name:<36} {lib.fmt(r)}  gate={lib.passes_gate(r)}  n={int(pos.sum())}  "
          f"bp by era: {t.groupby((t.index.year//10)*10).mean().round(0).to_dict()}")
print("registry size:", lib.n_tests())
