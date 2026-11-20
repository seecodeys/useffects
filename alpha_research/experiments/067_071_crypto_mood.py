"""067 - Retail-mood spillover from crypto weekends: BTC return from the UTC close of the Friday before (~19-20h ET)
      to the UTC close of Sunday (~19-20h ET) -> known before Monday's open. Follow the mood when |weekend| > 3%:
      Monday QQQ (and ARKK) open->close = sign(weekend). 0.5bp QQQ, 2bp ARKK.
071 - Crypto crash shock: BTC UTC-day return < -10% -> next US session QQQ / ARKK open->close, pre-specified SHORT
      (retail risk-appetite shock). If the UTC day is a weekday, the 'next US session' is the next US date after it.
BTC-USD starts 2014-09 -> TRAIN window nearly empty; the gate cannot pass by construction; judge on VALID (2015-22).
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
btc = lib.get("BTC-USD")["Close"]
for sym, cost in [("QQQ", 0.5), ("ARKK", 2.0)]:
    d = lib.get(sym).loc["2014-10":]
    idx = d.index
    pos67 = pd.Series(0.0, index=idx)
    for t in idx[idx.dayofweek == 0]:
        fri, sun = t - pd.Timedelta(days=3), t - pd.Timedelta(days=1)
        if fri in btc.index and sun in btc.index:
            w = btc[sun] / btc[fri] - 1
            if abs(w) > 0.03:
                pos67[t] = np.sign(w)
    r = lib.register("067_crypto_weekend", f"067 {sym} Monday: follow BTC weekend move if |move|>3%", pos67, d.oc, cost)
    print(f"067 {sym:<5} {lib.fmt(r)}  n={int(pos67.abs().sum())}")
    crash = btc.pct_change() < -0.10
    pos71 = pd.Series(0.0, index=idx)
    for day in crash[crash].index:
        nxt = idx[idx > day]
        if len(nxt):
            pos71[nxt[0]] = -1.0
    r = lib.register("071_crypto_crash", f"071 {sym} short next session after BTC day < -10%", pos71, d.oc, cost)
    print(f"071 {sym:<5} {lib.fmt(r)}  n={int(pos71.abs().sum())}")
print("registry size:", lib.n_tests())
