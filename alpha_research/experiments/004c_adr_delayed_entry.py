"""004c - Bid-ask-bounce check for 004 via delayed entry (Yahoo 1h bars, ~730 days).

If the XS ADR signal is just the opening print landing on bid/ask, then entering
at 10:30 ET (close of the first hourly bar) instead of the open should kill it.
Compares, on the same days, gross L/S returns: open->close, open->10:30, 10:30->close.
Diagnostic only (recent ~2 years overlap the holdout, which has already had its one look).
"""
import os, sys, time
import numpy as np
import pandas as pd
import requests
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
sys.path.insert(0, os.path.join(lib.HERE, "experiments"))
import importlib
m = importlib.import_module("004_adr_catchup")


def hourly(sym):
    path = os.path.join(lib.DATA, f"{sym}_1h.csv")
    if not os.path.exists(path):
        r = requests.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}",
                         params={"range": "730d", "interval": "1h"}, headers=lib._HDR, timeout=30)
        res = r.json()["chart"]["result"][0]
        q = res["indicators"]["quote"][0]
        ts = pd.to_datetime(res["timestamp"], unit="s", utc=True).tz_convert("America/New_York").tz_localize(None)
        pd.DataFrame({"ts": ts, "Open": q["open"], "Close": q["close"]}).dropna().to_csv(path, index=False)
        time.sleep(0.3)
    return pd.read_csv(path, parse_dates=["ts"])


S, Si, Y = m.build()
px = pd.DataFrame({a: lib.get(a)["Close"].shift(1) for a in S}).reindex(S.index)
Sx = S.where(px >= 20)
p1030, opn, cls = {}, {}, {}
for a in Sx:
    h = hourly(a)
    h["d"] = h.ts.dt.normalize()
    first = h[(h.ts.dt.hour == 9) & (h.ts.dt.minute == 30)].set_index("d")
    p1030[a] = first["Close"]
    d = lib.get(a)
    opn[a], cls[a] = d["Open"], d["Close"]
P1030, O, C = pd.DataFrame(p1030), pd.DataFrame(opn), pd.DataFrame(cls)
days = P1030.dropna(how="all").index.intersection(Sx.index)
Sx, O, C, P1030 = Sx.loc[days], O.reindex(days), C.reindex(days), P1030.reindex(days)
legs = {"open->close": C / O - 1, "open->10:30": P1030 / O - 1, "10:30->close": C / P1030 - 1}
rk, k = Sx.rank(axis=1), Sx.notna().sum(axis=1)
w = pd.DataFrame(0.0, index=Sx.index, columns=Sx.columns)
w[rk.ge(k.values[:, None] - 1)] = 0.25
w[rk.le(2)] = -0.25
w = w[k >= 5].reindex(Sx.index).fillna(0.0)
print(f"days: {len(days)}  ({days.min().date()} -> {days.max().date()})")
for name, R in legs.items():
    g = (w * R.fillna(0)).sum(axis=1)
    print(f"{name:<13} gross {1e4*g.mean():+6.2f} bp/day  t={g.mean()/g.std()*np.sqrt(len(g)):+.2f}")
g = (w * legs["10:30->close"].fillna(0)).sum(axis=1)
r = lib.register("004_adr_catchup", "DIAG XS L/S price>=$20, delayed entry 10:30->close (1h data, ~2y)",
                 pd.Series(1.0, index=g.index), g - 6e-4, 0.0, notes="3bp/side; diagnostic on 2024-26 only, train/valid empty")
