"""006b - Holdout look + delayed-entry (10:30) bounce check for 006 gap reversal @1bp/side."""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
sys.path.insert(0, os.path.join(lib.HERE, "experiments"))
import importlib
m = importlib.import_module("006_stock_gap_reversal")

OC, PC, GAP = m.load_panel()
spy = lib.get("SPY")
rg = GAP.sub(spy.gap.reindex(GAP.index), axis=0).where((PC >= 20) & GAP.abs().lt(0.3))
for c in [1.0, 2.0]:
    m.COST = c
    gross, net = m.decile_ls(rg, OC, True)
    h = lib.holdout_eval(m.EXP, f"006 gap reversal (cost {c}bp/side)", pd.Series(1.0, index=net.index), net, 0.0)
    print("HOLDOUT", c, {k: round(float(v), 2) for k, v in h.items()})
g = gross.loc["2023":]; print("holdout gross bp/day by year:", (g.groupby(g.index.year).mean() * 1e4).round(1).to_dict())

# delayed entry with Yahoo 1h bars (~730d)
import requests, time
def first_hour_close(sym):
    path = os.path.join(lib.DATA, f"{sym}_1h.csv")
    if not os.path.exists(path):
        r = requests.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}",
                         params={"range": "730d", "interval": "1h"}, headers=lib._HDR, timeout=30)
        res = r.json()["chart"]["result"][0]; q = res["indicators"]["quote"][0]
        ts = pd.to_datetime(res["timestamp"], unit="s", utc=True).tz_convert("America/New_York").tz_localize(None)
        pd.DataFrame({"ts": ts, "Open": q["open"], "Close": q["close"]}).dropna().to_csv(path, index=False)
        time.sleep(0.2)
    h = pd.read_csv(path, parse_dates=["ts"])
    f = h[(h.ts.dt.hour == 9) & (h.ts.dt.minute == 30)]
    return pd.Series(f.Close.values, index=f.ts.dt.normalize())
P1030 = pd.DataFrame({s: first_hour_close(s) for s in OC.columns})
days = P1030.dropna(how="all").index.intersection(rg.index)
O = pd.DataFrame({s: lib.get(s)["Open"] for s in OC.columns}).reindex(days)
C = pd.DataFrame({s: lib.get(s)["Close"] for s in OC.columns}).reindex(days)
sig = rg.reindex(days)
for name, R in {"open->close": C / O - 1, "open->10:30": P1030.reindex(days) / O - 1,
                "10:30->close": C / P1030.reindex(days) - 1}.items():
    m.COST = 0.0
    g, _ = m.decile_ls(sig, R, True)
    print(f"{name:<13} gross {1e4*g.mean():+6.2f} bp/day  t={g.mean()/g.std()*np.sqrt(len(g)):+.2f}  (n={len(g)})")
