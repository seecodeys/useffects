"""121 - [arXiv 2608.03703] US analog of LETF pre-positioning, Yahoo 1h bars (~2 years; DESCRIPTIVE, not registered,
overlaps the holdout). Signal = SPY/QQQ return from the open to 15:30. On days with |signal| > 1%:
  (i) last half hour 15:30->close in the SAME direction (pre-positioning for LETF close rebalancing)?
  (ii) close -> next open in the OPPOSITE direction (unwind)?
"""
import os, sys, time
import numpy as np, pandas as pd, requests
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib


def hourly(sym):
    p = os.path.join(lib.DATA, f"{sym}_1h.csv")
    if not os.path.exists(p):
        r = requests.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}", params={"range": "730d", "interval": "1h"},
                         headers=lib._HDR, timeout=30)
        res = r.json()["chart"]["result"][0]; q = res["indicators"]["quote"][0]
        ts = pd.to_datetime(res["timestamp"], unit="s", utc=True).tz_convert("America/New_York").tz_localize(None)
        pd.DataFrame({"ts": ts, "Open": q["open"], "Close": q["close"]}).dropna().to_csv(p, index=False)
    return pd.read_csv(p, parse_dates=["ts"])


for sym in ["SPY", "QQQ"]:
    h = hourly(sym); h["d"] = h.ts.dt.normalize()
    p1530 = h[(h.ts.dt.hour == 15) & (h.ts.dt.minute == 30)].set_index("d")["Open"]
    d = lib.get(sym)
    o, c = d["Open"], d["Close"]
    days = p1530.index.intersection(d.index)
    sig = (p1530.reindex(days) / o.reindex(days) - 1)
    last = c.reindex(days) / p1530.reindex(days) - 1
    nxt = (o.shift(-1) / c - 1).reindex(days)
    big = sig.abs() > 0.01
    s = np.sign(sig)
    li, nx = (s * last)[big] * 1e4, (-s * nxt)[big].dropna() * 1e4
    print(f"{sym}: {len(days)} days ({days.min().date()}..{days.max().date()}), big-move days {int(big.sum())}")
    print(f"   last half-hour in move direction: mean {li.mean():+.1f}bp  hit {100*(li>0).mean():.0f}%  t={li.mean()/li.std()*np.sqrt(len(li)):+.2f}")
    print(f"   next-open reversal:               mean {nx.mean():+.1f}bp  hit {100*(nx>0).mean():.0f}%  t={nx.mean()/nx.std()*np.sqrt(len(nx)):+.2f}")
