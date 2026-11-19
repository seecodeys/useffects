"""013c - Confirmation of 013 on other equity ETFs + intraday timing.
Same pre-specified rule: on the last trading day of a (completed) month,
pos = -sign(SPY MTD - TLT MTD through prior close), held open->close.
Instruments: IWM, QQQ, DIA (1bp/side). Gate-passers get one holdout look.
Timing (descriptive, Yahoo 1h, ~35 month-ends): SPY open->12:30 vs 12:30->close vs last hour.
"""
import os, sys, time
import numpy as np, pandas as pd, requests
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
import contextlib, io
with contextlib.redirect_stdout(io.StringIO()):
    src = open(os.path.join(lib.HERE, "experiments", "013_month_end_rebalance.py")).read().split("S = {")[0]
    exec(src)
EXP = "013_month_end_rebalance"
for sym in ["IWM", "QQQ", "DIA"]:
    d = lib.get(sym)
    pos = sig.where(last1, 0).reindex(d.index).fillna(0).loc["2002-08-01":]
    y = d.oc.loc[pos.index]
    r = lib.register(EXP, f"013c {sym} last day of month: fade SPY-TLT MTD gap", pos, y, 1.0)
    ok = lib.passes_gate(r)
    print(f"{sym} {lib.fmt(r)}  gate={ok}")
    if ok:
        h = lib.holdout_eval(EXP, f"013c {sym} last day fade SPY-TLT MTD gap (1bp)", pos, y, 1.0)
        print("   HOLDOUT", {k: round(float(v), 2) for k, v in h.items()})

# intraday timing on SPY (1h bars)
path = os.path.join(lib.DATA, "SPY_1h.csv")
if not os.path.exists(path):
    r = requests.get("https://query1.finance.yahoo.com/v8/finance/chart/SPY", params={"range": "730d", "interval": "1h"},
                     headers=lib._HDR, timeout=30)
    res = r.json()["chart"]["result"][0]; q = res["indicators"]["quote"][0]
    ts = pd.to_datetime(res["timestamp"], unit="s", utc=True).tz_convert("America/New_York").tz_localize(None)
    pd.DataFrame({"ts": ts, "Open": q["open"], "Close": q["close"]}).dropna().to_csv(path, index=False)
h = pd.read_csv(path, parse_dates=["ts"]); h["d"] = h.ts.dt.normalize()
p = lambda hh: h[(h.ts.dt.hour == hh) & (h.ts.dt.minute == 30)].set_index("d")
o, c1230, c1530 = p(9)["Open"], p(12)["Open"], p(15)["Open"]
cl = s["Close"]
days = [x for x in last1[last1].index if x in o.index and x in c1530.index]
pos_d = sig.loc[days]
rows = pd.DataFrame({"open->12:30": (c1230.reindex(days) / o.reindex(days) - 1),
                     "12:30->15:30": (c1530.reindex(days) / c1230.reindex(days) - 1),
                     "15:30->close": (cl.reindex(days) / c1530.reindex(days) - 1)}).mul(pos_d, axis=0) * 1e4
print(f"SPY month-ends with 1h data: {len(days)}; mean signed bp by segment:")
print(rows.mean().round(1).to_string()); print("hit% by segment:", (rows > 0).mean().mul(100).round(0).to_dict())
print("registry size:", lib.n_tests())
