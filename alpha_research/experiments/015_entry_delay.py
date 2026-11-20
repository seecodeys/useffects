"""015 - Execution reality check for 004 (ADR XS, price>=$20) and 006 (large-cap gap reversal):
how much of the open->close edge survives entering 5 / 10 / 30 minutes after the open?
Yahoo 5m bars (~60 days, ~40 sessions) -> descriptive only, tiny sample, NOT registered.
Signals are the same pre-open/at-open signals as in 004b/006.
"""
import os, sys, time, importlib, contextlib, io
import numpy as np, pandas as pd, requests
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
sys.path.insert(0, os.path.join(lib.HERE, "experiments"))


def bars5(sym):
    path = os.path.join(lib.DATA, f"{sym}_5m.csv")
    if not os.path.exists(path):
        r = requests.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}",
                         params={"range": "60d", "interval": "5m"}, headers=lib._HDR, timeout=30)
        res = r.json()["chart"]["result"][0]; q = res["indicators"]["quote"][0]
        ts = pd.to_datetime(res["timestamp"], unit="s", utc=True).tz_convert("America/New_York").tz_localize(None)
        pd.DataFrame({"ts": ts, "Open": q["open"], "Close": q["close"]}).dropna().to_csv(path, index=False)
        time.sleep(0.2)
    return pd.read_csv(path, parse_dates=["ts"])


def entry_prices(syms):
    out = {k: {} for k in ["0935", "0940", "1000"]}
    for s in syms:
        b = bars5(s); b["d"] = b.ts.dt.normalize()
        for k, (hh, mm) in {"0935": (9, 35), "0940": (9, 40), "1000": (10, 0)}.items():
            x = b[(b.ts.dt.hour == hh) & (b.ts.dt.minute == mm)].set_index("d")["Open"]
            out[k][s] = x
    return {k: pd.DataFrame(v) for k, v in out.items()}


def ls_weights(sig, long_low, q=None, top=None):
    if top:
        rk, k = sig.rank(axis=1), sig.notna().sum(axis=1)
        w = pd.DataFrame(0.0, index=sig.index, columns=sig.columns)
        w[rk.ge(k.values[:, None] - top + 1)] = 0.5 / top; w[rk.le(top)] = -0.5 / top
        return w[k >= 5].reindex(sig.index).fillna(0)
    rk = sig.rank(axis=1, pct=True); lo, hi = rk.le(q), rk.gt(1 - q)
    w = lo.astype(float).div(lo.sum(axis=1), axis=0) * 0.5 - hi.astype(float).div(hi.sum(axis=1), axis=0) * 0.5
    return (w if long_low else -w).fillna(0)


def report(name, sig, w_fn, syms):
    E = entry_prices(syms)
    days = E["0935"].dropna(how="all").index.intersection(sig.dropna(how="all").index)
    O = pd.DataFrame({s: lib.get(s)["Open"] for s in syms}).reindex(days)
    C = pd.DataFrame({s: lib.get(s)["Close"] for s in syms}).reindex(days)
    w = w_fn(sig.reindex(days))
    print(f"{name}: {len(days)} sessions {days.min().date()}..{days.max().date()}")
    for k, P in [("open", O)] + [(k, E[k].reindex(days)) for k in ["0935", "0940", "1000"]]:
        g = (w * (C / P - 1).fillna(0)).sum(axis=1)
        print(f"   entry {k:<5} -> close: gross {1e4*g.mean():+6.2f} bp/day  (t={g.mean()/g.std()*np.sqrt(len(g)):+.2f})")


with contextlib.redirect_stdout(io.StringIO()):
    m4 = importlib.import_module("004_adr_catchup"); S, Si, Y = m4.build()
px = pd.DataFrame({a: lib.get(a)["Close"].shift(1) for a in S}).reindex(S.index)
report("004 ADR XS catch-up (price>=$20, top2/bottom2)", S.where(px >= 20), lambda s: ls_weights(s, False, top=2), list(S.columns))
with contextlib.redirect_stdout(io.StringIO()):
    m6 = importlib.import_module("006_stock_gap_reversal"); OC, PC, GAP = m6.load_panel()
rg = GAP.sub(lib.get("SPY").gap.reindex(GAP.index), axis=0).where((PC >= 20) & GAP.abs().lt(0.3))
report("006 large-cap gap reversal (decile L/S)", rg, lambda s: ls_weights(s, True, q=0.1), list(OC.columns))
