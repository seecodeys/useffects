"""353 — SPY ex-dividend day vs its S&P 500 twins (IVV, VOO). Card 6, results/ideas_options_etf.md.

Hypothesis: on a fund's ex-date, the total-return spread vs a same-basket twin isolates the ex-day price
drop relative to the dividend (tax clienteles, dividend capture, deep-ITM call early exercise).
Spread return (ex fund minus twin) uses RAW Yahoo closes plus the known dividend on each fund's own ex-date:
    r_fund = (P_ex + D_fund_if_ex_today) / P_prev - 1.     Positive spread => drop smaller than dividend.
Ex-dates/amounts from the Yahoo chart API events=div. Ex-dates are announced in advance -> knowable at prior close.
Costs: 0.5bp/side per leg, two legs -> 1.0bp/side on the unit spread position.

PRE-SPECIFIED VARIANTS (fixed before looking at results):
  A: long SPY / short IVV, close before SPY ex-date -> ex-date close.
  B: long IVV / short SPY, close before IVV ex-date -> ex-date close.
  C: long VOO / short SPY, close before VOO ex-date -> ex-date close (2010+).
  D: overnight only (prev close -> ex-date open, dividend added) pooled over A-C events.
Sign is pre-specified: long the ex fund (positive if drop < dividend). Also report drop ratio dP/D.
"""
import os, sys, time
import numpy as np, pandas as pd, requests
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import lib

EXP = "353_spy_exdiv_twins"
COST = 1.0   # 0.5bp/side per leg x 2 legs
END = "2022-12-31"   # never look past valid


def divs(sym):
    path = os.path.join(lib.DATA, f"{sym}_divs.csv")
    if not os.path.exists(path):
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}"
        r = requests.get(url, params={"period1": 946684800, "period2": int(time.time()), "interval": "1d",
                                      "events": "div"}, headers=lib._HDR, timeout=30)
        res = r.json()["chart"]["result"][0]
        tz = res["meta"]["exchangeTimezoneName"]
        ev = res.get("events", {}).get("dividends", {})
        d = pd.DataFrame([{"Date": pd.Timestamp(v["date"], unit="s", tz="UTC").tz_convert(tz).strftime("%Y-%m-%d"),
                           "div": v["amount"]} for v in ev.values()])
        d.to_csv(path, index=False)
    d = pd.read_csv(path, parse_dates=["Date"]).groupby("Date")["div"].sum()
    return d


px = {s: lib.get(s) for s in ["SPY", "IVV", "VOO"]}
dv = {s: divs(s) for s in px}


def tr(s, kind):
    p = px[s]
    d = dv[s].reindex(p.index).fillna(0)
    num = (p["Close"] if kind == "cc" else p["Open"]) + d
    return num / p["Close"].shift(1) - 1


legs = {"A": ("SPY", "IVV"), "B": ("IVV", "SPY"), "C": ("VOO", "SPY")}
idx = px["SPY"].index
rows, ev_cc, ev_on = [], {}, {}
for v, (f, t) in legs.items():
    cc = (tr(f, "cc") - tr(t, "cc")).reindex(idx)
    on = (tr(f, "on") - tr(t, "on")).reindex(idx)
    exd = [d for d in dv[f].index if d in idx and d >= px[f].index[1] and d >= px[t].index[1]]
    exd = [d for d in exd if pd.notna(cc.get(d)) and pd.notna(on.get(d))]
    ev_cc[v], ev_on[v] = cc.loc[exd], on.loc[exd]
    prev = px[f]["Close"].shift(1).reindex(idx)
    for d in exd:
        if d > pd.Timestamp(END):
            continue
        D = dv[f][d]
        rows.append({"variant": v, "date": d.date(), "fund": f, "twin": t, "div": D, "yield_bp": 1e4 * D / prev[d],
                     "spread_cc_bp": 1e4 * cc[d], "spread_on_bp": 1e4 * on[d],
                     "drop_ratio_cc": 1 - cc[d] * prev[d] / D, "drop_ratio_on": 1 - on[d] * prev[d] / D,
                     "twin_ex_same_day": d in dv[t].index})

ev = pd.DataFrame(rows)
os.makedirs(os.path.join(lib.HERE, "results"), exist_ok=True)
ev.to_csv(os.path.join(lib.HERE, "results", "353_events.csv"), index=False)

res = {}
for v in legs:
    s = ev_cc[v]
    pos = pd.Series(1.0, index=s.index).reindex(idx).fillna(0)
    ret = s.reindex(idx).fillna(0)
    res[v] = lib.register(EXP, v, pos, ret, COST, notes=f"long {legs[v][0]} short {legs[v][1]} on {legs[v][0]} ex-date, cc")
pooled = pd.concat([ev_on[v] for v in legs]).groupby(level=0).mean()
pos = pd.Series(1.0, index=pooled.index).reindex(idx).fillna(0)
res["D"] = lib.register(EXP, "D", pos, pooled.reindex(idx).fillna(0), COST, notes="pooled A-C overnight close->ex open")

print(f"registry tests in this file: {lib.n_tests()}")
for v, r in res.items():
    print(v, lib.fmt(r), "n_tr", round(r["train_traded_%"] * r["train_n_days"] / 100), "n_va",
          round(r["valid_traded_%"] * r["valid_n_days"] / 100), "GATE" if lib.passes_gate(r) else "fail")

ev["period"] = np.where(pd.to_datetime(ev["date"]) <= pd.Timestamp("2014-12-31"), "train", "valid")
print(ev.groupby(["variant", "period"]).agg(n=("div", "size"), yld=("yield_bp", "mean"), cc=("spread_cc_bp", "mean"),
      cc_sd=("spread_cc_bp", "std"), on=("spread_on_bp", "mean"), ratio_cc_med=("drop_ratio_cc", "median"),
      ratio_on_med=("drop_ratio_on", "median"), twin_same=("twin_ex_same_day", "sum")).round(2).to_string())
ev["year"] = pd.to_datetime(ev["date"]).dt.year
print(ev.pivot_table(index="year", columns="variant", values="spread_cc_bp", aggfunc="sum").round(1).to_string())
