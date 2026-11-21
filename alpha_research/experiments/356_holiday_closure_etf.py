"""356 — Country-ETF excess move during home-market holiday closures: fade at the reopen (idea card 8,
results/ideas_options_etf.md). LOG 028 only tested Japan fiscal year-end; the China/HK closure note was never run.

Mechanism: while the home market is shut, US-listed country ETFs trade without a live basket (no creation/redemption
arbitrage). Their move vs SPY during the closure is unanchored; when the home market reopens it either confirms
the move or the ETF gives part of it back.

Pairs (ETF -> home index used to detect closures): FXI,EWH -> ^HSI; ASHR -> 000001.SS (2014+); EWT -> ^TWII;
EWY -> ^KS11; EWJ -> ^N225; EWZ -> ^BVSP.
Closure day: a US trading day (SPY bar) with no home-index bar on that date. Closure window: run of >=2 consecutive
US trading days that are closure days (runs > 8 days treated as data gaps and dropped). Reopen known in advance from
the published exchange holiday calendar (here proxied by the next home bar).
X = ETF cumulative cc return over the window - beta * SPY cumulative return, beta = trailing 250-day OLS of ETF cc on
SPY cc ending before the window. Trade: -sign(X) * (ETF - beta*SPY) held from the US close of the LAST closure day to
the next US close (home market has reopened by then). Pooled: equal weight across simultaneous events.
Costs: 2bp/side ETF leg, 0.5bp/side SPY leg.

Pre-specified variants (fixed before seeing results):
  A: lag-1 realistic — X measured through the close of the PENULTIMATE closure day (signal one close before entry),
     |X| > 1%, fade, 1-day hold. All pairs.
  B: A restricted to Lunar New Year closures (window starting Jan 15 - Feb 28, homes HSI/SSE/TWII/KS11).
  C: A with |X| > 2%.
  D: best-case same-close — X through the close of the last closure day (entry at that same close), |X| > 1%.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import lib

PAIRS = [("FXI", "^HSI"), ("EWH", "^HSI"), ("ASHR", "000001.SS"), ("EWT", "^TWII"),
         ("EWY", "^KS11"), ("EWJ", "^N225"), ("EWZ", "^BVSP")]
LNY_HOMES = {"^HSI", "000001.SS", "^TWII", "^KS11"}
END = "2022-12-31"          # never look past VALID

spy = lib.get("SPY")
spy_r = spy["cc"].loc[:END]
usd = spy_r.index


def events_for(etf, home):
    e = lib.get(etf)["cc"].loc[:END]
    h = lib.get(home)
    if home == "000001.SS":
        e = e.loc["2014-01-01":]
    dates = usd[(usd >= e.index[0]) & (usd >= h.index[0] + pd.Timedelta(days=400))]
    closed = pd.Series(~dates.isin(h.index), index=dates)
    beta_df = pd.concat([e, spy_r], axis=1, keys=["e", "s"]).dropna()
    cov = beta_df["e"].rolling(250).cov(beta_df["s"])
    beta = (cov / beta_df["s"].rolling(250).var()).shift(1)
    out = []
    i, n = 0, len(dates)
    while i < n:
        if not closed.iloc[i]:
            i += 1; continue
        j = i
        while j + 1 < n and closed.iloc[j + 1]:
            j += 1
        L = j - i + 1
        if 2 <= L <= 8 and j + 1 < n and i > 0:
            win = dates[i:j + 1]
            b = beta.asof(dates[i - 1])
            if np.isfinite(b) and win.isin(e.index).all():
                x1 = (1 + e.loc[win[:-1]]).prod() - 1 - b * ((1 + spy_r.loc[win[:-1]]).prod() - 1)
                x0 = (1 + e.loc[win]).prod() - 1 - b * ((1 + spy_r.loc[win]).prod() - 1)
                out.append(dict(etf=etf, home=home, start=win[0], last=win[-1], penult=win[-2],
                                exit=dates[j + 1], days=L, beta=b, X_lag1=x1, X_lag0=x0,
                                lny=(home in LNY_HOMES and ((win[0].month == 1 and win[0].day >= 15)
                                                            or win[0].month == 2))))
        i = j + 1
    return out


ev = pd.DataFrame([r for p in PAIRS for r in events_for(*p)])
R = pd.concat({t: lib.get(t)["cc"] for t in [p[0] for p in PAIRS] + ["SPY"]}, axis=1).loc[:END]


def run(name, sel, xcol, thr, lag):
    sub = ev[sel & (ev[xcol].abs() > thr)].copy()
    sigdate = sub["penult"] if lag == 1 else sub["last"]
    We = pd.DataFrame(0.0, index=R.index, columns=R.columns)   # ETF legs
    Ws = pd.DataFrame(0.0, index=R.index, columns=R.columns)   # SPY hedge leg
    cnt = sigdate.value_counts()
    for (_, r), d in zip(sub.iterrows(), sigdate):
        s = -np.sign(r[xcol]) / cnt[d]
        We.loc[d, r["etf"]] += s
        Ws.loc[d, "SPY"] += -s * r["beta"]
    net = lib.backtest_weights(We, R, 2.0, lag=lag) + lib.backtest_weights(Ws, R, 0.5, lag=lag)
    gross = lib.backtest_weights(We, R, 0.0, lag=lag) + lib.backtest_weights(Ws, R, 0.0, lag=lag)
    active = (We.shift(1 + lag).abs().sum(axis=1) > 0).astype(float)
    row = lib.register("356_holiday_closure_etf", name, active, net, cost_bp=0.0,
                       notes=f"{len(sub)} events; fade X ({xcol}) |X|>{thr}; lag={lag}")
    tr = sub[sub["last"] <= "2014-12-31"]; va = sub[sub["last"] >= "2015-01-01"]
    print(f"{name}: events train {len(tr)} valid {len(va)}  {lib.fmt(row)}  gate={lib.passes_gate(row)}")
    for per, (a, b) in lib.PERIODS.items():
        g, nn, ac = gross.loc[a:b], net.loc[a:b], active.loc[a:b] > 0
        print(f"   {per}: gross {1e4*g[ac].mean():+.1f}bp/day-active, net {1e4*nn[ac].mean():+.1f}bp, "
              f"active days {ac.sum()}")
    yr = net[active > 0].groupby(net[active > 0].index.year).agg(["sum", "count"])
    print("   yearly net %:", " ".join(f"{y}:{100*s:+.1f}({c})" for y, (s, c) in yr.iterrows()))
    return row, sub


if __name__ == "__main__":
    print("closure windows by home (2002-2022):")
    print(ev.groupby("home").agg(n=("days", "size"), mean_len=("days", "mean"),
                                 sd_X0=("X_lag0", "std")).round(3))
    allp = pd.Series(True, index=ev.index)
    rows = {}
    rows["A"], subA = run("A_lag1_X1pct", allp, "X_lag1", 0.01, 1)
    rows["B"], _ = run("B_lag1_LNY", ev["lny"], "X_lag1", 0.01, 1)
    rows["C"], _ = run("C_lag1_X2pct", allp, "X_lag1", 0.02, 1)
    rows["D"], _ = run("D_lag0_bestcase", allp, "X_lag0", 0.01, 0)
    # descriptive: reopen-day hedged ETF return vs X (all events, no threshold), slope through 2022
    nxt = []
    for _, r in ev.iterrows():
        nxt.append(R.loc[r["exit"], r["etf"]] - r["beta"] * R.loc[r["exit"], "SPY"])
    ev["reopen_excess"] = nxt
    for col in ["X_lag1", "X_lag0"]:
        for per, (a, b) in lib.PERIODS.items():
            s = ev[(ev["last"] >= a) & (ev["last"] <= b)]
            sl = np.polyfit(s[col], s["reopen_excess"], 1)[0]
            print(f"slope reopen_excess on {col} [{per}, n={len(s)}]: {sl:+.3f}  corr {s[col].corr(s['reopen_excess']):+.3f}")
    ev.to_csv(os.path.join(os.path.dirname(__file__), "..", "results", "356_events.csv"), index=False)
    print("registry rows:", lib.n_tests())
