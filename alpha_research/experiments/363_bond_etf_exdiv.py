"""363 — Bond-ETF month-start ex-date: cum-day / ex-day residuals vs the distribution. Card S4, results/ideas_settlement.md.

Hypothesis (Elton-Gruber / tax clienteles): bond-ETF distributions are ordinary income, so taxable holders sell cum and
the ex-day price drop is smaller than the distribution (positive ex-day total-return residual), with cum-day selling
pressure (negative cum-day residual).
Returns use RAW Yahoo closes plus the actual dividend (Yahoo chart API events=div): r_t = (C_t + D_t)/C_{t-1} - 1.
Rate residual: e = r_ETF - beta * r_IEF, beta = 60-day rolling OLS of total returns, estimated with data through the
decision close (ex-2 for ex-day/ex+1 trades, ex-3 for the cum-day trade). Ex-dates are pre-announced (iShares schedule),
so the decision is taken at the close before entry and implemented with lib.backtest_weights(..., lag=1).
Basket: HYG, LQD, EMB, AGG, TLT (equal weight among those going ex that day), hedge with IEF. Costs 1bp/side every leg.
Events: all Yahoo ex-dates 2002-2022 (holdout 2023+ never touched).

PRE-SPECIFIED VARIANTS (fixed before seeing results; signs fixed):
  A: long basket - beta*IEF, close ex-1 -> close ex (ex-day residual; expect > 0).
  B: short basket + beta*IEF, close ex-2 -> close ex-1 (cum-day residual; expect cum-day < 0, so short earns > 0).
  C: long HYG - beta*IEF, close ex-1 -> close ex+1.
  D: long 50/50 HYG+EMB - beta*IEF, close ex-1 -> close ex+1.
Descriptive (not registered): (c) price-drop / dividend ratio per ETF (raw and rate-adjusted), and
the bond-bid audit: do ex-dates fall in the last-3-session window of IEF/TLT (LOG 275/276) and do Yahoo-adjusted
window returns (lib.get 'aC', as used there) match raw-price-plus-dividend window returns over 2002-22?
"""
import os, sys, time
import numpy as np, pandas as pd, requests
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import lib

EXP = "363_bond_etf_exdiv"
COST = 1.0
END = "2022-12-31"
BASKET = ["HYG", "LQD", "EMB", "AGG", "TLT"]
ALL = BASKET + ["IEF"]
OUT = os.path.join(lib.HERE, "results")


def divs(sym):
    path = os.path.join(lib.DATA, f"{sym}_divs.csv")
    if not os.path.exists(path):
        r = requests.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}",
                         params={"period1": 946684800, "period2": int(time.time()), "interval": "1d", "events": "div"},
                         headers=lib._HDR, timeout=30)
        res = r.json()["chart"]["result"][0]
        tz = res["meta"]["exchangeTimezoneName"]
        ev = res.get("events", {}).get("dividends", {})
        pd.DataFrame([{"Date": pd.Timestamp(v["date"], unit="s", tz="UTC").tz_convert(tz).strftime("%Y-%m-%d"),
                       "div": v["amount"]} for v in ev.values()]).sort_values("Date").to_csv(path, index=False)
        time.sleep(0.5)
    return pd.read_csv(path, parse_dates=["Date"]).groupby("Date")["div"].sum()


px = {s: lib.get(s) for s in ALL}
C = pd.DataFrame({s: px[s]["Close"] for s in ALL}).loc[:END]
AC = pd.DataFrame({s: px[s]["aC"] for s in ALL}).loc[:END]
D = pd.DataFrame({s: divs(s) for s in ALL}).reindex(C.index).fillna(0.0)
for s in ALL:   # every Yahoo ex-date should be a trading day
    miss = divs(s).loc[:END].index.difference(C.index)
    if len(miss):
        print(f"warning {s}: {len(miss)} ex-dates not trading days")
TR = (C + D) / C.shift(1) - 1          # raw price + actual dividend
YA = AC / AC.shift(1) - 1              # Yahoo adjusted (what lib.get 'cc' gives)
idx = C.index
pos_of = {d: i for i, d in enumerate(idx)}


def beta_at(sym, i):
    """60-day OLS beta of sym on IEF using TR returns ending at row i (inclusive)."""
    x = TR["IEF"].iloc[i - 59:i + 1]; y = TR[sym].iloc[i - 59:i + 1]
    ok = x.notna() & y.notna()
    if ok.sum() < 40:
        return np.nan
    return np.cov(y[ok], x[ok])[0, 1] / x[ok].var()


exd = {s: [d for d in D.index[D[s] > 0] if d >= pd.Timestamp("2002-08-01")] for s in ALL}

# ---------------------------------------------------------------- event table (descriptive + trade building)
rows = []
for s in ALL:
    for d in exd[s]:
        i = pos_of[d]
        if i < 70 or i + 1 >= len(idx):
            continue
        b2 = beta_at(s, i - 2) if s != "IEF" else 1.0
        b3 = beta_at(s, i - 3) if s != "IEF" else 1.0
        if np.isnan(C[s].iloc[i - 1]) or np.isnan(b2):
            continue
        ex_r = TR[s].iloc[i]; cum_r = TR[s].iloc[i - 1]; ief_ex = TR["IEF"].iloc[i]; ief_cum = TR["IEF"].iloc[i - 1]
        e_ex = ex_r - b2 * ief_ex if s != "IEF" else np.nan
        e_cum = cum_r - b3 * ief_cum if s != "IEF" else np.nan
        dv = D[s].iloc[i]; cp = C[s].iloc[i - 1]
        drop = cp - C[s].iloc[i]
        rows.append(dict(sym=s, date=d, div=dv, yld_bp=1e4 * dv / cp, beta=b2, ex_tr_bp=1e4 * ex_r,
                         ex_resid_bp=1e4 * e_ex, cum_resid_bp=1e4 * e_cum, drop_ratio=drop / dv,
                         adj_drop_ratio=1 - cp * e_ex / dv if s != "IEF" else np.nan,
                         yahoo_minus_tr_bp=1e4 * (YA[s].iloc[i] - ex_r)))
ev = pd.DataFrame(rows)
ev.to_csv(os.path.join(OUT, "363_events.csv"), index=False)


def tstat(x):
    x = pd.Series(x).dropna()
    return x.mean() / x.std() * np.sqrt(len(x)) if len(x) > 2 else np.nan


print("=== (c) per-ETF ex-day stats, 2002-22 (all ex-dates) ===")
print(f"{'sym':5}{'n':>5}{'div bp':>8}{'medDrop/D':>10}{'medAdj/D':>9}{'ex resid bp':>12}{'t':>6}{'cum resid bp':>13}{'t':>6}")
for s in ALL:
    e = ev[ev.sym == s]
    print(f"{s:5}{len(e):5d}{e.yld_bp.mean():8.1f}{e.drop_ratio.median():10.2f}{e.adj_drop_ratio.median():9.2f}"
          f"{e.ex_resid_bp.mean():12.2f}{tstat(e.ex_resid_bp):6.2f}{e.cum_resid_bp.mean():13.2f}{tstat(e.cum_resid_bp):6.2f}")
for per, (a, b) in [("train", lib.TRAIN), ("valid", lib.VALID)]:
    e = ev[(ev.sym != "IEF") & (ev.date >= a) & (ev.date <= b)]
    print(f"pooled basket {per}: n={len(e)} ex resid {e.ex_resid_bp.mean():+.2f}bp (t={tstat(e.ex_resid_bp):+.2f}), "
          f"cum resid {e.cum_resid_bp.mean():+.2f}bp (t={tstat(e.cum_resid_bp):+.2f})")

# ---------------------------------------------------------------- registered trades
R = TR.fillna(0.0)


def build(syms, entry_off, n_days, sign, beta_off):
    """W at decision row = (ex row + entry_off - 1); with lag=1 the position earns rows ex+entry_off .. +n_days-1."""
    W = pd.DataFrame(0.0, index=idx, columns=ALL)
    book = {}
    for d in sorted(set().union(*[exd[s] for s in syms])):
        i = pos_of[d]
        legs = [s for s in syms if d in exd[s] and not np.isnan(C[s].iloc[i - 1])]
        if i < 70 or i + n_days + 1 >= len(idx) or not legs:
            continue
        for k in range(n_days):
            r = i + entry_off - 2 + k      # decision row for return row i+entry_off+k (lag=1 -> +2)
            for s in legs:
                b = beta_at(s, i + entry_off - 2)
                if np.isnan(b):
                    continue
                W.iloc[r, ALL.index(s)] += sign / len(legs)
                W.iloc[r, ALL.index("IEF")] -= sign * b / len(legs)
        book[d] = (idx[i + entry_off], idx[min(i + entry_off + n_days, len(idx) - 1)])
    net = lib.backtest_weights(W, R, COST, lag=1)
    # attribute each event's net P&L (incl. exit cost the day after) to one booking date
    pnl = pd.Series(0.0, index=idx)
    for d, (a, b) in book.items():
        pnl[d] = net.loc[a:b].sum()
    pnl = pnl.loc["2002-08-01":]
    pos = (pnl != 0).astype(float)
    return pos, pnl, W


variants = {
    "A long basket-bIEF ex-day": (BASKET, 0, 1, +1),
    "B short basket+bIEF cum-day": (BASKET, -1, 1, -1),
    "C long HYG-bIEF ex-1..ex+1": (["HYG"], 0, 2, +1),
    "D long HYG+EMB-bIEF ex-1..ex+1": (["HYG", "EMB"], 0, 2, +1),
}
summ = []
for name, (syms, off, nd, sg) in variants.items():
    pos, pnl, W = build(syms, off, nd, sg, None)
    row = lib.register(EXP, name, pos, pnl, 0.0, notes="TR raw+div, net 1bp/side all legs, event P&L booked on ex-date")
    gate = lib.passes_gate(row)
    nt = int(pos.loc[:lib.TRAIN[1]].sum()); nv = int(pos.loc[lib.VALID[0]:].sum())
    print(f"{name:34s} {lib.fmt(row)}  n_train={nt} n_valid={nv}  gate={gate}")
    # gross (0 cost) per-event bp for context
    summ.append(dict(variant=name, **{k: row[k] for k in ["train_t", "valid_t", "valid_sharpe", "train_bp_per_trade",
                                                          "valid_bp_per_trade"]}, gate=gate, n_train=nt, n_valid=nv))
pd.DataFrame(summ).to_csv(os.path.join(OUT, "363_summary.csv"), index=False)

# baseline: hedged basket residual on all days (exposure-matched, unconditional)
allres = []
for s in BASKET:
    b = TR[s].rolling(60).cov(TR["IEF"]) / TR["IEF"].rolling(60).var()
    allres.append((TR[s] - b.shift(2) * TR["IEF"]).loc["2002-08":])
allres = pd.concat(allres)
print(f"baseline hedged residual, all basket days 2002-22: {1e4*allres.mean():+.2f}bp/day")

# ---------------------------------------------------------------- bond-bid audit (LOG 275/276), descriptive
print("\n=== Bond-bid audit: last-3-session window, Yahoo adjusted (aC) vs raw+dividend, 2002-08..2022 ===")
audit = []
for s in ["IEF", "TLT"]:
    ym = pd.Series(idx.to_period("M"), index=idx)
    bwd = pd.Series(1, index=idx)[::-1].groupby(ym[::-1]).cumsum()[::-1]
    inwin = (bwd <= 3) & (idx >= pd.Timestamp("2002-08-01"))
    win = pd.DataFrame({"ym": ym[inwin], "ya": YA[s][inwin], "tr": TR[s][inwin], "ex": D[s][inwin] > 0})
    g = win.groupby("ym").agg(ya=("ya", lambda x: np.prod(1 + x) - 1), tr=("tr", lambda x: np.prod(1 + x) - 1),
                               ex=("ex", "any"))
    g["diff_bp"] = 1e4 * (g.ya - g.tr)
    exm = g[g.ex]
    print(f"{s}: months={len(g)}, months with an ex-date inside window={len(exm)} "
          f"({', '.join(str(p) for p in exm.index)})")
    print(f"   window mean: adjusted {1e4*g.ya.mean():+.2f}bp (t={tstat(g.ya):+.2f})  raw+div {1e4*g.tr.mean():+.2f}bp "
          f"(t={tstat(g.tr):+.2f});  |diff| mean {g.diff_bp.abs().mean():.3f}bp, max {g.diff_bp.abs().max():.2f}bp")
    if len(exm):
        print("   ex-in-window months: adjusted vs raw+div bp:",
              "; ".join(f"{p} {1e4*r.ya:+.1f}/{1e4*r.tr:+.1f}" for p, r in exm.iterrows()))
    for per, (a, b) in [("train", lib.TRAIN), ("valid", lib.VALID)]:
        h = g[(g.index.start_time >= a) & (g.index.start_time <= b)]
        print(f"   {per}: adjusted {1e4*h.ya.mean():+.2f}bp t={tstat(h.ya):+.2f} | raw+div {1e4*h.tr.mean():+.2f}bp "
              f"t={tstat(h.tr):+.2f}")
    # price-only window (what you'd get ignoring dividends) for the months with ex in window
    po = (C[s] / C[s].shift(1) - 1)[inwin].groupby(ym[inwin]).apply(lambda x: np.prod(1 + x) - 1)
    print(f"   price-only window mean {1e4*po.mean():+.2f}bp (gap vs raw+div {1e4*(g.tr.mean()-po.mean()):+.2f}bp)")
    audit.append(g.assign(sym=s))
pd.concat(audit).to_csv(os.path.join(OUT, "363_bondbid_audit.csv"))
e = ev[ev.sym.isin(["IEF", "TLT"])]
print(f"ex-day Yahoo-adjusted minus raw+div return (IEF/TLT, all ex-days): mean {e.yahoo_minus_tr_bp.mean():+.3f}bp, "
      f"max |.| {e.yahoo_minus_tr_bp.abs().max():.2f}bp")
print("N registered tests in this registry:", lib.n_tests())
