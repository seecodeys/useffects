"""328 - Mega-cap earnings clusters and buyback blackouts. Mechanism: issuers stop buying back stock in the weeks
before earnings (blackout) and resume after; when the biggest index weights have reported, index-level buyback flow
resumes and hedgers/vol-sellers re-enter. 113 (flat calendar days 14-45 after quarter-end) was dead; this version
uses ACTUAL report dates.
Data: SEC EDGAR 8-K Item 2.02 filing dates (tools/edgar_earnings.py) for 15 long-lived mega-caps chosen with
hindsight (AAPL MSFT AMZN GOOGL XOM GE JNJ JPM WMT PG CVX IBM PFE INTC CSCO; META/NVDA not long-lived enough).
Report date per company per calendar quarter-end E = LAST 2.02 filing in (E+10d, E+55d] (drops interim updates /
pre-announcements at ~E+8 and late extra filings). Cluster date = first date on which >= 8 of 15 have reported;
cluster week = Mon-Fri week containing it. AFTER = 5 sessions following that week; BEFORE = 5 sessions before the
week begins (report dates are announced weeks in advance, so the BEFORE window is tradeable in practice).
Close->close, weights via lib.backtest_weights, 0.5bp/side, events 2005-2022.
Pre-specified variants (max 4, no tuning after):
  A long SPY over AFTER (lag-1: position from close of first session after the week; returns sessions 2-6)
  B long SPY over AFTER (lag-0, best-case: from close of last session of the cluster week) minus BEFORE (short SPY)
  C long QQQ - short SPY over AFTER (lag-1)
  D long QQQ - short SPY over AFTER minus same over BEFORE (lag-0)
Timing value: A's P&L minus the unconditional SPY drift over the same days (t-stat by period)."""
import os, sys, glob
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
sys.path.insert(0, os.path.join(lib.HERE, "tools"))
import edgar_earnings as ee

spy, qqq = lib.get("SPY"), lib.get("QQQ")
idx = spy.index[(spy.index >= "2004-10-01") & (spy.index <= "2022-12-31")]      # never past 2022
R = pd.DataFrame({"SPY": spy.cc, "QQQ": qqq.cc}).loc[idx]
d = pd.concat(ee.fetch(t) for t in ee.CIKS); d["fd"] = pd.to_datetime(d.filingDate)
d = d[(d.fd >= "2005-01-01") & (d.fd <= "2022-12-31")]

clusters = []
for E in pd.date_range("2004-12-31", "2022-09-30", freq="QE"):
    w = d[(d.fd > E + pd.Timedelta(days=10)) & (d.fd <= E + pd.Timedelta(days=55))]
    rep = w.groupby("ticker").fd.max().sort_values()
    if len(rep) < 8: continue
    cd = rep.iloc[7]                                                  # 8th company -> majority
    wk0 = cd - pd.Timedelta(days=cd.weekday()); wk1 = wk0 + pd.Timedelta(days=4)
    clusters.append((E, len(rep), cd, wk0, wk1))
cl = pd.DataFrame(clusters, columns=["qend", "n_rep", "cluster_date", "wk_start", "wk_end"])
pos = pd.Series(np.arange(len(idx)), index=idx)
after, before = pd.Series(0.0, index=idx), pd.Series(0.0, index=idx)   # weights decided at close t -> return t+1
for _, c in cl.iterrows():
    last_in_wk = pos[idx <= c.wk_end].iloc[-1]; first_in_wk = pos[idx >= c.wk_start].iloc[0]
    after.iloc[last_in_wk:last_in_wk + 5] = 1.0                       # earns returns of 5 sessions after the week
    before.iloc[first_in_wk - 6:first_in_wk - 1] = 1.0                # earns returns of 5 sessions before the week
print(f"clusters: {len(cl)}  median cluster lag (days after qend): {(cl.cluster_date - cl.qend).dt.days.median():.0f}"
      f"  min reported per q: {cl.n_rep.min()}")
print(cl.assign(lag=(cl.cluster_date - cl.qend).dt.days).lag.describe().round(1).to_dict())

ls = pd.DataFrame({"SPY": -1.0, "QQQ": 1.0}, index=[0])
def W(sig, mode):
    if mode == "spy": return sig.to_frame("SPY")
    return pd.DataFrame({"QQQ": sig, "SPY": -sig})
runs = [("A long SPY after cluster week (lag-1)", W(after, "spy"), 1),
        ("B SPY after(+) minus before(-) (lag-0 best-case)", W(after - before, "spy"), 0),
        ("C QQQ-SPY after cluster week (lag-1)", W(after, "rel"), 1),
        ("D QQQ-SPY after(+) minus before(-) (lag-0 best-case)", W(after - before, "rel"), 0)]
tt = lambda s: s.mean() / s.std() * np.sqrt(len(s))
for name, Wt, lag in runs:
    net = lib.backtest_weights(Wt, R[Wt.columns], 0.5, lag=lag).loc["2005":"2022"]
    r = lib.register("328_megacap_earnings", f"328{name}", pd.Series(1.0, index=net.index), net, 0.0)
    held = Wt.reindex(R.index).fillna(0).shift(1 + lag).fillna(0).loc[net.index]
    on = held.abs().sum(axis=1) > 0
    ev = net[on]
    yr = net.groupby(net.index.year).sum().mul(1e4).round(0)
    print(f"328{name}\n   {lib.fmt(r)}  | avg bp/held day train {1e4*ev.loc[:'2014'].mean():+.1f} valid {1e4*ev.loc['2015':].mean():+.1f}")
    if name.startswith("A"):
        tv = net - held["SPY"] * R.SPY.loc[net.index].mean()
        print(f"   timing t (vs unconditional SPY drift) {tt(tv.loc[:'2014']):+.2f} / {tt(tv.loc['2015':]):+.2f}")
    print("   yearly bp:", yr.astype(int).to_dict())
print("registry size:", lib.n_tests(), " luck t for best of N ~", round(np.sqrt(2*np.log(max(lib.n_tests(),2))), 2))
