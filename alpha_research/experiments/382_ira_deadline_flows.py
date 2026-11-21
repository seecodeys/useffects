"""382 - Prior-year IRA/HSA contribution deadline flows.
Story: US savers may make prior-year IRA/HSA contributions until the federal tax-filing deadline (~Apr 15; shifted to
the next business day on weekends / DC Emancipation Day; Jul 15 in 2020, May 17 in 2021). Contribution cash lands in
brokerage/retirement accounts in the weeks before and is invested -> buying pressure into the deadline. Calendar-known,
so positions are set at the prior close with lag 0 (no price information used). Prior related tests: 177 (short SPY 3
sessions into quarterly tax dates, weak) and 305 (TGA tax-date drain, dead) - both about tax PAYMENTS, not the
retirement-contribution window, which is tested here. 1 event/year -> 3 variants only, per-year results reported.

Windows: deadline session d = first session on/after the deadline date. PRE = the 10 sessions ending with d (inclusive,
the last day contributions count); POST = the 5 sessions after d. 'Other months' comparison window = the same offsets
around the session on/after the 15th of each other month of that year.

Pre-specified variants (written before any results):
 A. Long SPY close->close over PRE; timing value vs exposure-matched always-long SPY (per period); 0.5bp/side.
 B. Long SPY over PRE+POST (15 sessions) in the deadline month, short 1/11 SPY over the same 15-session window in each
    of the other 11 months of the year (exposure-matched calendar spread: 'deadline window vs same window elsewhere');
    0.5bp/side.
 C. Long VTI (broad-fund proxy, 2001-06+) over PRE; timing value vs exposure-matched always-long VTI; 0.5bp/side.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

EXP = "382_ira_deadline"

# Federal individual filing (= prior-year IRA contribution) deadlines
DEADLINES = {2000: "2000-04-17", 2001: "2001-04-16", 2002: "2002-04-15", 2003: "2003-04-15", 2004: "2004-04-15",
             2005: "2005-04-15", 2006: "2006-04-17", 2007: "2007-04-17", 2008: "2008-04-15", 2009: "2009-04-15",
             2010: "2010-04-15", 2011: "2011-04-18", 2012: "2012-04-17", 2013: "2013-04-15", 2014: "2014-04-15",
             2015: "2015-04-15", 2016: "2016-04-18", 2017: "2017-04-18", 2018: "2018-04-17", 2019: "2019-04-15",
             2020: "2020-07-15", 2021: "2021-05-17", 2022: "2022-04-18", 2023: "2023-04-18", 2024: "2024-04-15",
             2025: "2025-04-15", 2026: "2026-04-15"}
NPRE, NPOST = 10, 5


def window(idx, date, npre, npost):
    j = idx.searchsorted(pd.Timestamp(date), side="left")
    if j >= len(idx) or j - npre + 1 < 0:
        return pd.DatetimeIndex([])
    return idx[j - npre + 1: min(j + 1 + npost, len(idx))]


def deadline_pos(idx, npre, npost):
    p = pd.Series(0.0, index=idx)
    for y, d in DEADLINES.items():
        w = window(idx, d, npre, npost)
        if len(w) == npre + npost:   # full window only
            p.loc[w] = 1.0
    return p


def spread_pos(idx, npre, npost):
    p = pd.Series(0.0, index=idx)
    for y, d in DEADLINES.items():
        dm = pd.Timestamp(d).month
        w = window(idx, d, npre, npost)
        if len(w) != npre + npost:
            continue
        p.loc[w] += 1.0
        for m in range(1, 13):
            if m == dm:
                continue
            wo = window(idx, pd.Timestamp(y, m, 15), npre, npost)
            if len(wo) == npre + npost:
                p.loc[wo] -= 1.0 / 11
    return p


def timing_value(pos, cc, cost):
    """pos(t)=exposure held over session t (decided at close t-1). Net minus exposure-matched buy&hold per period."""
    W = pos.shift(-1, fill_value=0.0).to_frame("S")
    net = lib.backtest_weights(W, cc.to_frame("S"), cost)
    held = pos.reindex(net.index).fillna(0); out = pd.Series(0.0, index=net.index)
    for a, b in [lib.TRAIN, lib.VALID, lib.HOLDOUT]:
        out.loc[a:b] = net.loc[a:b] - held.loc[a:b].mean() * cc.loc[a:b].fillna(0)
    return net, out


def reg(variant, series, held, notes=""):
    s = series.loc["2000":]
    r = lib.register(EXP, variant, pd.Series(1.0, index=s.index), s, 0.0, notes)
    trades = {k: int((held.loc[a:b].abs().diff().fillna(held.loc[a:b].abs()) > 0).sum()) for k, (a, b) in lib.PERIODS.items()}
    bp = {k: 1e4 * s.loc[a:b].sum() / max(trades[k], 1) for k, (a, b) in lib.PERIODS.items()}
    print(f"{variant[:62]:62s} {lib.fmt(r)} gate={lib.passes_gate(r)} events={trades} net_bp/event(train,valid)="
          f"{bp['train']:+.1f},{bp['valid']:+.1f}")
    return r


def yearly(s):
    s = s.loc["2000":"2022"]
    return (s.groupby(s.index.year).sum() * 1e4).round(0).astype(int).to_dict()


spy = lib.get("SPY"); idx = spy.index; cc = spy.cc
pA = deadline_pos(idx, NPRE, 0)
_, tvA = timing_value(pA, cc, 0.5)
reg("382A long SPY 10 sessions into IRA deadline (timing value)", tvA, pA)

pB = spread_pos(idx, NPRE, NPOST)
netB = lib.backtest_weights(pB.shift(-1, fill_value=0.0).to_frame("S"), cc.to_frame("S"), 0.5)
reg("382B SPY deadline pre10+post5 vs same window other months", netB, (pB > 0).astype(float))

vti = lib.get("VTI"); pC = deadline_pos(vti.index, NPRE, 0)
_, tvC = timing_value(pC, vti.cc, 0.5)
reg("382C long VTI 10 sessions into IRA deadline (timing value)", tvC, pC)

# per-year components (<=2022 only): raw SPY pre-10 and post-5 log-ish sums, and registered series by year
pre = deadline_pos(idx, NPRE, 0); full = deadline_pos(idx, NPRE, NPOST); post = full - pre
print("\nper-year (bp, <=2022): raw SPY pre10 | raw SPY post5 | A timing value | B spread | C VTI timing value")
yA, yB, yC = yearly(tvA), yearly(netB), yearly(tvC)
for y in range(2000, 2023):
    a, b = f"{y}", f"{y}"
    r_pre = 1e4 * cc.loc[a:b][pre.loc[a:b] == 1].sum(); r_post = 1e4 * cc.loc[a:b][post.loc[a:b] == 1].sum()
    print(f"{y}: {r_pre:+6.0f} | {r_post:+6.0f} | {yA.get(y,0):+5d} | {yB.get(y,0):+5d} | {yC.get(y,0):+5d}")
for k, (a, b) in lib.PERIODS.items():
    c = cc.loc[a:b]
    print(f"{k}: mean SPY cc pre10 {1e4*c[pre.loc[a:b]==1].mean():+.2f}bp/day, post5 "
          f"{1e4*c[post.loc[a:b]==1].mean():+.2f}bp/day, all {1e4*c.mean():+.2f}bp/day")
print("registry size:", lib.n_tests(), " luck t ~", round(np.sqrt(2*np.log(max(lib.n_tests(), 2))), 2))
