"""362 - Agency-MBS P&I payment-day reinvestment (idea card S2, results/ideas_settlement.md).

Mechanism: agency MBS pay P&I on fixed calendar days (Fannie/Freddie/UMBS the 25th, Ginnie II the 20th; next business
day if a holiday/weekend). Holders (index MBS funds, banks, Fed pre-QT) reinvest that cash into TBAs/pools within days.
Signals are pure calendar -> known in advance, so lag 0 (weights decided at close t earn close t -> t+1) is honest.
P = first trading session on/after the anchor day. Windows that hold ANY return day among the last 3 sessions of the
month are dropped (to avoid re-measuring the month-end bond bid, LOG 275-319). Close->close, 1bp/side per leg.
Hedge beta: OLS slope of daily MBB on IEF returns over the 60 sessions ending at the entry close (known then).

Pre-specified variants (fixed before seeing results, no tuning after):
  V1  MBB long, close P-1 -> close P+2, P = 25th (card a)
  V2  MBB - beta*IEF, same window, P = 25th (card c)
  V3  MBB long, close P-4 -> close P-1, P = 25th (card b, dealer pre-positioning)
  V4  MBB - beta*IEF, close P-1 -> close P+2, P = 20th (Ginnie II; GNMA ETF too short, so MBB proxy)
Each variant registered as per-window net returns booked on the exit day (pos=1 that day), costs already inside.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

END = "2022-12-31"                    # descriptive cut-off; holdout never looked at
mbb, ief = lib.get("MBB"), lib.get("IEF")
idx = mbb.index.intersection(ief.index)
R = pd.DataFrame({"MBB": mbb.cc.reindex(idx), "IEF": ief.cc.reindex(idx)}).dropna()
idx = R.index
ym = pd.Series(idx.to_period("M"), index=idx)
bwd = pd.Series(1, index=idx)[::-1].groupby(ym[::-1]).cumsum()[::-1]     # 1 = last session of month
last3 = (bwd <= 3).values
cov = R.MBB.rolling(60).cov(R.IEF); var = R.IEF.rolling(60).var(); beta = (cov / var)

def anchors(day):
    out = []
    for p in pd.period_range(idx[0], idx[-1], freq="M"):
        a = pd.Timestamp(p.year, p.month, day)
        k = idx.searchsorted(a)
        if k < len(idx) and idx[k].to_period("M") == p:
            out.append(k)
    return out

def windows(day, lo, hi):
    """entry close at P+lo, exit close at P+hi; return list of (entry_pos, exit_pos)"""
    w = []
    for k in anchors(day):
        e, x = k + lo, k + hi
        if e < 0 or x >= len(idx): continue
        if last3[e + 1:x + 1].any(): continue          # a held return day is in the month-end window
        w.append((e, x))
    return w

def run(name, day, lo, hi, hedge):
    W = pd.DataFrame(0.0, index=idx, columns=["MBB", "IEF"])
    for e, x in windows(day, lo, hi):
        b = beta.iloc[e] if hedge else 0.0
        if hedge and not np.isfinite(b): continue
        W.iloc[e:x, 0] = 1.0; W.iloc[e:x, 1] = -b      # held over closes e..x-1 -> earns returns e+1..x
    net = lib.backtest_weights(W, R, 1.0, lag=0)
    # book each window's net P&L (incl. exit cost on day after last hold? turnover at x is entry->hold; exit at x+1)
    held = (W.shift(1).abs().sum(axis=1) > 0)
    tid = (held & ~held.shift(1, fill_value=False)).cumsum().where(held | held.shift(1, fill_value=False))
    per = net.groupby(tid).sum()
    exit_day = pd.Series(net.index, index=net.index).groupby(tid).max()
    ret = pd.Series(0.0, index=idx); pos = pd.Series(0.0, index=idx)
    ret.loc[exit_day.values] = per.values; pos.loc[exit_day.values] = 1.0
    r = lib.register("362_mbs_pi_reinvest", name, pos, ret, 0.0,
                     notes="calendar-known, lag0; excl. windows touching last 3 sessions; 1bp/side/leg")
    print(f"{name}\n   {lib.fmt(r)}  gate={lib.passes_gate(r)}  n_train={int(pos.loc[:'2014'].sum())} "
          f"n_valid={int(pos.loc['2015':END].sum())}")
    # exposure-matched control: same-length unhedged/hedged average over non-event, non-month-end days (<=2022)
    hold_days = W.shift(1).abs().sum(axis=1) > 0
    leg = R.MBB - (beta.shift(1) * R.IEF if hedge else 0)
    ctrl = leg[(~hold_days) & (~pd.Series(last3, index=idx))]
    for nm, (a, b) in lib.PERIODS.items():
        ev = ret[pos > 0].loc[a:b]; c = ctrl.loc[a:b]
        L = hi - lo
        print(f"   {nm}: event net mean {1e4*ev.mean():+.2f}bp/trade (n={len(ev)}), control {L}-day gross "
              f"{1e4*L*c.mean():+.2f}bp -> timing value (gross) {1e4*(ev.mean()+2e-4*(1+hedge*abs(beta.mean())) - L*c.mean()):+.2f}bp")
    yr = ret[pos > 0].loc[:END].groupby(ret[pos > 0].loc[:END].index.year).sum() * 1e4
    print("   yearly net bp:", " ".join(f"{y}:{v:+.0f}" for y, v in yr.items()))
    return r

rows = [run("V1 MBB long P25-1->P25+2", 25, -1, 2, False),
        run("V2 MBB-beta*IEF P25-1->P25+2", 25, -1, 2, True),
        run("V3 MBB long P25-4->P25-1", 25, -4, -1, False),
        run("V4 MBB-beta*IEF P20-1->P20+2 (Ginnie)", 20, -1, 2, True)]
n = lib.n_tests()
print(f"registry size {n}; luck threshold sqrt(2 ln N) = {np.sqrt(2*np.log(max(n,2))):.2f}")
