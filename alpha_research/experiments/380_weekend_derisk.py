"""380 - Weekend risk reduction by risk-managed desks / market makers.
Story: gamma and gap risk over the (untradeable) weekend make dealers and leveraged traders cut gross exposure into the
Friday close and re-add on Monday. If positioning follows the recent trend, Friday-afternoon de-risking trades AGAINST
the week's move (selling after up weeks, covering after down weeks) and Monday-morning re-risking trades WITH it.
Prior coverage (LOG.md): 001 weekday open->close longs (dead), 288-290 intraday flow on all days, 311 non-OPEX Friday
10:30-15:00 short (dead), 327D short SPY Friday before long weekends (dead), 057 Monday after a Friday drop (dead).
New here: Friday LAST TWO HOURS, conditional on the week's move, and Monday-morning re-risking conditional on last week.

"Big week": |week-to-date move| above the 67th percentile of the previous 52 weeks' values of the same measure (trailing,
known at decision time). s = sign(week move) on big weeks, 0 otherwise. Costs 0.5bp/side; all positions intraday or
calendar-known, so no close->close lag issue. CFD = 50/50 SPX + NSX (HistData 1-min, NY local time), 2010-14 train,
2015-22 valid. Daily = SPY 2000-22.

PRE-SPECIFIED VARIANTS (candidates):
 A  CFD: short Friday 14:00->15:59 every Friday (unconditional weekend de-risking).
 B  CFD: Friday 14:00->15:59 position -s, week move = last close of prior week -> Fri 14:00.
 C  CFD: Monday 09:30->11:00 position +s, week move = prior-prior week's last close -> prior week's last close.
 D  SPY daily: Friday open->close at -s, then Friday close->Monday open at +s (de-risk then re-risk round trip),
    week move = prior week's last close -> Thursday close (known before Friday's open). 4 sides of cost per event.
CONTROLS (registered, not candidates; give the exposure/timing benchmark):
 A0 CFD: short Mon-Thu 14:00->15:59 (is Friday different from any afternoon?)
 B0 CFD: same fade rule on Mon-Thu 14:00->15:59, move = close 5 sessions earlier -> 14:00 today.
"""
import os, sys, importlib
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
sys.path.insert(0, os.path.join(lib.HERE, "experiments"))
fx = importlib.import_module("003_overnight_fx")
EXP = "380_weekend_derisk"
END = "2022-12-31"
C1 = 1e-4                                      # round trip at 0.5bp/side

spy = lib.get("SPY").loc[:END]
D = spy.index


def prev_day(dates, back_days):
    """last trading day <= date - back_days (calendar)."""
    tgt = pd.DatetimeIndex(dates) - pd.Timedelta(days=back_days)
    i = D.searchsorted(tgt, side="right") - 1
    return pd.DatetimeIndex(np.where(i >= 0, D[np.clip(i, 0, None)], pd.NaT))


def big_sign(move, n=52, q=0.67):
    a = move.abs()
    thr = a.shift(1).rolling(n, min_periods=26).quantile(q)
    return (np.sign(move) * (a > thr)).where(thr.notna(), 0.0).fillna(0.0)


def reg(variant, ev, notes=""):
    """ev: per-event net return series indexed by date; registered as a daily series on SPY's calendar."""
    full = pd.Series(0.0, index=D)
    ev = ev.dropna()
    full.loc[ev.index] = ev
    lo = "2010-01-01" if variant.startswith(("A", "B", "C")) else "2000-01-01"
    full = full.loc[lo:END]
    traded = pd.Series((full != 0).astype(float), index=full.index)
    r = lib.register(EXP, variant, traded, full, 0.0, notes)
    out = []
    for nm, (a, b) in lib.PERIODS.items():
        e = ev.loc[a:b] * 1e4
        e = e[e != 0]
        out.append(f"{nm} n={len(e)} {e.mean():+.1f}bp")
    print(f"{variant:<44} {lib.fmt(r)} gate={lib.passes_gate(r)} | {' | '.join(out)}")
    return r


# ----------------------------------------------------------------- CFD
cfd = D[(D >= "2010-01-01")]
px = {s: fx.ny_minute_close(s, range(2010, 2023)) for s in ["SPXUSD", "NSXUSD"]}


def lvl(dates, hh, mm, tol=5):
    return {s: fx.level_at(p, pd.DatetimeIndex(dates), hh, mm, tol).values for s, p in px.items()}


def ret_between(d1, t1, d2, t2):
    a, b = lvl(d1, *t1), lvl(d2, *t2)
    return pd.Series(0.5 * sum(b[s] / a[s] - 1 for s in px), index=pd.DatetimeIndex(d2))


fri = cfd[cfd.dayofweek == 4]
pm_fri = ret_between(fri, (14, 0), fri, (15, 59))
wk_fri = ret_between(prev_day(fri, 5), (15, 59), fri, (14, 0))
s_fri = big_sign(wk_fri)
mon = cfd[cfd.dayofweek == 0]
am_mon = ret_between(mon, (9, 30), mon, (11, 0))
wk_mon = ret_between(prev_day(mon, 10), (15, 59), prev_day(mon, 3), (15, 59))
s_mon = big_sign(wk_mon)
oth = cfd[cfd.dayofweek < 4]
pm_oth = ret_between(oth, (14, 0), oth, (15, 59))
pos_o = np.clip(D.searchsorted(oth) - 5, 0, None)
wk_oth = ret_between(D[pos_o], (15, 59), oth, (14, 0))
s_oth = big_sign(wk_oth, n=208)                 # ~52 weeks of Mon-Thu observations

print(f"CFD events: Fridays {pm_fri.notna().sum()}, big-week Fridays {(s_fri != 0).sum()}, Mondays {am_mon.notna().sum()}")
reg("A short Fri 14:00-16:00", -pm_fri - C1)
reg("B fade big week Fri 14:00-16:00", (-s_fri * pm_fri - C1).where(s_fri != 0))
reg("C follow big prior week Mon 09:30-11:00", (s_mon * am_mon - C1).where(s_mon != 0))
reg("A0 control short Mon-Thu 14:00-16:00", -pm_oth - C1, "control")
reg("B0 control fade big 5d Mon-Thu 14:00-16:00", (-s_oth * pm_oth - C1).where(s_oth != 0), "control")

# ----------------------------------------------------------------- SPY daily
dfri = D[(D.dayofweek == 4) & (D >= "2000-01-01")]
idx = D.searchsorted(dfri)
nxt = D[np.clip(idx + 1, None, len(D) - 1)]
ok = (idx + 1 < len(D)) & (nxt.dayofweek == 0)       # plain weekend (skip holiday Mondays)
thu = D[idx - 1]
wk = pd.Series(spy.aC.loc[thu].values / spy.aC.loc[prev_day(dfri, 5)].values - 1, index=dfri)
sd = big_sign(wk)
oc = pd.Series(spy.oc.loc[dfri].values, index=dfri)
gp = pd.Series(np.where(ok, spy.gap.reindex(nxt).values, np.nan), index=dfri)
leg1, leg2 = (-sd * oc), (sd * gp)
reg("D SPY fade wk Fri o->c + follow Fri c->Mon o", (leg1 + leg2 - 2 * C1).where((sd != 0) & ok))

print("\nDescriptive (gross bp, mean / t), train | valid:")
def dsc(name, x):
    parts = []
    for nm, (a, b) in lib.PERIODS.items():
        e = x.loc[a:b].dropna(); e = e[e != 0] * 1e4
        parts.append(f"{e.mean():+6.1f} ({e.mean() / e.std() * np.sqrt(len(e)):+.1f}) n={len(e)}")
    print(f"  {name:<46} " + " | ".join(parts))
dsc("D leg1 -s * Fri open->close", leg1.where((sd != 0) & ok))
dsc("D leg2 +s * Fri close->Mon open", leg2.where((sd != 0) & ok))
dsc("SPY Fri close->Mon open (all plain weekends)", gp.where(ok))
allgap = spy.gap.loc["2000":END]; dsc("SPY all close->open", allgap[allgap.index.dayofweek != 0])
dsc("SPX/NSX Fri 14-16 after big UP week", pm_fri.where(s_fri > 0))
dsc("SPX/NSX Fri 14-16 after big DOWN week", pm_fri.where(s_fri < 0))
dsc("SPX/NSX Mon-Thu 14-16 all", pm_oth)
dsc("SPX/NSX Fri 14-16 all", pm_fri)

# yearly P&L of B and C (valid concentration check)
for nm, ev in [("B", (-s_fri * pm_fri - C1).where(s_fri != 0)), ("C", (s_mon * am_mon - C1).where(s_mon != 0)),
               ("D", (leg1 + leg2 - 2 * C1).where((sd != 0) & ok))]:
    y = (ev.dropna() * 1e4).groupby(ev.dropna().index.year).sum().loc[:2022]
    print(f"yearly sum bp {nm}: " + " ".join(f"{k}:{v:+.0f}" for k, v in y.items()))
print("registry size:", lib.n_tests())
