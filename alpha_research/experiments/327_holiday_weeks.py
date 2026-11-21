"""327 - Holiday-shortened weeks and long-weekend inventory flows (pre-holiday single session already dead: 026, 163).
Story: before multi-day closures dealers / market makers cut inventory and risk-takers de-risk (gap risk), then re-load
after the break; holiday weeks have thin participation.
Event calendar: a scheduled holiday = weekday with no SPY bar AND signals.nyse_closed(d) (drops unscheduled closures
such as 9/11 and Sandy, and pre-2022 'Juneteenth' which the helper wrongly flags but SPY traded). Calendar-known in
advance -> lag=0 is legitimate (no signal from a closing print).
  break    = gap between consecutive SPY sessions containing >=1 scheduled holiday weekday
  post     = first session after a break;  pre = last session before a break that also spans a weekend (long weekend)
  hweek    = all sessions of an ISO week (Mon-Fri) that contains a scheduled holiday weekday
PRE-SPECIFIED variants (max 4, signs fixed before looking):
  A  hweek: long SPY close->close over every session of holiday weeks (position from prior week's last close), 0.5bp/side
  B  post : long SPY open->close on the session after a holiday break (risk re-loading), 0.5bp/side
  C  post : short TLT open->close on the session after a holiday break (flight out of duration as risk re-loads), 1bp/side
  D  pre  : short SPY open->close on the last session before a long weekend (de-risking before the gap), 0.5bp/side
Close->next-open legs of (b) were not run (variant budget of 4). Timing value = net - mean_exposure * underlying return
(exposure-matched long of the same return type). Nothing printed beyond 2022-12-31; no holdout call."""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
from signals import nyse_closed

END = "2022-12-31"
spy, tlt = lib.get("SPY"), lib.get("TLT")
days = spy.index
hol = [d for d in pd.bdate_range(days[0], days[-1]) if d not in days and nyse_closed(d)]
hol = pd.DatetimeIndex(hol)
post = pd.Series(False, index=days); pre = pd.Series(False, index=days)
for a, b in zip(days[:-1], days[1:]):
    h = hol[(hol > a) & (hol < b)]
    if len(h):
        post[b] = True
        if (b - a).days >= 3 and any(x.dayofweek >= 5 for x in pd.date_range(a, b)[1:-1]):
            pre[a] = True
wk = days.to_period("W-SUN")
hweeks = set(pd.DatetimeIndex(hol).to_period("W-SUN"))
hweek = pd.Series([w in hweeks for w in wk], index=days)

tt = lambda s: s.mean() / s.std() * np.sqrt(len(s))
def report(name, pos, ret, cost, ev):
    pos, ret = pos.loc["2000":END].astype(float), ret.loc["2000":END]
    r = lib.register("327_holiday_weeks", name, pos, ret, cost, notes="calendar-known, lag0")
    net = lib.daily_pnl(pos, ret, cost)
    tv = net - pos.loc[:"2022"].mean() * ret.fillna(0)
    ev = ev.loc["2000":END]
    print(f"{name}: events {int(ev.loc[:'2014'].sum())}/{int(ev.loc['2015':].sum())}  {lib.fmt(r)}  "
          f"timing t {tt(tv.loc[:'2014']):+.2f} / {tt(tv.loc['2015':]):+.2f}  gate={lib.passes_gate(r)}")
    yr = net.groupby(net.index.year).sum() * 1e4
    print("   yearly bp:", " ".join(f"{y%100:02d}:{v:+.0f}" for y, v in yr.items()))
    return r

# A: weights decided at close of t apply to t+1 return; hold over sessions in holiday weeks
W = hweek.astype(float).shift(-1).fillna(0)              # set at prior close so the week's sessions are held
netA = lib.backtest_weights(W.to_frame("X"), spy.cc.to_frame("X"), 0.5, lag=0).loc["2000":END]
heldA = hweek.astype(float).loc[netA.index]
rA = lib.register("327_holiday_weeks", "327A hweek long SPY cc", pd.Series(1.0, index=netA.index), netA, 0.0,
                  notes="calendar-known, lag0")
tvA = netA - heldA.mean() * spy.cc.loc[netA.index]
evA = pd.Series(hweek.loc["2000":END].groupby(wk[days >= "2000"][:len(hweek.loc['2000':END])]).any().values,
                index=hweek.loc["2000":END].groupby(wk[days >= "2000"][:len(hweek.loc['2000':END])]).any().index.start_time)
print(f"327A hweek long SPY cc: weeks {int(evA.loc[:'2014'].sum())}/{int(evA.loc['2015':].sum())}  {lib.fmt(rA)}  "
      f"timing t {tt(tvA.loc[:'2014']):+.2f} / {tt(tvA.loc['2015':]):+.2f}  gate={lib.passes_gate(rA)}")
print("   yearly bp:", " ".join(f"{y%100:02d}:{v:+.0f}" for y, v in (netA.groupby(netA.index.year).sum()*1e4).items()))

report("327B post-break long SPY oc", post.astype(float), spy.oc, 0.5, post)
pt = post.reindex(tlt.index).fillna(False)
report("327C post-break short TLT oc", -pt.astype(float), tlt.oc, 1.0, pt)
report("327D pre-long-weekend short SPY oc", -pre.astype(float), spy.oc, 0.5, pre)
print("registry size:", lib.n_tests())
