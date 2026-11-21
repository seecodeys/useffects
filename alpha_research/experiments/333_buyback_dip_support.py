"""333 - Buyback desks support dips, but only in the open window (idea card B10). Mechanism: 10b5-1 / discretionary
buyback grids buy more on down days, but issuers are mostly out of the market in pre-earnings blackouts. So SPY dip
rebounds should be stronger when most big issuers are in their OPEN window than in the peak blackout weeks.
113 (flat calendar proxy) and 328 (EDGAR report dates, no dip conditioning) were dead; this adds the dip interaction.
Blackout calendar (EDGAR 8-K Item 2.02 dates, data/edgar_202_*.csv via tools/edgar_earnings.py, 15 hindsight
mega-caps): for company i and calendar quarter-end E, report date = LAST 2.02 filing in (E+10d, E+55d] (as 328;
fallback E+25d if missing; for E=2022-12-31 no 2023 data is read -> blackout runs to end-2022). Company i is in
blackout on calendar days [E-14d, report date] (report day inclusive; buying resumes next day). Day t is BLACKOUT if
>= 8 of 15 companies are in blackout, else OPEN. Report dates are pre-announced, so the status is knowable at t
(mild look-ahead on exact dates; acknowledged).
Trigger: SPY close->close return <= threshold at close t. Position: long SPY from close t+1 to close t+4 (3 sessions;
lib.backtest_weights lag=1, weights=1 on t..t+2, overlapping triggers capped at 1). Split by status ON THE TRIGGER DAY t.
Events 2005-2022 only; train 2005-14, valid 2015-22; 0.5bp/side.
Pre-specified variants (max 4, no tuning after):
  A open-window dips (<= -1.5%), long 3 sessions lag-1
  B blackout dips (<= -1.5%), long 3 sessions lag-1
  C interaction: A minus B (long after open-window dips, short after blackout dips), -1.5%
  D interaction as C with threshold -1.0%
Timing value = net P&L minus exposure-matched always-long (held weight x mean SPY daily return of the same period);
t-stat by period. The test is C/D (and A's timing value)."""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
sys.path.insert(0, os.path.join(lib.HERE, "tools"))
import edgar_earnings as ee

spy = lib.get("SPY")
idx = spy.index[(spy.index >= "2004-10-01") & (spy.index <= "2022-12-31")]      # never past 2022
R = spy.cc.loc[idx].to_frame("SPY")
d = pd.concat(ee.fetch(t) for t in ee.CIKS); d["fd"] = pd.to_datetime(d.filingDate)
d = d[(d.fd >= "2004-10-01") & (d.fd <= "2022-12-31")]
tick = sorted(d.ticker.unique())

cal = pd.date_range("2004-12-01", "2022-12-31", freq="D")
inbl = pd.DataFrame(False, index=cal, columns=tick)
for E in pd.date_range("2004-12-31", "2022-12-31", freq="QE"):
    w = d[(d.fd > E + pd.Timedelta(days=10)) & (d.fd <= E + pd.Timedelta(days=55))]
    rep = w.groupby("ticker").fd.max()
    for t in tick:
        rd = rep.get(t, E + pd.Timedelta(days=25)) if E < pd.Timestamp("2022-12-31") else pd.Timestamp("2022-12-31")
        inbl.loc[E - pd.Timedelta(days=14):rd, t] = True
nbl = inbl.sum(axis=1).reindex(idx)
blackout = nbl >= 8
print(f"companies: {len(tick)}  share of sessions in BLACKOUT 2005-22: {blackout.loc['2005':].mean():.2f}")

pos_i = np.arange(len(idx))
def sig(mask):
    s = pd.Series(0.0, index=idx)
    for k in pos_i[mask.values]:
        s.iloc[k:k + 3] = 1.0          # W at t,t+1,t+2 -> with lag=1 earns returns t+2..t+4 (close t+1 -> close t+4)
    return s
ret = R.SPY
valid_days = idx >= "2005-01-01"
def trig(th): return (ret <= th) & pd.Series(valid_days, index=idx)
sA, sB = sig(trig(-0.015) & ~blackout), sig(trig(-0.015) & blackout)
sAd, sBd = sig(trig(-0.010) & ~blackout), sig(trig(-0.010) & blackout)
for th in (-0.015, -0.010):
    for per, (a, b) in {"train": ("2005", "2014"), "valid": ("2015", "2022")}.items():
        tr = trig(th).loc[a:b]; bl = blackout.loc[a:b]
        print(f"events {th:+.3f} {per}: open {int((tr & ~bl).sum())}  blackout {int((tr & bl).sum())}")

runs = [("A open-window dips <=-1.5%, long 3d lag-1", sA),
        ("B blackout dips <=-1.5%, long 3d lag-1", sB),
        ("C open minus blackout dips <=-1.5% lag-1", sA - sB),
        ("D open minus blackout dips <=-1.0% lag-1", sAd - sBd)]
tt = lambda s: s.mean() / s.std() * np.sqrt(len(s))
out = []
for name, s in runs:
    W = s.to_frame("SPY")
    net = lib.backtest_weights(W, R, 0.5, lag=1).loc["2005":"2022"]
    r = lib.register("333_buyback_dip_support", f"333{name}", pd.Series(1.0, index=net.index), net, 0.0)
    held = W.reindex(R.index).fillna(0).shift(2).fillna(0).SPY.loc[net.index]
    tv = pd.concat([net.loc[a:b] - held.loc[a:b] * ret.loc[a:b].mean()
                    for a, b in (("2005", "2014"), ("2015", "2022"))])
    on = held != 0
    yr = net.groupby(net.index.year).sum().mul(1e4).round(0).astype(int)
    print(f"333{name}\n   {lib.fmt(r)}  gate={lib.passes_gate(r)}")
    print(f"   timing t {tt(tv.loc[:'2014']):+.2f} / {tt(tv.loc['2015':]):+.2f}   timing bp/held-day "
          f"{1e4*tv.loc[:'2014'][on.loc[:'2014']].mean():+.1f} / {1e4*tv.loc['2015':][on.loc['2015':]].mean():+.1f}")
    print("   yearly bp:", yr.to_dict())
print("registry size:", lib.n_tests(), " luck t for best of N ~", round(np.sqrt(2*np.log(max(lib.n_tests(), 2))), 2))
