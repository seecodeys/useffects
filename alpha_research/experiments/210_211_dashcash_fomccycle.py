"""210 - Dash for cash (Etula, Rinne, Suominen, Vaittinen, RFS 2020): institutions sell to raise cash for month-end
      payment obligations -> low returns in the days just before the month's last trading day, reversal at the turn.
      Pre-specified: short SPY close->close over sessions -3 and -2 (the 3rd- and 2nd-to-last of the month); timing as raw
      short (it's a negative-exposure bet). 0.5bp/side.
211 - FOMC cycle (Cieslak, Morse, Vissing-Jorgensen, JF 2019): equity returns concentrate in EVEN weeks of the FOMC
      cycle (weeks 0, 2, 4, 6 counting from the scheduled meeting day). Long SPY in even weeks, flat in odd weeks; timing
      value vs exposure-matched always-long. Week k = days [meeting + 5k - 1, meeting + 5k + 3] in trading days.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
spy = lib.get("SPY"); idx = spy.index; r = spy.cc
ym = idx.to_period("M")
bwd = pd.Series(1, index=idx)[::-1].groupby(ym[::-1]).cumsum()[::-1]
complete = pd.Series(ym != ym[-1], index=idx)
want = -((bwd.isin([2, 3])) & complete).astype(float)
pos = want.shift(-1).fillna(0)
net = lib.backtest_weights(pos.to_frame("S"), r.to_frame("S"), 0.5).loc["2000":]
res = lib.register("210_dash_for_cash", "210 short SPY sessions -3,-2 before month-end", pd.Series(1.0, index=net.index), net, 0.0)
print(f"210  {lib.fmt(res)}  gate={lib.passes_gate(res)}")

f = pd.read_csv(os.path.join(lib.HERE, "tools", "fomc_dates.csv"), parse_dates=["date"])
meet = [d for d in f[f.type == "scheduled"].date if d in idx]
week = pd.Series(np.nan, index=idx)
for m in meet:
    k = idx.get_loc(m)
    for wk in range(0, 8):
        a, b = k + 5 * wk - 1, k + 5 * wk + 3
        if a >= 0 and b < len(idx):
            week.iloc[a:b + 1] = week.iloc[a:b + 1].fillna(wk)
even = (week % 2 == 0).astype(float).where(week.notna(), 0.0)
pos = even.shift(-1).fillna(0)
netp = lib.backtest_weights(pos.to_frame("S"), r.to_frame("S"), 0.5)
held = pos.shift(1).fillna(0); out = pd.Series(0.0, index=netp.index)
for a, b in [lib.TRAIN, lib.VALID, lib.HOLDOUT]:
    out.loc[a:b] = netp.loc[a:b] - held.loc[a:b].mean() * r.loc[a:b].fillna(0)
res = lib.register("211_fomc_cycle", "211 long SPY in even FOMC-cycle weeks (timing value)", pd.Series(1.0, index=out.loc['2000':].index), out.loc["2000":], 0.0)
print(f"211  {lib.fmt(res)}  gate={lib.passes_gate(res)}  time-long={100*held.loc['2000':'2022'].mean():.0f}%")
print("registry size:", lib.n_tests())
