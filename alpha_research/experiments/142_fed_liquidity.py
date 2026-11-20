"""142 - 'Liquidity' flows from Fed/Treasury balance-sheet behavior. Net liquidity = WALCL (Fed assets, weekly as-of
Wednesday, published Thursday 16:30 ET) - WTREGEN (Treasury General Account, weekly Wednesday) - RRPONTSYD (reverse repo,
daily; Wednesday value). Signal: weekly change in net liquidity. Position set at the FRIDAY close after publication
(safe lag), held to the next Friday close: long SPY if the change > 0, flat otherwise. Timing value vs exposure-matched
always-long, 0.5bp/side. Data from 2003 (RRP effectively 0 before 2013).
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
f = lambda i: pd.read_csv(os.path.join(lib.DATA, f"fred_{i}.csv"), parse_dates=["observation_date"], na_values=".").set_index("observation_date").iloc[:, 0].astype(float)
walcl, tga, rrp = f("WALCL"), f("WTREGEN"), f("RRPONTSYD")
weds = walcl.index
net = walcl - tga.reindex(weds).ffill().fillna(0) - (rrp.reindex(pd.date_range(weds[0], weds[-1])).ffill().reindex(weds).fillna(0) * 1000)
chg = net.diff()
spy = lib.get("SPY"); idx = spy.index
sig = pd.Series(np.nan, index=idx)
for w, c in chg.dropna().items():
    fri = w + pd.Timedelta(days=2)
    k = idx.searchsorted(fri)
    if k < len(idx):
        sig.iloc[k] = 1.0 if c > 0 else 0.0        # decided at that Friday's close (or next session if holiday)
pos = sig.ffill().fillna(0)
netp = lib.backtest_weights(pos.to_frame("S"), spy.cc.to_frame("S"), 0.5, lag=0)
held = pos.shift(1).fillna(0); out = pd.Series(0.0, index=netp.index)
for a, b in [lib.TRAIN, lib.VALID, lib.HOLDOUT]:
    out.loc[a:b] = netp.loc[a:b] - held.loc[a:b].mean() * spy.cc.loc[a:b].fillna(0)
out = out.loc["2003-01":]
r = lib.register("142_fed_liquidity", "142 long SPY week after net-liquidity increase, flat after decrease (timing value)",
                 pd.Series(1.0, index=out.index), out, 0.0)
print(f"142  {lib.fmt(r)}  gate={lib.passes_gate(r)}  time-long={100*held.loc['2003':'2022'].mean():.0f}%")
print("registry size:", lib.n_tests())
