"""314 - Option hedging demand (CBOE put/call volume ratios, cdn.cboe.com volume_and_call_put_ratios; equity 2006-2019,
index 2006-2019). Flow mechanism: a burst of put buying leaves dealers short puts and hedged short the underlying; as
those hedges decay/unwind (and fear-driven hedges roll off) dealers buy back -> above-normal returns after hedging
spikes. Ratio z vs 60 sessions; published after the close -> used from the next close (lag=1). SPY close->close 0.5bp.
 A long 5 sessions when z(equity P/C) > 1.5, else flat   B same with index P/C   C sign: -sign(z equity P/C) |z|>1, 1 day
Valid sample ends 2019 (data stops)."""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
def rd(n):
    x = pd.read_csv(os.path.join(lib.DATA, f"cboe_{n}.csv"), skiprows=2)
    x.columns = [c.strip() for c in x.columns]
    x.index = pd.to_datetime(x.DATE.str.strip(), format="mixed"); return pd.to_numeric(x["P/C Ratio"], errors="coerce").sort_index()
z = lambda s: (s - s.rolling(60, min_periods=40).mean()) / s.rolling(60, min_periods=40).std()
ze, zi = z(rd("equitypc")), z(rd("indexpc"))
spy = lib.get("SPY"); idx = spy.index[(spy.index >= max(ze.dropna().index[0], zi.dropna().index[0])) & (spy.index <= "2019-10-04")]
hold5 = lambda trig: trig.astype(float).reindex(idx).fillna(0).rolling(5, min_periods=1).max()
V = {"A long 5d when z(equity P/C)>1.5": hold5(ze > 1.5), "B long 5d when z(index P/C)>1.5": hold5(zi > 1.5),
     "C -sign(z equity P/C) |z|>1, 1d": (-np.sign(ze)).where(ze.abs() > 1, 0.0).reindex(idx).fillna(0)}
for name, w in V.items():
    net = lib.backtest_weights(w.to_frame("X"), spy.cc.loc[idx].to_frame("X"), 0.5, lag=1)
    held = w.shift(2).fillna(0); tv = net - held.mean() * spy.cc.loc[idx]
    r = lib.register("314_put_call", f"314{name}", pd.Series(1.0, index=net.index), net, 0.0, notes="data 2006-2019")
    tt = lambda s: s.mean() / s.std() * np.sqrt(len(s))
    print(f"314{name}: {lib.fmt(r)}  mean pos {held.mean():+.2f}  timing t {tt(tv.loc[:'2014']):+.2f} / {tt(tv.loc['2015':]):+.2f}")
print("registry size:", lib.n_tests())
