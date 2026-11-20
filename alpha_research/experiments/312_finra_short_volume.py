"""312 - Off-exchange short volume as a flow signal (the 'DIX' idea: most FINRA-reported short volume is market makers
selling to customers who are buying; a high share = heavy customer buying absorbed off-exchange -> positive future
returns as dealers hedge/rebuild). Data: FINRA daily short-volume files (tools/finra_shvol.py), free only from 2019, so
there is NO 2000-14 sample: splits are 2019-20 / 2021-22 (reported manually), 2023+ untouched.
Ratio = short / total volume (share-weighted over current S&P 500 members; also SPY's own). z vs trailing 60 sessions.
File is published ~18:00 ET -> used from the NEXT day's close (lag=1). Pre-specified (SPY close->close, 0.5bp/side):
 A long 5 sessions when z(S&P ratio) > 1, else flat     B sign(z S&P ratio) when |z|>1, 1-day hold
 C long 5 sessions when z(SPY's own ratio) > 1"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
f = pd.read_csv(os.path.join(lib.DATA, "finra_shvol.csv"), dtype={"date": str})
f.index = pd.to_datetime(f.date); f = f.sort_index()
z = lambda s: (s - s.rolling(60, min_periods=40).mean()) / s.rolling(60, min_periods=40).std()
zs, zy = z(f.spx_short / f.spx_total), z(f.spy_short / f.spy_total)
spy = lib.get("SPY"); idx = spy.index[(spy.index >= f.index[0]) & (spy.index <= "2022-12-31")]
hold5 = lambda trig: trig.astype(float).reindex(idx).fillna(0).rolling(5, min_periods=1).max()
V = {"A long 5d when z(S&P short ratio)>1": hold5(zs > 1),
     "B sign(z S&P short ratio) |z|>1, 1d": np.sign(zs).where(zs.abs() > 1, 0.0).reindex(idx).fillna(0),
     "C long 5d when z(SPY short ratio)>1": hold5(zy > 1)}
for name, w in V.items():
    net = lib.backtest_weights(w.to_frame("X"), spy.cc.loc[idx].to_frame("X"), 0.5, lag=1)
    held = w.shift(2).fillna(0)
    tv = net - held.mean() * spy.cc.loc[idx]
    r = lib.register("312_finra_short_volume", f"312{name}", pd.Series(1.0, index=net.index), net, 0.0,
                     notes="2019-22 only; no train sample")
    out = []
    for a, b in [("2019", "2020"), ("2021", "2022")]:
        x, y = net.loc[a:b], tv.loc[a:b]
        out.append(f"{a}-{b[2:]}: Sharpe {x.mean()/x.std()*np.sqrt(252):+.2f} t {x.mean()/x.std()*np.sqrt(len(x)):+.2f}, timing t {y.mean()/y.std()*np.sqrt(len(y)):+.2f}")
    print(f"312{name}: mean pos {held.mean():+.2f} | " + " | ".join(out))
print("registry size:", lib.n_tests())
