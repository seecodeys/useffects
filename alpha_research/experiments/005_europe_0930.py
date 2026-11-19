"""005 - Europe at 9:30 ET, measured properly (HistData 1-min CFD bars, 2010-2025).
eu_ret   = index level at 09:29 NY on D / level at 15:59 NY on prior US day P - 1
           (DAX/CAC CFDs trade through the US afternoon; falls back to last bar within 60 min)
eu_late  = 09:29 / 08:29 NY on D - 1
Hypotheses (pre-specified), targets SPY plus the matching country ETF:
  H1 follow: sign(eu_ret)    H2 catch-up: sign(gap_hat(eu_ret) - gap)    H3 follow last hour: sign(eu_late)
TRAIN here is only 2010-2014 (data start). Costs 0.5bp/side SPY, 1bp/side EWG/EZU.
"""
import os, sys
import numpy as np
import pandas as pd
import statsmodels.api as sm
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
sys.path.insert(0, os.path.join(lib.HERE, "experiments"))
import importlib
fx = importlib.import_module("003_overnight_fx")

EXP = "005_europe_0930"
JOBS = {"GRXEUR": [("SPY", 0.5), ("EWG", 1.0)], "FRXEUR": [("SPY", 0.5), ("EZU", 1.0)]}
for cfd, targets in JOBS.items():
    px = fx.ny_minute_close(cfd, range(2010, 2026))
    for sym, cost in targets:
        d = lib.get(sym).loc[str(px.index[0].date()):"2025-12-31"]
        D = d.index
        P = pd.DatetimeIndex(pd.Series(D, index=D).shift(1)).fillna(D[0])
        df = pd.DataFrame({"y": d.oc, "gap": d.gap,
                           "eu": fx.level_at(px, D, 9, 29) / pd.Series(fx.level_at(px, P, 15, 59, 60).values, index=D) - 1,
                           "eu_late": fx.level_at(px, D, 9, 29) / fx.level_at(px, D, 8, 29) - 1}).dropna()
        df = df[df.eu.abs() < 0.15]
        print(f"{cfd}->{sym}: {len(df)} days {df.index[0].date()}..{df.index[-1].date()}  corr(eu,gap)={df.eu.corr(df.gap):.2f} "
              f"corr(eu,y)={df.eu.corr(df.y):+.3f} corr(eu_late,y)={df.eu_late.corr(df.y):+.3f}")
        tr = df.loc[:lib.TRAIN[1]]
        m = sm.OLS(tr.gap, sm.add_constant(tr.eu)).fit()
        df["surprise"] = m.predict(sm.add_constant(df.eu)) - df.gap
        S = {"H1 follow Europe since US close": np.sign(df.eu),
             "H2 catch-up to Europe-implied gap": np.sign(df.surprise),
             "H3 follow Europe last hour": np.sign(df.eu_late)}
        for name, pos in S.items():
            r = lib.register(EXP, f"{cfd}->{sym} {name}", pos, df.y, cost)
            print(f"   {name:<34} {lib.fmt(r)}  gate={lib.passes_gate(r)}")
print("registry size:", lib.n_tests())
