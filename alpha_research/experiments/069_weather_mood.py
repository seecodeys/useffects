"""069 - Weather mood (Hirshleifer-Shumway 2003): sunny mornings in New York -> better mood -> higher returns.
Morning cloud cover = mean of 06:00-09:00 NY hourly cloud cover (Open-Meteo ERA5 archive, known before the open;
data/nyc_cloud.json, re-download with the URL in the docstring of the fetch command in LOG.md).
Deseasonalised by subtracting the TRAIN-period mean for that calendar month.
  H1 long SPY open->close on the sunniest 20% of mornings, short on the cloudiest 20% (TRAIN quintile thresholds)
  H2 pos = -sign(deseasonalised cloud cover) every day
0.5bp/side.
"""
import os, sys, json
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
j = json.load(open(os.path.join(lib.DATA, "nyc_cloud.json")))["hourly"]
h = pd.Series(j["cloud_cover"], index=pd.to_datetime(j["time"]), dtype=float)
morning = h[(h.index.hour >= 6) & (h.index.hour <= 9)].groupby(h[(h.index.hour >= 6) & (h.index.hour <= 9)].index.normalize()).mean()
d = lib.get("SPY").loc["2000":"2026-11-18"]
cc = morning.reindex(d.index)
clim = cc.loc[lib.TRAIN[0]:lib.TRAIN[1]].groupby(cc.loc[lib.TRAIN[0]:lib.TRAIN[1]].index.month).mean()
ds = cc - clim.reindex(cc.index.month).values
tr = ds.loc[lib.TRAIN[0]:lib.TRAIN[1]]
lo, hi = tr.quantile(0.2), tr.quantile(0.8)
H1 = pd.Series(np.where(ds <= lo, 1.0, np.where(ds >= hi, -1.0, 0.0)), index=d.index)
H2 = -np.sign(ds).fillna(0)
for name, pos in {"069 H1 long sunniest 20% / short cloudiest 20% mornings": H1,
                  "069 H2 -sign(deseasonalised morning cloud cover)": H2}.items():
    r = lib.register("069_weather", name, pos, d.oc, 0.5)
    print(f"{name:<58} {lib.fmt(r)}  gate={lib.passes_gate(r)}")
print("coverage:", cc.notna().mean().round(3), " registry size:", lib.n_tests())
