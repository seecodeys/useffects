"""085 - OPEX-Friday tilt (001) via options: SELL a same-day ATM call on QQQ... we only have VIX/VIX9D for SPX, so use
      SPY with the 083 calibrated model (IV = VIX9D|VIX at prior close, x1.69 calibration from 083 TRAIN, 5% haircut):
      sell a same-day ATM SPY call on monthly OPEX Friday (bearish tilt). Model-priced (087: likely optimistic).
086 - Crash-fear sentiment: CBOE SKEW index (tail-insurance demand). Contrarian: when SKEW (prior close) is in the top
      20% of its TRAIN distribution, tail insurance is over-bought -> write puts (CBOE PUT index exposure) / long SPY.
      Timing value vs exposure-matched always-on; daily decision, 10bp per switch (PUT), 0.5bp (SPY).
"""
import os, sys, math
import numpy as np, pandas as pd
from scipy.stats import norm
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
spy = lib.get("SPY")
iv = lib.get("^VIX9D")["Close"].reindex(spy.index).combine_first(lib.get("^VIX")["Close"].reindex(spy.index)).shift(1) / 100
T = 6.5 / 24 / 365
S0, S1 = spy["Open"], spy["Close"]
prem = pd.Series(2 * norm.cdf(iv * math.sqrt(T) / 2) - 1, index=iv.index)
trn = slice(lib.TRAIN[0], lib.TRAIN[1])
k = 1.10 * (S1 / S0 - 1).abs().loc[trn].mean() / (2 * prem.loc[trn]).mean()
idx = spy.index
opex = pd.Series((idx.dayofweek == 4) & (idx.day >= 15) & (idx.day <= 21), index=idx)
sell_call = pd.Series(np.where(opex, 0.95 * k * prem - ((S1 - S0).clip(lower=0) / S0), 0.0), index=idx).loc["2000":]
if "--holdout" in sys.argv:
    pass
else:
  r = lib.register("085_opex_options", "085 sell same-day ATM SPY call on monthly OPEX Friday (calibrated model)",
                 pd.Series(1.0, index=sell_call.index), sell_call.fillna(0), 0.0)
  t = sell_call[opex.loc["2000":]]
  print(f"085 {lib.fmt(r)}  per-trade bp train {1e4*t.loc[trn].mean():+.1f} valid {1e4*t.loc[lib.VALID[0]:lib.VALID[1]].mean():+.1f}")

skew = lib.get("^SKEW")["Close"]
thr = skew.loc[trn].quantile(0.8)
put = pd.read_csv(os.path.join(lib.DATA, "cboe", "PUT.csv")); put["DATE"] = pd.to_datetime(put.DATE, format="%m/%d/%Y")
put = put.set_index("DATE").PUT
irx = lib.get("^IRX")["Close"] / 100 / 252
for name, ret, cost in ([] if "--holdout" in sys.argv else [("PUT", None, 10), ("SPY", spy.cc, 0.5)]):
    if ret is None:
        ii = put.index.intersection(spy.index); ret = (put.reindex(ii).pct_change() - irx.reindex(ii).ffill().fillna(0))
    ii = ret.index
    pos = (skew.reindex(ii).ffill().shift(1) > thr).astype(float)
    strat = pos * ret - pos.diff().abs().fillna(0) * cost / 1e4
    out = pd.Series(0.0, index=ii)
    for a, b in [lib.TRAIN, lib.VALID, lib.HOLDOUT]:
        out.loc[a:b] = strat.loc[a:b] - pos.loc[a:b].mean() * ret.loc[a:b]
    out = out.loc["2000":].fillna(0)
    rr = lib.register("086_skew_sentiment", f"086 {name} exposure only when SKEW top 20% (timing value)", pd.Series(1.0, index=out.index), out, 0.0)
    print(f"086 {name:<4} {lib.fmt(rr)}  gate={lib.passes_gate(rr)}  time-on={100*pos.loc['2000':'2022'].mean():.0f}%")
print("registry size:", lib.n_tests())

if "--holdout" in sys.argv:
    h = lib.holdout_eval("085_opex_options", "085 sell same-day ATM SPY call on OPEX Friday (calibrated model)",
                         pd.Series(1.0, index=sell_call.index), sell_call.fillna(0), 0.0)
    th = sell_call[opex.loc["2000":]].loc["2023":]
    print("HOLDOUT", {kk: round(float(v), 2) for kk, v in h.items()})
    print(f"holdout per-trade {1e4*th.mean():+.1f}bp, hit {100*(th>0).mean():.0f}%, n={len(th)}, worst {1e4*th.min():.0f}bp")
