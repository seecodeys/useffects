"""321 - 'Vanna/charm' OPEX-week drift. Observation: in the week into monthly options expiry, as time decay and falling
implied vol shrink the delta of dealer-held short-put hedges, dealers buy back their short futures hedges -> upward drift,
strongest when implied vol was elevated (lots of put OI) and is falling. Signal fixed at the close of the Friday before
OPEX week (known in advance). Pre-specified, SPY close->close Mon-Thu of OPEX week (exit at Thursday close, before the
Friday AM settlement), 0.5bp/side:
 A unconditional long Mon-Thu of OPEX week
 B long only if VIX(prior Fri) > its 20-day mean (elevated hedging demand)
 C long only if VIX elevated AND VIX fell over the prior week (vol crush underway)"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
spy = lib.get("SPY"); vix = lib.get("^VIX")["Close"]; idx = spy.index
v = vix.reindex(idx).ffill()
opex = pd.Series(False, index=idx)
for d in idx[(idx.dayofweek == 4) & (idx.day >= 15) & (idx.day <= 21)]: opex[d] = True
# OPEX week Mon-Thu: sessions in the 4 calendar days before an OPEX Friday
wk = pd.Series(False, index=idx); fri_sig = pd.Series(np.nan, index=idx)
for f in idx[opex.values]:
    days = idx[(idx > f - pd.Timedelta(days=5)) & (idx < f)]
    wk[days] = True
    prev = idx[idx <= f - pd.Timedelta(days=7)]
    if len(prev): fri_sig[days] = prev[-1].value
elev = v > v.rolling(20).mean(); fall = v < v.shift(5)
cond_e = pd.Series([elev.get(pd.Timestamp(int(x)), False) if x == x else False for x in fri_sig], index=idx)
cond_f = pd.Series([fall.get(pd.Timestamp(int(x)), False) if x == x else False for x in fri_sig], index=idx)
V = {"A long Mon-Thu OPEX week": wk, "B ... if VIX elevated (prior Fri)": wk & cond_e,
     "C ... if VIX elevated & falling (prior Fri)": wk & cond_e & cond_f}
for name, held in V.items():
    w = held.astype(float).shift(-1).fillna(0)
    net = lib.backtest_weights(w.to_frame("X"), spy.cc.to_frame("X"), 0.5).loc["2000":]
    tv = net - held.astype(float).loc[net.index].mean() * spy.cc.loc[net.index]
    r = lib.register("321_vanna_opex", f"321{name}", pd.Series(1.0, index=net.index), net, 0.0)
    tt = lambda s: s.mean() / s.std() * np.sqrt(len(s))
    print(f"321{name}: {lib.fmt(r)}  timing t {tt(tv.loc[:'2014']):+.2f} / {tt(tv.loc['2015':'2022']):+.2f}  exposure {held.loc['2000':'2022'].mean():.2f}")
print("registry size:", lib.n_tests())
