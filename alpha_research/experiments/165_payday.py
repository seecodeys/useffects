"""165 - [Quantpedia 'payday anomaly'] semi-monthly paychecks on the 15th -> retirement contributions invested on the 16th.
Pre-specified: long SPY close->close over the first trading session on or after the 16th (position set at the prior
close); timing value vs exposure-matched always-long; 0.5bp/side."""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
spy = lib.get("SPY"); idx = spy.index; ym = idx.to_period("M")
first16 = pd.Series(idx.day >= 16, index=idx).astype(int).groupby(ym).cumsum() == 1
pos = first16.shift(-1, fill_value=False).astype(float)
net = lib.backtest_weights(pos.to_frame("S"), spy.cc.to_frame("S"), 0.5)
held = pos.shift(1).fillna(0); out = pd.Series(0.0, index=net.index)
for a, b in [lib.TRAIN, lib.VALID, lib.HOLDOUT]:
    out.loc[a:b] = net.loc[a:b] - held.loc[a:b].mean() * spy.cc.loc[a:b].fillna(0)
r = lib.register("165_payday", "165 long SPY on first session >= 16th (timing value)", pd.Series(1.0, index=out.loc['2000':].index), out.loc["2000":], 0.0)
print(f"165  {lib.fmt(r)}  gate={lib.passes_gate(r)}")
print("registry size:", lib.n_tests())
