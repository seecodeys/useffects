"""177 - Tax-payment flows: individual estimated-tax / annual deadlines (Apr 15, Jun 15, Sep 15, Jan 15; corporate
estimated taxes share Apr/Jun/Sep/Dec 15). Investors sell to raise cash; Treasury's account drains reserves.
Pre-specified: short SPY close->close over the 3 sessions ending the deadline day (or next business day if a holiday);
timing value vs exposure-matched always-long... (short = negative exposure, so report raw net and t). 0.5bp/side."""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
spy = lib.get("SPY"); idx = spy.index
want = pd.Series(0.0, index=idx)
for y in range(1999, 2027):
    for m in [1, 4, 6, 9, 12]:
        dl = pd.Timestamp(y, m, 15)
        k = idx.searchsorted(dl)
        if 3 <= k < len(idx):
            want.iloc[k - 2:k + 1] = -1.0
pos = want.shift(-1).fillna(0)
net = lib.backtest_weights(pos.to_frame("S"), spy.cc.to_frame("S"), 0.5).loc["2000":]
r = lib.register("177_tax_dates", "177 short SPY 3 sessions into tax deadlines (Jan/Apr/Jun/Sep/Dec 15)", pd.Series(1.0, index=net.index), net, 0.0)
print(f"177  {lib.fmt(r)}  gate={lib.passes_gate(r)}")
print("registry size:", lib.n_tests())
