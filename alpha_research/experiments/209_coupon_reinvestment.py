"""209 - Treasury coupon/maturity cash reinvestment: refunding settlements, large coupon payments and maturities fall on the
15th of Feb/May/Aug/Nov; holders reinvest -> Treasury demand. Pre-specified: long TLT close->close over the 2 sessions
ending on that 15th (or the next business day), 1bp/side. Control: same on the 15th of other months."""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
tlt = lib.get("TLT"); idx = tlt.index
for tag, months in [("refunding months (Feb/May/Aug/Nov)", [2, 5, 8, 11]), ("control: other months", [1, 3, 4, 6, 7, 9, 10, 12])]:
    want = pd.Series(0.0, index=idx)
    for y in range(2002, 2027):
        for m in months:
            k = idx.searchsorted(pd.Timestamp(y, m, 15))
            if 2 <= k < len(idx):
                want.iloc[k - 1:k + 1] = 1.0
    pos = want.shift(-1).fillna(0)
    net = lib.backtest_weights(pos.to_frame("T"), tlt.cc.to_frame("T"), 1.0).loc["2002-08":]
    r = lib.register("209_coupon_reinvest", f"209 long TLT 2 sessions into the 15th, {tag}", pd.Series(1.0, index=net.index), net, 0.0)
    print(f"209 {tag:<36} {lib.fmt(r)}  gate={lib.passes_gate(r)}")
print("registry size:", lib.n_tests())
