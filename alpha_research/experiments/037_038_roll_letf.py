"""037 - Commodity index roll ('Goldman roll', business days 5-9 of each month): front-month crude pressured as index
      funds sell the expiring contract. Pre-specified: SHORT USO open->close on business days 5-9. 2bp/side. (USO 2006+)
038 - Leveraged-ETF rebalancing: LETFs must trade in the direction of the day's move near the close; on big-gap days
      the gap direction is a proxy. Pre-specified: follow the gap when |SPY gap| > 1% (SPY, QQQ), 0.5bp/side.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

uso = lib.get("USO")
idx = uso.index
bd = pd.Series(1, index=idx).groupby(idx.to_period("M")).cumsum()
roll = ((bd >= 5) & (bd <= 9)).astype(float)
r = lib.register("037_commodity_roll", "037 short USO business days 5-9", -roll, uso.oc, 2.0)
print(f"037 short USO roll window  {lib.fmt(r)}  gate={lib.passes_gate(r)}")
for sym in ["SPY", "QQQ"]:
    d = lib.get(sym).loc["2000":]
    big = d.gap.abs() > 0.01
    r = lib.register("038_letf_rebalance", f"038 {sym} follow gap when |gap|>1%", np.sign(d.gap).where(big, 0), d.oc, 0.5)
    print(f"038 {sym} follow big gap     {lib.fmt(r)}  gate={lib.passes_gate(r)}  days={int(big.sum())}")
print("registry size:", lib.n_tests())
