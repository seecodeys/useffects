"""176 - Bank balance-sheet window dressing at quarter-ends (dealers shrink inventories for reporting/G-SIB scores):
Treasuries cheapen into quarter-end and recover after. TLT close->close, 1bp/side:
  a) short TLT over the last 3 sessions of each quarter, long over the first 3 sessions of the next (pre-specified)
  b) same, year-end only (December/January), where G-SIB constraints bind hardest
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
tlt = lib.get("TLT"); idx = tlt.index; ym = idx.to_period("M")
fwd = pd.Series(1, index=idx).groupby(ym).cumsum()
bwd = pd.Series(1, index=idx)[::-1].groupby(ym[::-1]).cumsum()[::-1]
qm = pd.Series(idx.month % 3 == 0, index=idx); q1 = pd.Series(idx.month % 3 == 1, index=idx)
for tag, mq, mn in [("a quarter-ends", qm, q1), ("b year-end only", pd.Series(idx.month == 12, index=idx), pd.Series(idx.month == 1, index=idx))]:
    want = pd.Series(0.0, index=idx)
    want[(bwd <= 3) & mq] = -1.0
    want[(fwd <= 3) & mn] = 1.0
    want[idx >= idx[-1] - pd.Timedelta(days=10)] = 0.0
    pos = want.shift(-1).fillna(0)                    # set at prior close
    net = lib.backtest_weights(pos.to_frame("T"), tlt.cc.to_frame("T"), 1.0).loc["2002-08":]
    r = lib.register("176_qend_balance_sheet", f"176{tag}: short TLT last 3 / long first 3 sessions", pd.Series(1.0, index=net.index), net, 0.0)
    print(f"176{tag:<16} {lib.fmt(r)}  gate={lib.passes_gate(r)}")
print("registry size:", lib.n_tests())
