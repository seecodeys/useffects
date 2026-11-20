"""234 - SPY dividend reinvestment: SPY pays its quarterly dividend around the last business day of Jan/Apr/Jul/Oct;
reinvested cash -> buying pressure. Pre-specified: long SPY open->close on the last trading day of Jan/Apr/Jul/Oct;
control: other month-ends. 0.5bp/side."""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
spy = lib.get("SPY"); idx = spy.index; ym = idx.to_period("M")
rb = pd.Series(1, index=idx)[::-1].groupby(ym[::-1]).cumsum()[::-1]
last = (rb == 1) & pd.Series(ym != ym[-1], index=idx)
pay = last & pd.Series(np.isin(idx.month, [1, 4, 7, 10]), index=idx)
for name, m in [("234 long SPY last day of Jan/Apr/Jul/Oct (pay months)", pay), ("234 control: long SPY other month-ends", last & ~pay)]:
    r = lib.register("234_div_reinvest", name, m.astype(float), spy.oc, 0.5)
    print(f"{name:<54} {lib.fmt(r)}  gate={lib.passes_gate(r)}")
print("registry size:", lib.n_tests())
