"""267 - International option-expiration replication of 001 (short on monthly expiry, dealer hedge unwind):
  a) Eurex DAX/Euro Stoxx options expire on the 3rd Friday -> short EXS1.DE open->close on the 3rd Friday
  b) Japan SQ (index options/futures special quotation) on the 2nd Friday -> short 1306.T open->close on the 2nd Friday
  c) Canada (Montréal Exchange) 3rd Friday -> short XIU.TO open->close
3bp/side."""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
for tag, sym, lo, hi in [("a Eurex 3rd Fri", "EXS1.DE", 15, 21), ("b Japan SQ 2nd Fri", "1306.T", 8, 14), ("c Canada 3rd Fri", "XIU.TO", 15, 21)]:
    d = lib.get(sym); idx = d.index
    ex = pd.Series((idx.dayofweek == 4) & (idx.day >= lo) & (idx.day <= hi), index=idx)
    r = lib.register("267_intl_opex", f"267{tag}: short {sym} open->close", -ex.astype(float), d.oc, 3.0)
    print(f"267{tag:<20} {lib.fmt(r)}  gate={lib.passes_gate(r)}")
print("registry size:", lib.n_tests())
