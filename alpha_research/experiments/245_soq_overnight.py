"""245 - Monthly SPX AM settlement (SOQ) at Friday's open: dealer hedge unwinds into the settlement print. SPY / QQQ
overnight from Thursday's close to the OPEX Friday open; direction chosen on TRAIN (sign of mean), 1bp/side."""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
for sym in ["SPY", "QQQ"]:
    d = lib.get(sym); idx = d.index
    opex = pd.Series((idx.dayofweek == 4) & (idx.day >= 15) & (idx.day <= 21), index=idx)
    ov = d["Open"] / d["Close"].shift(1) - 1                 # Thu close -> Fri open (booked on Friday)
    sgn = np.sign(ov[opex].loc[lib.TRAIN[0]:lib.TRAIN[1]].mean())
    ser = (sgn * ov - 2e-4).where(opex, 0).fillna(0).loc["2000":]
    r = lib.register("245_soq_overnight", f"245 {sym} Thu close -> OPEX Fri open, sign {int(sgn):+d} from train", pd.Series(1.0, index=ser.index), ser, 0.0)
    print(f"245 {sym} sign{int(sgn):+d}  {lib.fmt(r)}  gate={lib.passes_gate(r)}")
print("registry size:", lib.n_tests())
