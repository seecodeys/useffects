"""025 - US jobs-report days (8:30 ET release; news lands in the opening gap).
Dates: first Friday of each month (BLS usually releases then; occasional exceptions, e.g. when the 1st Friday is
very early or around holidays — approximation, so expect some misclassified days, biasing toward zero).
  H1 fade the gap on jobs days:  pos = -sign(gap)
  H2 long on jobs days (uncertainty resolved -> risk premium)
SPY/QQQ, 0.5bp/side.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

for sym in ["SPY", "QQQ"]:
    d = lib.get(sym).loc["2000":]
    idx = d.index
    first_fri = pd.Series((idx.dayofweek == 4) & (idx.day <= 7), index=idx)
    for name, pos in {"025 H1 fade gap on jobs-report day": (-np.sign(d.gap)).where(first_fri, 0),
                      "025 H2 long on jobs-report day": first_fri.astype(float)}.items():
        r = lib.register("025_jobs_report", f"{sym} {name}", pos.fillna(0), d.oc, 0.5)
        print(f"{sym} {name:<36} {lib.fmt(r)}  gate={lib.passes_gate(r)}")
    g = d[first_fri]
    print(f"   {sym}: |gap| jobs days {g.gap.abs().mean()*1e4:.0f}bp vs other Fridays "
          f"{d[(idx.dayofweek==4)&~first_fri].gap.abs().mean()*1e4:.0f}bp")
print("registry size:", lib.n_tests())
