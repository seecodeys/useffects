"""004b - Is the 004 cross-sectional ADR edge a bid-ask/tick-size artifact?
Check 1: per-ADR gross catch-up edge vs tick size in bp (0.01 / price).
Check 2: XS L/S restricted to ADRs with prior close >= $20 on that day (tick <= 5bp).
"""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
sys.path.insert(0, os.path.join(lib.HERE, "experiments"))
import importlib
m = importlib.import_module("004_adr_catchup")

S, Si, Y = m.build()
Y = Y.reindex(S.index)
px = pd.DataFrame({a: lib.get(a)["Close"].shift(1) for a in S}).reindex(S.index)   # raw, unadjusted prior close
rows = []
for a in S:
    s, y = S[a], Y[a]
    ok = s.notna() & y.notna()
    for per, (lo, hi) in {"train": lib.TRAIN, "valid": lib.VALID}.items():
        k = ok & (s.index >= lo) & (s.index <= hi)
        rows.append({"adr": a, "period": per, "gross_bp": 1e4 * (np.sign(s[k]) * y[k]).mean(),
                     "tick_bp": 1e4 * (0.01 / px[a][k]).median()})
d = pd.DataFrame(rows).pivot(index="adr", columns="period")
d.columns = [f"{a}_{b}" for a, b in d.columns]
print(d.round(1).sort_values("tick_bp_valid").to_string())
print("corr(gross edge, tick bp) valid:", round(d.gross_bp_valid.corr(d.tick_bp_valid), 2),
      " train:", round(d.gross_bp_train.corr(d.tick_bp_train), 2))

for thr in [20.0]:
    Sx = S.where(px >= thr)
    gross, net = m.xs(Sx, Y, top=2, min_n=5)
    r = lib.register("004_adr_catchup", f"XS catch-up L/S top2-bottom2, prior close >= ${thr:.0f}",
                     pd.Series(1.0, index=net.index), net, 0.0, notes="3bp/side in series; tick-artifact check")
    print(f"price>=${thr:.0f}:", lib.fmt(r))
    g = gross.loc[:"2022"]
    print("   gross bp/day by year:", (g.groupby(g.index.year).mean() * 1e4).round(1).to_dict())
    print("   avg names/day:", Sx.notna().sum(axis=1).loc["2015":"2022"].mean().round(1))
print("registry size:", lib.n_tests())
