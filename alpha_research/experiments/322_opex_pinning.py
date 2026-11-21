"""322 - Strike pinning on expiry days. Observation: on big expiries the index often 'sticks' to round strikes in the
afternoon — dealers long gamma at high-OI strikes sell rallies above / buy dips below the strike. SPX CFD 1-min (2010-22).
At 13:00 ET, d = level - nearest multiple of K; position = -sign(d) (bet on moving toward the strike) 13:00->15:55,
only when |d| > K/10 (not already pinned). 0.5bp/side. Pre-specified:
 A monthly OPEX Fridays, K=25    B monthly OPEX Fridays, K=50    C control: other Fridays, K=25
(SPY/SPXW PM-settled options expire at the Friday close; monthly SPX AM options settled at the open.)"""
import os, sys, importlib
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
sys.path.insert(0, os.path.join(lib.HERE, "experiments"))
fx = importlib.import_module("003_overnight_fx")
px = fx.ny_minute_close("SPXUSD", range(2010, 2023))
spy = lib.get("SPY"); idx = spy.index[(spy.index >= "2010-01-01") & (spy.index <= "2022-12-31")]
fri = idx[idx.dayofweek == 4]; is_opex = (fri.day >= 15) & (fri.day <= 21)
for name, days, K in [("A OPEX Fri K=25", fri[is_opex], 25), ("B OPEX Fri K=50", fri[is_opex], 50), ("C other Fri K=25", fri[~is_opex], 25)]:
    a, b = fx.level_at(px, days, 13, 0, tol_min=5), fx.level_at(px, days, 15, 55, tol_min=5)
    d = a - (a / K).round() * K
    pos = (-np.sign(d)).where(d.abs() > K / 10, 0.0)
    ret = (pos * (b / a - 1) - pos.abs() * 1e-4).dropna()
    full = pd.Series(0.0, index=spy.index); full.loc[ret.index] = ret
    r = lib.register("322_opex_pinning", f"322{name}", pd.Series(1.0, index=full.index), full.loc["2010":"2022"], 0.0)
    t = ret[pos.reindex(ret.index) != 0] * 1e4
    print(f"322{name}: {lib.fmt(r)}  per-trade {t.mean():+.1f}bp hit {100*(t>0).mean():.0f}% n={len(t)}")
print("registry size:", lib.n_tests())
