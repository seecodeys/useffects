"""257 - Out-of-country replication of the month-end stock-bond rebalancing effect (013), using each market's own equity
and government-bond ETFs and local trading days. On the local last trading day of each completed month,
pos = −sign(equity MTD − bond MTD) (through the prior local close), equity ETF open->close, 2bp/side.
Pairs: Canada XIU.TO/XBB.TO, UK ISF.L/IGLT.L, Australia STW.AX/IAF.AX, Germany/euro EXS1.DE/EUNH.DE.
Pooled equal-weight across available countries as well. Holdout (2023+) untouched unless the pooled test passes the gate.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
PAIRS = {"Canada": ("XIU.TO", "XBB.TO"), "UK": ("ISF.L", "IGLT.L"), "Australia": ("STW.AX", "IAF.AX"), "Euro": ("EXS1.DE", "EUNH.DE")}


def country(eq, bd):
    e, b = lib.get(eq), lib.get(bd)
    idx = e.index.intersection(b.index)
    e, b = e.loc[idx], b.loc[idx]
    ym = pd.Series(idx.to_period("M"), index=idx)
    last = (ym != ym.shift(-1)) & (ym != ym.iloc[-1])
    def mtd(x):
        base = x["aC"].where(ym != ym.shift(-1)).shift(1).ffill()
        base = base.where(ym != ym.shift(1)).ffill()
        return x["aC"].shift(1) / base - 1
    pos = (-np.sign(mtd(e) - mtd(b))).where(last, 0).fillna(0)
    return pos, e.oc


P = {}
for name, (eq, bd) in ({} if '--holdout-canada' in sys.argv else PAIRS).items():
    pos, y = country(eq, bd)
    r = lib.register("257_intl_monthend", f"257 {name} {eq}/{bd} month-end rule", pos, y, 2.0)
    pnl = lib.daily_pnl(pos, y, 2.0)
    P[name] = pnl
    t = (pos * y)[pos != 0] * 1e4
    print(f"257 {name:<9} {lib.fmt(r)}  per-trade bp train {t.loc[:'2014'].mean():+.1f} (n={len(t.loc[:'2014'])}) valid {t.loc['2015':'2022'].mean():+.1f} (n={len(t.loc['2015':'2022'])})")
if P:
  Pdf = pd.DataFrame(P)
  active = Pdf.ne(0).sum(axis=1).replace(0, np.nan)
  pooled = (Pdf.fillna(0).sum(axis=1) / active).fillna(0)
  r = lib.register("257_intl_monthend", "257 pooled 4-country month-end rule", pd.Series(1.0, index=pooled.index), pooled, 0.0)
  print(f"257 pooled   {lib.fmt(r)}  gate={lib.passes_gate(r)}")
print("registry size:", lib.n_tests())

if "--holdout-canada" in sys.argv:
    pos, y = country("XIU.TO", "XBB.TO")
    h = lib.holdout_eval("257_intl_monthend", "257 Canada XIU/XBB month-end rule (2bp)", pos, y, 2.0)
    t = (pos * y)[pos != 0].loc["2023":] * 1e4
    print("HOLDOUT Canada", {k: round(float(v), 2) for k, v in h.items()}, f" per-trade {t.mean():+.1f}bp hit {100*(t>0).mean():.0f}% n={len(t)}")
