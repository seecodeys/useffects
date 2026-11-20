"""054-057 - More behavioral tests; all signals from the prior close; trade next open->close.
054 anchoring (George-Hwang): long large caps that closed at a new 252-day high yesterday, short those at a new 252-day
    low; equal-weight each side, residual market exposure hedged with SPY. 2bp/side/leg. (survivorship-biased universe)
055 lottery demand (MAX effect): short top-decile / long bottom-decile by max daily return over the last 21 days. 2bp.
056 sector herding: 9 sector SPDRs, long the 2 worst / short the 2 best prior-day residual (vs SPY). 1bp/side.
057 Monday mood: long SPY Monday open->close after a Friday close-to-close drop < -1%. 0.5bp.
"""
import os, sys, importlib
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
sys.path.insert(0, os.path.join(lib.HERE, "experiments"))
U = importlib.import_module("006_stock_gap_reversal").UNIVERSE
spy = lib.get("SPY")
OC, AC, CC, PX = {}, {}, {}, {}
for s in U:
    try:
        d = lib.get(s).loc["1998-06":]
    except Exception:
        continue
    OC[s], AC[s], CC[s], PX[s] = d.oc, d.aC, d.cc, d.Close
OC, AC, CC, PX = (pd.DataFrame(x) for x in (OC, AC, CC, PX))
elig = (PX.shift(1) >= 20)
spy_oc = spy.oc.reindex(OC.index).fillna(0)


def hedged_ls(longs, shorts, cost):
    nl, ns_ = longs.sum(axis=1), shorts.sum(axis=1)
    w = (longs.astype(float).div(nl.replace(0, np.nan), axis=0).fillna(0) * 0.5
         - shorts.astype(float).div(ns_.replace(0, np.nan), axis=0).fillna(0) * 0.5)
    nb = w.sum(axis=1)
    gross = (w * OC.fillna(0)).sum(axis=1) - nb * spy_oc
    return (gross - (w.abs().sum(axis=1) + nb.abs()) * 2 * cost / 1e4).loc["2000":]


def show(exp, name, net):
    r = lib.register(exp, name, pd.Series(1.0, index=net.index), net, 0.0, notes="costs in series")
    print(f"{name:<52} {lib.fmt(r)}  gate={lib.passes_gate(r)}")


hi = (AC >= AC.rolling(252).max()).shift(1) & elig
lo = (AC <= AC.rolling(252).min()).shift(1) & elig
show("054_anchoring", "054 long new 52w highs / short new 52w lows (hedged)", hedged_ls(hi, lo, 2.0))
mx = CC.rolling(21).max().shift(1).where(elig)
rk = mx.rank(axis=1, pct=True)
show("055_lottery", "055 short top-decile MAX / long bottom-decile", hedged_ls(rk.le(0.1), rk.gt(0.9), 2.0))

SECT = ["XLB", "XLE", "XLF", "XLI", "XLK", "XLP", "XLU", "XLV", "XLY"]
so = pd.DataFrame({s: lib.get(s).oc for s in SECT}).loc["2000":]
sr = pd.DataFrame({s: lib.get(s).cc for s in SECT}).sub(spy.cc, axis=0).shift(1).loc["2000":]
r2 = sr.rank(axis=1)
w = pd.DataFrame(0.0, index=so.index, columns=SECT)
w[r2.le(2)] = 0.25; w[r2.ge(8)] = -0.25
show("056_sector_herding", "056 sectors: long 2 worst / short 2 best prior-day resid",
     (w * so.fillna(0)).sum(axis=1) - w.abs().sum(axis=1) * 2 * 1.0 / 1e4)

y = spy.oc.loc["2000":]
fri_drop = (spy.cc < -0.01) & (spy.index.dayofweek == 4)
mon = pd.Series(y.index.dayofweek == 0, index=y.index) & fri_drop.shift(1).reindex(y.index).fillna(False)
r = lib.register("057_monday_mood", "057 long SPY Monday after Friday drop < -1%", mon.astype(float), y, 0.5)
print(f"{'057 long SPY Monday after Friday drop < -1%':<52} {lib.fmt(r)}  gate={lib.passes_gate(r)}  n={int(mon.sum())}")
print("registry size:", lib.n_tests())
