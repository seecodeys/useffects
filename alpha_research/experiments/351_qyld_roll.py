"""351 - Nasdaq-100 covered-call roll (QYLD/BXNT): dealer net-delta flow on the session before monthly OPEX.
Card 2, results/ideas_options_etf.md.
Mechanism: on the roll day QYLD buys back last month's NDX call and sells a new ATM one. A dealer long the old call is
short ~delta_old x notional as a hedge; long the new ATM call it needs ~0.5 x notional short. Net dealer trade =
(delta_old - 0.5) x notional: buy NDX when NDX is above the old strike, sell when below.
Signal (known at the prior close): m = NDX close on the session before the roll / NDX close on the previous roll day
- 1 (old strike proxy). S = sign(m).
Roll day R = trading session before the monthly OPEX date (3rd Friday, or the prior session if that Friday is a holiday).
Pre-specified variants (registered; standard periods TRAIN <=2014, VALID 2015-22; QYLD launched Dec 2013, so TRAIN is
mostly a no-fund control period; by-year results reported):
  A  S x (QQQ.oc - beta*SPY.oc) on R, beta = rolling 250d cc beta known at prior close. Cost 0.5bp/side/leg.
  B  S x NSX CFD 12:00->15:59 NY on R (HistData 1-min, 2010+). Cost 0.5bp/side. Descriptive VWAP-window proxy.
  C  A only when |m| > 2%.
  P  placebo: A's rule on the session one week before R (signal vs previous roll at the prior close). Expect ~0.
Descriptive (not new variants): A split 2000-13 / 2014-18 / 2019-22 (dose-response with QYLD AUM), A at 1bp/side/leg.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
sys.path.insert(0, os.path.join(lib.HERE, "experiments"))
from importlib import import_module
h003 = import_module("003_overnight_fx")

EXP = "351_qyld_roll"
END = "2022-12-31"
q, s, n = (lib.get(x).loc["2000":END] for x in ("QQQ", "SPY", "^NDX"))
idx = q.index.intersection(s.index).intersection(n.index)
q, s, n = q.loc[idx], s.loc[idx], n.loc[idx]

# OPEX date per month: 3rd Friday, or the last session on/before it
opex = []
for (y, mth), _ in pd.Series(0, index=idx).groupby([idx.year, idx.month]):
    first = pd.Timestamp(y, mth, 1)
    fri3 = first + pd.Timedelta(days=(4 - first.dayofweek) % 7 + 14)
    on = idx[idx <= fri3]
    if len(on) and on[-1].month == mth:
        opex.append(on[-1])
opex = pd.DatetimeIndex(opex)
pos_of = {d: i for i, d in enumerate(idx)}
roll = pd.DatetimeIndex([idx[pos_of[d] - 1] for d in opex if pos_of[d] >= 1])
placebo = pd.DatetimeIndex([idx[idx <= r - pd.Timedelta(days=7)][-1] for r in roll[1:]])

ndx = n["Close"]
def signal(days):
    out = {}
    for d in days:
        prev_roll = roll[roll < d]
        if not len(prev_roll):
            continue
        prior = idx[pos_of[d] - 1]
        if prior <= prev_roll[-1]:
            continue
        out[d] = ndx.loc[prior] / ndx.loc[prev_roll[-1]] - 1
    return pd.Series(out)

m_roll, m_plc = signal(roll), signal(placebo)
beta = (q.cc.rolling(250).cov(s.cc) / s.cc.rolling(250).var()).shift(1)
spread = q.oc - beta * s.oc
bmean = float(beta.loc[:"2014"].mean())
cA = 0.5 * (1 + bmean)

def mkpos(m, thr=0.0):
    p = pd.Series(0.0, index=idx)
    mm = m[m.abs() > thr]
    p.loc[mm.index] = np.sign(mm.values)
    return p

posA, posC, posP = mkpos(m_roll), mkpos(m_roll, 0.02), mkpos(m_plc)
rows = {}
rows["A"] = lib.register(EXP, "A S*(QQQ-bSPY) oc roll day", posA, spread, cA, f"beta mean {bmean:.2f}, cost per leg 0.5")
rows["C"] = lib.register(EXP, "C A if |m|>2%", posC, spread, cA)
rows["P"] = lib.register(EXP, "P placebo A one week earlier", posP, spread, cA)

px = h003.ny_minute_close("NSXUSD", range(2010, 2023))
cd = idx[idx >= "2010-01-01"]
b = h003.level_at(px, cd, 15, 59, 5) / h003.level_at(px, cd, 12, 0, 5) - 1
b = b.reindex(idx)
posB = posA.copy(); posB.loc[:"2009"] = 0; posB[b.isna()] = 0
rows["B"] = lib.register(EXP, "B S*NSX CFD 12:00-15:59 roll day", posB, b.fillna(0), 0.5)

for k, r in rows.items():
    print(f"{k}: {lib.fmt(r)} train_bp={r['train_bp_per_trade']:+.1f} n_tr_days={r['train_traded_%']*r['train_n_days']/100:.0f} "
          f"n_va={r['valid_traded_%']*r['valid_n_days']/100:.0f} gate={lib.passes_gate(r)}")

# descriptive splits / by year (<=2022 only)
def bp(pos, ret, c):
    t = pos != 0
    return 1e4 * (pos * ret - pos.abs() * 2 * c / 1e4)[t]
for name, pos, ret, c in [("A", posA, spread, cA), ("A@1bp/leg", posA, spread, 2 * cA), ("B", posB, b.fillna(0), 0.5),
                          ("C", posC, spread, cA), ("P", posP, spread, cA)]:
    x = bp(pos, ret, c)
    for a, z in [("2000", "2013"), ("2014", "2018"), ("2019", "2022")]:
        xx = x.loc[a:z]
        if len(xx):
            print(f"  {name:10s} {a}-{z}: n={len(xx):3d} mean={xx.mean():+6.1f}bp t={xx.mean()/xx.std()*np.sqrt(len(xx)):+.2f}")
xa, xb = bp(posA, spread, cA), bp(posB, b.fillna(0), 0.5)
yr = pd.DataFrame({"A_bp_sum": xa.groupby(xa.index.year).sum(), "A_n": xa.groupby(xa.index.year).size(),
                   "B_bp_sum": xb.groupby(xb.index.year).sum()}).loc[:2022]
print(yr.round(0).to_string())
# raw (unsigned) spread on roll days, and m vs return correlation
ra = spread.loc[m_roll.index].dropna()
print("corr(m, spread) roll days 2014-22:", round(float(np.corrcoef(m_roll.loc["2014":].reindex(ra.index).dropna(),
      ra.loc[m_roll.loc['2014':].index].dropna())[0, 1]), 3))
print("registry size:", lib.n_tests())
