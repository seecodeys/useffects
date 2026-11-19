"""008 - Relative opening gaps among liquid ETFs.
For ETF X vs SPY: resid_gap = gap_X - beta*gap_SPY (beta from TRAIN regression).
H (reversal): an out-of-line opening gap reverts intraday -> pos_X-vs-SPY = -sign(resid_gap)
   implemented as long/short X vs beta-hedged SPY: ret = oc_X - beta*oc_SPY.
Pairs: IWM, QQQ, DIA vs SPY. Cross-section: 9 original sector SPDRs, long 2 most
negative resid gap / short 2 most positive (equal $). Costs 1bp/side per leg.
"""
import os, sys
import numpy as np
import pandas as pd
import statsmodels.api as sm
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

EXP = "008_relative_gap"
COST = 1.0
spy = lib.get("SPY").loc["2000":]


def resid(sym):
    x = lib.get(sym).loc["2000":]
    df = pd.DataFrame({"gx": x.gap, "gs": spy.gap, "ox": x.oc, "os": spy.oc}).dropna()
    df = df[(df.gx.abs() < 0.2)]
    tr = df.loc[lib.TRAIN[0]:lib.TRAIN[1]]
    b = sm.OLS(tr.gx, sm.add_constant(tr.gs)).fit().params["gs"]
    bo = sm.OLS(tr.ox, sm.add_constant(tr.os)).fit().params["os"]   # intraday hedge ratio
    df["rg"] = df.gx - b * df.gs
    df["hedged"] = df.ox - bo * df.os
    df["legs"] = 1 + abs(bo)
    return df


out = []
for sym in ["IWM", "QQQ", "DIA"]:
    df = resid(sym)
    pos = -np.sign(df.rg)
    net = pos * df.hedged - df.legs * 2 * COST / 1e4
    r = lib.register(EXP, f"{sym} vs SPY: fade residual gap (hedged)", pd.Series(1.0, index=net.index), net, 0.0,
                     notes="1bp/side per leg in series")
    out.append(r); print(f"{sym} fade resid gap          {lib.fmt(r)}  gate={lib.passes_gate(r)}")

SECT = ["XLB", "XLE", "XLF", "XLI", "XLK", "XLP", "XLU", "XLV", "XLY"]
RG, OC = {}, {}
for s in SECT:
    df = resid(s); RG[s], OC[s] = df.rg, df.ox
RG, OC = pd.DataFrame(RG), pd.DataFrame(OC)
rk = RG.rank(axis=1); k = RG.notna().sum(axis=1)
w = pd.DataFrame(0.0, index=RG.index, columns=RG.columns)
w[rk.le(2)] = 0.25           # most negative residual gap -> long (reversal)
w[rk.ge(k.values[:, None] - 1)] = -0.25
w = w[k >= 7].reindex(RG.index).fillna(0)
gross = (w * OC.fillna(0)).sum(axis=1)
net = gross - w.abs().sum(axis=1) * 2 * COST / 1e4
r = lib.register(EXP, "sector XS: fade residual gap top2/bottom2", pd.Series(1.0, index=net.index), net, 0.0,
                 notes="1bp/side in series")
out.append(r); print(f"sector XS fade resid gap     {lib.fmt(r)}  gate={lib.passes_gate(r)}")
g = gross.loc[:"2022"]; print("  gross bp/day by year:", (g.groupby(g.index.year).mean() * 1e4).round(1).to_dict())
pd.DataFrame(out).to_csv(os.path.join(lib.HERE, "results", "008_relative_gap.csv"), index=False)
print("registry size:", lib.n_tests())
