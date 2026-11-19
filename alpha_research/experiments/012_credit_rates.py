"""012 - Credit & rates ETFs as risk signals for SPY / IWM open->close.
Pre-specified (betas fitted on TRAIN, or first 5y where data starts later):
  H1 credit-equity divergence yesterday: d = HYG_cc - b*SPY_cc (prior day). Credit leads -> pos = sign(d)
  H2 flight to quality yesterday: pos = -sign(TLT prior-day cc)
  H3 [best-case, simultaneous] HYG gap residual vs SPY gap -> follow credit: pos = sign(hyg_gap - b*spy_gap)
  H4 [best-case, simultaneous] TLT gap -> pos = -sign(tlt_gap)
Costs 0.5bp/side SPY, 1bp/side IWM.
"""
import os, sys
import numpy as np
import pandas as pd
import statsmodels.api as sm
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

EXP = "012_credit_rates"
spy, hyg, tlt = lib.get("SPY"), lib.get("HYG"), lib.get("TLT")


def beta(y, x):
    df = pd.concat([y, x], axis=1).dropna()
    tr = df.loc[lib.TRAIN[0]:lib.TRAIN[1]]
    if len(tr) < 750:
        tr = df.iloc[:1250]
    return sm.OLS(tr.iloc[:, 0], sm.add_constant(tr.iloc[:, 1])).fit().params.iloc[1]


b_cc = beta(hyg.cc, spy.cc)
b_gap = beta(hyg.gap, spy.gap)
div = (hyg.cc - b_cc * spy.cc).shift(1)
hres = hyg.gap - b_gap * spy.gap
print(f"HYG starts {hyg.index[0].date()}, TLT starts {tlt.index[0].date()}; b_cc={b_cc:.2f} b_gap={b_gap:.2f}")
for sym, cost in [("SPY", 0.5), ("IWM", 1.0)]:
    d = lib.get(sym).loc["2000":]
    S = {"H1 follow credit-equity divergence (prior day)": np.sign(div),
         "H2 fade TLT prior-day move": -np.sign(tlt.cc.shift(1)),
         "H3 [best-case] follow HYG residual gap": np.sign(hres),
         "H4 [best-case] fade TLT gap": -np.sign(tlt.gap)}
    for name, pos in S.items():
        pos = pos.reindex(d.index).dropna()
        r = lib.register(EXP, f"{sym} {name}", pos, d.oc.reindex(pos.index), cost)
        print(f"{sym} {name:<48} {lib.fmt(r)}  gate={lib.passes_gate(r)}")
print("registry size:", lib.n_tests())
