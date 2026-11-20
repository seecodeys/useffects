"""301 - Crowded short-volatility positioning (CFTC TFF, VIX futures 1170E1). Mechanism: when leveraged funds are
unusually short VIX futures (and asset managers via short-vol ETPs), a vol shock forces them to buy back -> hedging
flows amplify sell-offs; when they're unusually long VIX, hedges are on and dips get bought. Same timing as 299
(first close after the Friday release, held one week). Pre-specified variants:
 A SPY  sign(z lev VIX net) when |z|>1   (crowded short vol -> short SPY; crowded long vol -> long SPY)
 B SPY  sign(z asset-mgr VIX net) when |z|>1
 C SPY  long only, flat when lev-fund z < -1 (risk-overlay form; compare with always-long)"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
exec(open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "299_cftc_positioning.py")).read().split("ES, TY, US =")[0])
VX = load("1170E1"); zl, za = z(VX.lev), z(VX.am)
d = lib.get("SPY")
variants = {"A SPY sign(z lev VIX) |z|>1": np.sign(zl).where(zl.abs() > 1, 0.0),
            "B SPY sign(z asset-mgr VIX) |z|>1": np.sign(za).where(za.abs() > 1, 0.0),
            "C SPY long, flat when z lev VIX < -1": pd.Series(1.0, index=zl.index).where(~(zl < -1), 0.0).where(zl.notna())}
for name, sig in variants.items():
    w = lib.asof(sig.dropna(), d.index, strict=True).fillna(0.0)
    net = lib.backtest_weights(w.to_frame("X"), d.cc.to_frame("X"), 0.5).loc["2009":]
    r = lib.register("299_cftc", f"301{name}", pd.Series(1.0, index=net.index), net, 0.0)
    held = w.shift(1).loc["2009":"2022"]
    ex = (net.loc[:"2022"] - held.mean() * d.cc.loc["2009":"2022"]).dropna()
    print(f"301{name}: {lib.fmt(r)}  mean pos {held.mean():+.2f}  timing-value t (2009-22) {ex.mean()/ex.std()*np.sqrt(len(ex)):+.2f}")
print("registry size:", lib.n_tests())
