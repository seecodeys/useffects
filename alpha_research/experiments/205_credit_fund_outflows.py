"""205 - Credit-fund outflow pressure: bond mutual-fund/ETF investors redeem after losses (flow-performance sensitivity),
forcing sales that extend the decline. Pre-specified: when HYG's 5-day return (through close t) is in the bottom 10% of
its TRAIN-period distribution, short HYG / long IEF (duration-neutral-ish 1:1) for the next 5 sessions (lag-1),
1.5bp/side/leg. Same for LQD vs IEF. 2007+ (HYG) / 2002+ (LQD)."""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
ief = lib.get("IEF")
for sym in ["HYG", "LQD"]:
    d = lib.get(sym)
    r5 = d.aC / d.aC.shift(5) - 1
    tr = r5.loc[lib.TRAIN[0]:lib.TRAIN[1]]
    trig = (r5 < tr.quantile(0.10)).astype(float)
    hold = trig.rolling(5, min_periods=1).max()
    R = pd.concat({"X": d.cc, "IEF": ief.cc}, axis=1).dropna()
    W = pd.DataFrame({"X": -hold, "IEF": hold}).reindex(R.index).fillna(0)
    net = lib.backtest_weights(W, R, 1.5, lag=1).loc["2002-08":]
    r = lib.register("205_credit_outflows", f"205 short {sym}/long IEF 5d after bottom-decile 5d {sym} return", pd.Series(1.0, index=net.index), net, 0.0)
    print(f"205 {sym}  {lib.fmt(r)}  gate={lib.passes_gate(r)}  active%={100*(hold>0).loc[:'2022'].mean():.0f}")
print("registry size:", lib.n_tests())
