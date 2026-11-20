"""082 - Fear over-reaction via VIX futures ETPs. VIXY (long 1-month VIX futures, 2011+); short VIXY = short vol.
Short VIXY earns a large roll premium on average, so each rule is measured as TIMING VALUE over an exposure-matched
always-short position (per period). Close->close, lag-1 (signal at close t, position from close t+1), 5bp/side.
  a) spike fade: short VIXY for 5 days after VIX closes > 25% above its 20-day mean (fear over-shoot)
  b) term-structure: short VIXY only when VIX < VIX3M (contango), flat in backwardation
TRAIN for VIXY is only 2011-2014.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
vixy = lib.get("VIXY"); vix = lib.get("^VIX")["Close"]; vix3m = lib.get("^VIX3M")["Close"]
idx = vixy.index
r = vixy.cc
spike = (vix / vix.rolling(20).mean() > 1.25).reindex(idx).fillna(False)
rules = {"082a short VIXY 5d after VIX > 1.25x 20d mean": spike.astype(float).rolling(5, min_periods=1).max(),
         "082b short VIXY only in contango (VIX<VIX3M)": (vix.reindex(idx) < vix3m.reindex(idx)).astype(float)}
for name, sig in rules.items():
    pos = -sig
    net = lib.backtest_weights(pos.to_frame("V"), r.to_frame("V"), 5.0, lag=1)
    held = pos.shift(2).fillna(0)
    out = pd.Series(0.0, index=net.index)
    for a, b in [lib.TRAIN, lib.VALID, lib.HOLDOUT]:
        out.loc[a:b] = net.loc[a:b] - held.loc[a:b].mean() * r.loc[a:b].fillna(0)
    res = lib.register("082_short_vol_etp", name + " (timing value)", pd.Series(1.0, index=out.index), out.fillna(0), 0.0)
    m_raw = lib.evaluate(pd.Series(1.0, index=net.index), net.loc[:"2022"], 0)
    print(f"{name:<48} {lib.fmt(res)}  gate={lib.passes_gate(res)}  exposure={100*held.loc[:'2022'].abs().mean():.0f}%  raw Sharpe 2011-22={m_raw['sharpe']:.2f}")
print("always-short VIXY raw Sharpe 2011-22:", round(lib.evaluate(pd.Series(-1.0, index=idx), r.loc[:'2022'].fillna(0), 5.0)['sharpe'], 2))
print("registry size:", lib.n_tests())
