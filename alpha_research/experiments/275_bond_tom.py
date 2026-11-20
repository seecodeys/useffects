"""275 - Bond turn-of-month flows (index extension at month-end + coupon reinvestment early month):
  a) long TLT close->close over the last 3 sessions of each month   b) long TLT over the first 3 sessions of each month
1bp/side; raw (TLT has no strong drift)."""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
tlt = lib.get("TLT"); idx = tlt.index; ym = pd.Series(idx.to_period("M"), index=idx)
fwd = pd.Series(1, index=idx).groupby(ym).cumsum()
bwd = pd.Series(1, index=idx)[::-1].groupby(ym[::-1]).cumsum()[::-1]
for tag, want in ([] if set(sys.argv) & {"--check","--holdout"} else [("a last 3 sessions", ((bwd <= 3) & (ym != ym.iloc[-1])).astype(float)), ("b first 3 sessions", (fwd <= 3).astype(float))]):
    pos = want.shift(-1).fillna(0)
    net = lib.backtest_weights(pos.to_frame("T"), tlt.cc.to_frame("T"), 1.0).loc["2002-08":]
    r = lib.register("275_bond_tom", f"275{tag}: long TLT", pd.Series(1.0, index=net.index), net, 0.0)
    print(f"275{tag:<20} {lib.fmt(r)}  gate={lib.passes_gate(r)}")
print("registry size:", lib.n_tests())

if set(sys.argv) & {"--check", "--holdout"}:
    want = ((bwd <= 3) & (ym != ym.iloc[-1])).astype(float); pos = want.shift(-1).fillna(0)
    net = lib.backtest_weights(pos.to_frame("T"), tlt.cc.to_frame("T"), 1.0).loc["2002-08":]
    if "--check" in sys.argv:
        y = (net.groupby(net.index.year).sum() * 100).loc[:2022]
        print("yearly %:", y.round(1).to_dict(), " positive", (y > 0).sum(), "/", len(y), " ex-best-3 total", round(y.sort_values().iloc[:-3].sum(), 1), "of", round(y.sum(), 1))
    if "--holdout" in sys.argv:
        for c in [1.0, 2.0]:
            n2 = lib.backtest_weights(pos.to_frame("T"), tlt.cc.to_frame("T"), c).loc["2002-08":]
            h = lib.holdout_eval("275_bond_tom", f"275a long TLT last 3 sessions of month (cost {c}bp)", pd.Series(1.0, index=n2.index), n2, 0.0)
            print("HOLDOUT", c, {k: round(float(v), 2) for k, v in h.items()})
