"""277/278 - Extensions of the month-end bond bid (275/276), train/valid consistency only (no holdout looks):
277 corporate: LQD, HYG long over the last 3 sessions of the month. 278 international government bonds (local sessions):
IGLT.L (UK gilts), XBB.TO (Canada universe bonds), EUNH.DE (euro govts). Close->close, 1.5bp/side."""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
for tag, sym in [("277", "LQD"), ("277", "HYG"), ("278", "IGLT.L"), ("278", "XBB.TO"), ("278", "EUNH.DE")]:
    d = lib.get(sym); idx = d.index; ym = pd.Series(idx.to_period("M"), index=idx)
    bwd = pd.Series(1, index=idx)[::-1].groupby(ym[::-1]).cumsum()[::-1]
    pos = ((bwd <= 3) & (ym != ym.iloc[-1])).astype(float).shift(-1).fillna(0)
    net = lib.backtest_weights(pos.to_frame("X"), d.cc.to_frame("X"), 1.5).loc["2002-08":]
    r = lib.register("275_bond_tom", f"{tag} long {sym} last 3 sessions of month", pd.Series(1.0, index=net.index), net, 0.0)
    print(f"{tag} {sym:<8} {lib.fmt(r)}  start {net.index[0].date()}")
print("registry size:", lib.n_tests())
